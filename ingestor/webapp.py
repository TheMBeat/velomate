"""Minimal built-in HTTP server for FIT upload UI + REST endpoints."""

from __future__ import annotations

import cgi
import json
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from fit_import import FitImportError
from hr_fit_merge import (
    FitHrMergeError,
    MergeOptions,
    parse_apple_hr_payload_details,
    parse_fit_records_for_merge,
)
from hr_merge_service import parse_merge_options, run_merge
from import_service import persist_fit_import, preview_fit_import
from stores import ExpiringTokenStore

_PENDING_TTL = timedelta(minutes=30)
_MERGED_TTL = timedelta(minutes=30)
_PENDING_STORE = ExpiringTokenStore(ttl=_PENDING_TTL)
_MERGED_STORE = ExpiringTokenStore(ttl=_MERGED_TTL)

# Backwards-compatible aliases used by tests.
_PENDING_IMPORTS = _PENDING_STORE.items
_PENDING_IMPORTS_LOCK = _PENDING_STORE.lock


def _store_pending(payload: dict) -> str:
    return _PENDING_STORE.put(payload)


def _load_pending(token: str, pop: bool = False) -> dict:
    try:
        return _PENDING_STORE.get(token, pop=pop)
    except KeyError as exc:
        raise KeyError("Unknown or expired import token") from exc


def _store_merged_artifact(filename: str, content: bytes, report: dict) -> str:
    return _MERGED_STORE.put({"filename": filename, "content": content, "report": report})


def _load_merged_artifact(token: str) -> dict:
    try:
        return _MERGED_STORE.get(token, pop=False)
    except KeyError as exc:
        raise KeyError("Unknown artifact token") from exc


def _save_import(token: str) -> tuple[int, int]:
    parsed = _load_pending(token, pop=True)
    return persist_fit_import(parsed)


def _handle_fit_preview(filename: str, content: bytes) -> dict:
    parsed = preview_fit_import(filename, content)
    token = _store_pending(parsed)
    return {"import_token": token, "preview": parsed["preview"]}


def _handle_hr_merge_preview(fit_filename: str, fit_content: bytes, apple_content: bytes, apple_source_type: str) -> dict:
    if not fit_filename.lower().endswith(".fit"):
        raise FitHrMergeError("FIT input must end with .fit")

    fit_payload = parse_fit_records_for_merge(fit_content)
    apple_parsed = parse_apple_hr_payload_details(apple_content, source_type=apple_source_type)
    apple_raw = list(apple_parsed.get("samples", []))
    raw_debug = dict(apple_parsed.get("debug", {}))
    fit_start = fit_payload["summary"]["start_time"]
    fit_end = fit_payload["summary"]["end_time"]
    overlap_count = sum(1 for row in apple_raw if fit_start <= row.get("timestamp", "") <= fit_end)
    apple_text = apple_content.decode("utf-8", errors="replace")
    apple_debug = {
        "requested_source_type": apple_source_type,
        "detected_source_type": apple_parsed.get("source_type"),
        "parser_mode": raw_debug.get("parser_mode"),
        "top_level_keys": raw_debug.get("top_level_keys", []),
        "data_keys": raw_debug.get("data_keys", []),
        "workouts_found": raw_debug.get("workouts_found", 0),
        "selected_workout_index": raw_debug.get("selected_workout_index"),
        "selected_workout_id": raw_debug.get("selected_workout_id"),
        "selected_workout_has_heart_rate_data": raw_debug.get("selected_workout_has_heart_rate_data", False),
        "selected_workout_heart_rate_point_count": raw_debug.get("selected_workout_heart_rate_point_count", 0),
        "selected_workout_parseable_point_count": raw_debug.get("selected_workout_parseable_point_count", 0),
        "fallback_workout_index": raw_debug.get("fallback_workout_index"),
        "raw_heart_rate_entries_found": raw_debug.get("raw_heart_rate_entries_found", 0),
        "parsed_heart_rate_entries_count": raw_debug.get("parsed_heart_rate_entries_count", len(apple_raw)),
        "rejected_entries_count": raw_debug.get("rejected_entries_count", 0),
        "rejection_reasons": raw_debug.get("rejection_reasons", {}),
        "sample_preview": apple_raw[:5],
        "apple_bytes": len(apple_content),
        "apple_text_preview": apple_text[:200],
    }
    print(
        "[webapp._handle_hr_merge_preview] Apple parse debug: "
        f"requested_source_type={apple_debug.get('requested_source_type')}, "
        f"detected_source_type={apple_debug.get('detected_source_type')}, "
        f"parser_mode={apple_debug.get('parser_mode')}, "
        f"workouts_found={apple_debug.get('workouts_found')}, "
        f"selected_workout_index={apple_debug.get('selected_workout_index')}, "
        f"selected_workout_id={apple_debug.get('selected_workout_id')}, "
        f"selected_workout_has_heart_rate_data={apple_debug.get('selected_workout_has_heart_rate_data')}, "
        f"selected_workout_heart_rate_point_count={apple_debug.get('selected_workout_heart_rate_point_count')}, "
        f"selected_workout_parseable_point_count={apple_debug.get('selected_workout_parseable_point_count')}, "
        f"fallback_workout_index={apple_debug.get('fallback_workout_index')}, "
        f"raw_heart_rate_entries_found={apple_debug.get('raw_heart_rate_entries_found')}, "
        f"parsed_heart_rate_entries_count={apple_debug.get('parsed_heart_rate_entries_count')}, "
        f"rejected_entries_count={apple_debug.get('rejected_entries_count')}, "
        f"apple_bytes={apple_debug.get('apple_bytes')}, "
        f"extracted_hr_points={len(apple_raw)}, overlap_points={overlap_count}"
    )
    warnings = []
    if not apple_raw:
        warnings.append("No Apple HR points could be parsed from the uploaded file; inspect apple_debug for parser/input details.")
    elif overlap_count == 0:
        warnings.append("Apple HR points were parsed but none overlap the FIT timeline; verify timezone/export range.")
    merge_payload = {
        "fit_filename": fit_filename,
        "fit_bytes": fit_content,
        "fit_records": fit_payload["records"],
        "apple_raw": apple_raw,
        "apple_debug": apple_debug,
    }
    response = {
        "fit_summary": fit_payload["summary"],
        "apple_debug": apple_debug,
        "apple_summary": {
            "point_count": len(apple_raw),
            "first_timestamp": apple_raw[0]["timestamp"] if apple_raw else None,
            "last_timestamp": apple_raw[-1]["timestamp"] if apple_raw else None,
            "debug": apple_debug,
        },
        "estimated_overlap_points": overlap_count,
        "warnings": warnings,
    }
    token = _store_pending(merge_payload)
    return {"import_token": token, **response}


def _run_hr_merge(import_token: str, options: MergeOptions) -> dict:
    payload = _load_pending(import_token, pop=False)
    output_name, content, report = run_merge(payload, options)
    artifact_token = _store_merged_artifact(output_name, content, report)
    return {
        "artifact_token": artifact_token,
        "download_url": f"/tools/fit-hr-merge/download?token={artifact_token}",
        "report": report,
        "apple_debug": payload.get("apple_debug", {}),
        "output_format": "fit",
    }

def _render_upload_page() -> str:
    return """
    <html><head><title>VeloMate FIT Import</title>
    <style>body{background:#111827;color:#e5e7eb;font-family:Arial} .card{max-width:760px;margin:32px auto;background:#1f2937;border-radius:12px;padding:24px} .btn{background:#2563eb;color:#fff;border:0;border-radius:8px;padding:10px 16px}</style>
    </head><body><div class='card'>
    <h1>Import FIT file</h1>
    <form action='/imports/fit/preview' method='post' enctype='multipart/form-data'>
      <input type='file' name='file' accept='.fit' required/><br/><br/>
      <button class='btn' type='submit'>Upload & Preview</button>
    </form>
    <p><a href='/tools/fit-hr-merge' style='color:#93c5fd'>Open Apple HR + FIT merger tool</a></p>
    </div></body></html>
    """


def _render_hr_merge_page() -> str:
    return """
    <html><head><title>VeloMate Apple HR + FIT Merger</title>
    <style>
      body{background:#111827;color:#e5e7eb;font-family:Arial;margin:0;padding:20px}
      .card{max-width:920px;margin:20px auto;background:#1f2937;border-radius:12px;padding:20px}
      .grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
      .btn{background:#2563eb;color:#fff;border:0;border-radius:8px;padding:10px 14px;margin-top:10px}
      .muted{color:#9ca3af}
      .debug-panel{margin-top:12px;border:1px solid #374151;border-radius:8px;background:#111827}
      .debug-panel summary{cursor:pointer;padding:10px 12px;font-weight:600}
      .debug-panel pre{margin:0 12px 12px 12px}
      input,select{background:#111827;color:#e5e7eb;border:1px solid #374151;border-radius:6px;padding:6px}
      pre{white-space:pre-wrap;background:#111827;border:1px solid #374151;padding:12px;border-radius:8px}
    </style>
    </head><body>
      <div class='card'>
        <h1>Apple HR + FIT Merger</h1>
        <p class='muted'>FIT timeline is master. Apple heart rate is mapped to existing FIT timestamps only.</p>
        <form id='previewForm'>
          <div class='grid'>
            <div><label>FIT file</label><br/><input type='file' name='fit_file' accept='.fit' required/></div>
            <div><label>Apple HR export</label><br/><input type='file' name='apple_file' required/></div>
          </div><br/>
          <label>Apple source type</label>
          <select name='apple_source_type'>
            <option value='auto'>Auto-detect</option>
            <option value='json'>JSON</option>
            <option value='csv'>CSV</option>
          </select>
          <button class='btn' type='submit'>Preview</button>
        </form>
        <hr style='border-color:#374151'/>
        <form id='runForm'>
          <input type='hidden' name='import_token' />
          <div class='grid'>
            <div><label>Tolerance (seconds)</label><br/><input type='number' name='tolerance_seconds' value='2' min='0' max='30'/></div>
            <div><label>Overwrite existing FIT HR</label><br/><select name='overwrite_existing_hr'><option value='false'>No</option><option value='true'>Yes</option></select></div>
            <div><label>Ignore implausible HR</label><br/><select name='ignore_implausible_hr'><option value='true'>Yes</option><option value='false'>No</option></select></div>
            <div><label>HR min / max</label><br/><input type='number' name='min_hr' value='30' min='1' max='240'/> <input type='number' name='max_hr' value='240' min='60' max='260'/></div>
          </div>
          <button class='btn' type='submit'>Run Merge</button>
        </form>
        <details id='appleDebugPanel' class='debug-panel'>
          <summary>Apple Parse Debug</summary>
          <pre id='appleDebugOutput' class='muted'>No parser debug available yet.</pre>
        </details>
        <pre id='output' class='muted'>Preview results will appear here.</pre>
      </div>
      <script>
        let importToken = null;
        const output = document.getElementById('output');
        const appleDebugOutput = document.getElementById('appleDebugOutput');
        const appleDebugPanel = document.getElementById('appleDebugPanel');

        const renderAppleDebug = (data) => {
          const appleDebug = data.apple_debug || (data.apple_summary && data.apple_summary.debug) || null;
          if (!appleDebug) {
            appleDebugOutput.textContent = 'No parser debug available for this response.';
            return;
          }
          const compact = {
            requested_source_type: appleDebug.requested_source_type ?? null,
            detected_source_type: appleDebug.detected_source_type ?? null,
            parser_mode: appleDebug.parser_mode ?? null,
            top_level_keys: appleDebug.top_level_keys ?? [],
            data_keys: appleDebug.data_keys ?? [],
            workouts_found: appleDebug.workouts_found ?? 0,
            selected_workout_index: appleDebug.selected_workout_index ?? null,
            selected_workout_id: appleDebug.selected_workout_id ?? null,
            selected_workout_has_heart_rate_data: appleDebug.selected_workout_has_heart_rate_data ?? false,
            selected_workout_heart_rate_point_count: appleDebug.selected_workout_heart_rate_point_count ?? 0,
            selected_workout_parseable_point_count: appleDebug.selected_workout_parseable_point_count ?? 0,
            fallback_workout_index: appleDebug.fallback_workout_index ?? null,
            raw_heart_rate_entries_found: appleDebug.raw_heart_rate_entries_found ?? 0,
            parsed_heart_rate_entries_count: appleDebug.parsed_heart_rate_entries_count ?? 0,
            rejected_entries_count: appleDebug.rejected_entries_count ?? 0,
            rejection_reasons: appleDebug.rejection_reasons ?? {},
            sample_preview: appleDebug.sample_preview ?? [],
            apple_bytes: appleDebug.apple_bytes ?? null,
            apple_text_preview: appleDebug.apple_text_preview ?? null
          };
          appleDebugOutput.textContent = JSON.stringify(compact, null, 2);
          appleDebugPanel.open = true;
        };

        document.getElementById('previewForm').addEventListener('submit', async (e) => {
          e.preventDefault();
          const fd = new FormData(e.target);
          const res = await fetch('/api/tools/fit-hr-merge/preview', { method: 'POST', body: fd });
          const data = await res.json();
          if (data.import_token) {
            importToken = data.import_token;
            document.querySelector('#runForm input[name="import_token"]').value = importToken;
          }
          renderAppleDebug(data);
          output.textContent = JSON.stringify(data, null, 2);
        });
        document.getElementById('runForm').addEventListener('submit', async (e) => {
          e.preventDefault();
          const form = new FormData(e.target);
          const payload = {
            import_token: form.get('import_token') || importToken,
            tolerance_seconds: Number(form.get('tolerance_seconds') || 2),
            overwrite_existing_hr: form.get('overwrite_existing_hr') === 'true',
            ignore_implausible_hr: form.get('ignore_implausible_hr') !== 'false',
            min_hr: Number(form.get('min_hr') || 30),
            max_hr: Number(form.get('max_hr') || 240)
          };
          const res = await fetch('/api/tools/fit-hr-merge/run', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify(payload)
          });
          const data = await res.json();
          renderAppleDebug(data);
          output.textContent = JSON.stringify(data, null, 2);
          if (data.download_url) {
            const a = document.createElement('a');
            a.href = data.download_url;
            a.textContent = 'Download merged output';
            a.style.color = '#86efac';
            output.parentNode.appendChild(a);
          }
        });
      </script>
    </body></html>
    """


def _render_preview_page(token: str, preview: dict) -> str:
    return f"""
    <html><body style='background:#111827;color:#e5e7eb;font-family:Arial;'>
    <div style='max-width:760px;margin:32px auto;background:#1f2937;border-radius:12px;padding:24px;'>
    <h2>Preview: {preview['source_file_name']}</h2>
    <ul>
      <li>Start: {preview['start_time']}</li><li>End: {preview['end_time']}</li>
      <li>Duration (s): {preview['duration_s']}</li><li>Distance (m): {preview['distance_m']}</li>
      <li>GPS: {preview['has_gps_track']}</li><li>Speed: {preview['has_speed']}</li>
      <li>Cadence: {preview['has_cadence']}</li><li>Power: {preview['has_power']}</li>
      <li>Heart rate: {preview['has_heart_rate']}</li><li>Samples: {preview['sample_count']}</li>
    </ul>
    <form action='/imports/fit/confirm' method='post'>
      <input type='hidden' name='import_token' value='{token}' />
      <button style='background:#2563eb;color:#fff;border:0;border-radius:8px;padding:10px 16px;' type='submit'>Confirm Import</button>
    </form>
    </div></body></html>
    """


class _Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, body: bytes, content_type: str = "text/html; charset=utf-8"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code: int, payload: dict):
        self._send(code, json.dumps(payload).encode("utf-8"), "application/json")

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/" or parsed.path == "/imports/fit":
            self._send(200, _render_upload_page().encode("utf-8"))
            return
        if parsed.path == "/tools/fit-hr-merge":
            self._send(200, _render_hr_merge_page().encode("utf-8"))
            return
        if parsed.path == "/tools/fit-hr-merge/download":
            token = parse_qs(parsed.query).get("token", [""])[0]
            try:
                item = _load_merged_artifact(token)
            except KeyError:
                self._send(404, b"Not found", "text/plain")
                return
            body = item["content"]
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Disposition", f"attachment; filename={item['filename']}")
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/health":
            self._json(200, {"status": "ok"})
            return
        self._send(404, b"Not found", "text/plain")

    def do_POST(self):
        if self.path in ("/api/imports/fit", "/imports/fit/preview"):
            form = cgi.FieldStorage(fp=self.rfile, headers=self.headers, environ={"REQUEST_METHOD": "POST", "CONTENT_TYPE": self.headers.get("Content-Type", "")})
            item = form["file"] if "file" in form else None
            if item is None or not getattr(item, "filename", ""):
                message = {"error": "Missing file"}
                if self.path.startswith("/api/"):
                    self._json(400, message)
                else:
                    self._send(400, b"Missing file", "text/plain")
                return
            try:
                result = _handle_fit_preview(item.filename, item.file.read())
            except FitImportError as exc:
                if self.path.startswith("/api/"):
                    self._json(400, {"error": str(exc)})
                else:
                    self._send(400, f"Invalid FIT file: {exc}".encode("utf-8"), "text/plain")
                return
            if self.path.startswith("/api/"):
                self._json(200, result)
            else:
                self._send(200, _render_preview_page(result["import_token"], result["preview"]).encode("utf-8"))
            return

        if self.path == "/api/tools/fit-hr-merge/preview":
            form = cgi.FieldStorage(fp=self.rfile, headers=self.headers, environ={"REQUEST_METHOD": "POST", "CONTENT_TYPE": self.headers.get("Content-Type", "")})
            fit_item = form["fit_file"] if "fit_file" in form else None
            apple_item = form["apple_file"] if "apple_file" in form else None
            source_type = form.getfirst("apple_source_type", "auto")
            if fit_item is None or apple_item is None:
                self._json(400, {"error": "Missing fit_file or apple_file"})
                return
            try:
                result = _handle_hr_merge_preview(
                    getattr(fit_item, "filename", ""),
                    fit_item.file.read(),
                    apple_item.file.read(),
                    source_type,
                )
            except (FitImportError, FitHrMergeError, ValueError) as exc:
                self._json(400, {"error": str(exc)})
                return
            self._json(200, result)
            return

        if self.path == "/api/tools/fit-hr-merge/run":
            raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            try:
                payload = json.loads(raw or b"{}")
            except json.JSONDecodeError:
                self._json(400, {"error": "Invalid JSON body"})
                return
            if not isinstance(payload, dict):
                self._json(400, {"error": "JSON body must be an object"})
                return
            try:
                options = parse_merge_options(payload)
                result = _run_hr_merge(payload.get("import_token", ""), options)
            except KeyError:
                self._json(404, {"error": "Unknown or expired import token"})
                return
            except (FitHrMergeError, FitImportError, ValueError) as exc:
                self._json(400, {"error": str(exc)})
                return
            self._json(200, result)
            return

        if self.path in ("/api/imports/fit/confirm", "/imports/fit/confirm"):
            if self.path.startswith("/api/"):
                raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                try:
                    payload = json.loads(raw or b"{}")
                except json.JSONDecodeError:
                    self._json(400, {"error": "Invalid JSON body"})
                    return
                if not isinstance(payload, dict):
                    self._json(400, {"error": "JSON body must be an object"})
                    return
                token = payload.get("import_token", "")
            else:
                raw = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
                token = parse_qs(raw).get("import_token", [""])[0]

            try:
                activity_id, sample_count = _save_import(token)
            except KeyError:
                if self.path.startswith("/api/"):
                    self._json(404, {"error": "Unknown or expired import token"})
                else:
                    self._send(404, b"Import token expired", "text/plain")
                return

            if self.path.startswith("/api/"):
                self._json(200, {"status": "imported", "activity_id": activity_id, "sample_count": sample_count})
            else:
                html = f"<body style='background:#111827;color:#86efac;font-family:Arial;padding:30px;'><h2>Import complete</h2><p>Activity {activity_id} saved ({sample_count} samples).</p><a href='/imports/fit'>Import another file</a></body>"
                self._send(200, html.encode("utf-8"))
            return

        self._send(404, b"Not found", "text/plain")


def run_server(host: str, port: int, debug: bool = False) -> None:
    """Run HTTP server. `debug` is an explicit opt-in compatibility flag."""
    if debug:
        print("[web] Debug mode enabled")
    server = ThreadingHTTPServer((host, port), _Handler)
    server.serve_forever()
