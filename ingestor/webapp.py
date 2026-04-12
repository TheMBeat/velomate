"""Minimal built-in HTTP server for FIT upload UI + REST endpoints."""

from __future__ import annotations

import cgi
import json
import uuid
from threading import RLock
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from db import get_connection
from fit_import import FitImportError, parse_fit_bytes, import_fit_payload
from hr_fit_merge import (
    FitHrMergeError,
    MergeOptions,
    merge_fit_with_hr,
    parse_apple_hr_payload,
    parse_fit_records_for_merge,
    render_merged_output_json,
    render_merged_output_fit,
)

_PENDING_IMPORTS: dict[str, dict] = {}
_PENDING_TTL = timedelta(minutes=30)
_PENDING_IMPORTS_LOCK = RLock()

_MERGED_ARTIFACTS: dict[str, dict] = {}
_MERGED_TTL = timedelta(minutes=30)
_MERGED_LOCK = RLock()


def _store_merged_artifact(filename: str, content: bytes, report: dict) -> str:
    with _MERGED_LOCK:
        now = datetime.now(timezone.utc)
        expired = [token for token, payload in _MERGED_ARTIFACTS.items() if now - payload["created_at"] > _MERGED_TTL]
        for token in expired:
            _MERGED_ARTIFACTS.pop(token, None)
        token = str(uuid.uuid4())
        _MERGED_ARTIFACTS[token] = {"created_at": now, "filename": filename, "content": content, "report": report}
        return token


def _load_merged_artifact(token: str) -> dict:
    with _MERGED_LOCK:
        item = _MERGED_ARTIFACTS.get(token)
        if not item:
            raise KeyError("Unknown artifact token")
        now = datetime.now(timezone.utc)
        if now - item["created_at"] > _MERGED_TTL:
            _MERGED_ARTIFACTS.pop(token, None)
            raise KeyError("Expired artifact token")
        return item


def _purge_pending() -> None:
    with _PENDING_IMPORTS_LOCK:
        now = datetime.now(timezone.utc)
        expired = [token for token, payload in _PENDING_IMPORTS.items() if now - payload["created_at"] > _PENDING_TTL]
        for token in expired:
            _PENDING_IMPORTS.pop(token, None)


def _store_pending(parsed: dict) -> str:
    with _PENDING_IMPORTS_LOCK:
        now = datetime.now(timezone.utc)
        expired = [token for token, payload in _PENDING_IMPORTS.items() if now - payload["created_at"] > _PENDING_TTL]
        for token in expired:
            _PENDING_IMPORTS.pop(token, None)
        token = str(uuid.uuid4())
        _PENDING_IMPORTS[token] = {"created_at": now, "parsed": parsed}
        return token


def _load_pending(token: str, pop: bool = False) -> dict:
    with _PENDING_IMPORTS_LOCK:
        pending = _PENDING_IMPORTS.get(token)
        if pending:
            now = datetime.now(timezone.utc)
            if now - pending["created_at"] > _PENDING_TTL:
                _PENDING_IMPORTS.pop(token, None)
                pending = None
            elif pop:
                pending = _PENDING_IMPORTS.pop(token, None)
    if not pending:
        raise KeyError("Unknown or expired import token")
    return pending["parsed"]


def _save_import(token: str) -> tuple[int, int]:
    parsed = _load_pending(token, pop=True)
    conn = get_connection()
    try:
        return import_fit_payload(conn, parsed, run_fitness_recalc=True)
    finally:
        conn.close()


def _handle_fit_preview(filename: str, content: bytes) -> dict:
    if not filename:
        raise FitImportError("Missing file")
    if not filename.lower().endswith(".fit"):
        raise FitImportError("Only .fit files are supported")
    parsed = parse_fit_bytes(content, filename)
    token = _store_pending(parsed)
    return {"import_token": token, "preview": parsed["preview"]}


def _handle_hr_merge_preview(fit_filename: str, fit_content: bytes, apple_content: bytes, apple_source_type: str) -> dict:
    if not fit_filename.lower().endswith(".fit"):
        raise FitHrMergeError("FIT input must end with .fit")
    fit_payload = parse_fit_records_for_merge(fit_content)
    apple_raw = parse_apple_hr_payload(apple_content, source_type=apple_source_type)

    fit_start = fit_payload["summary"]["start_time"]
    fit_end = fit_payload["summary"]["end_time"]
    overlap_count = sum(1 for row in apple_raw if fit_start <= row.get("timestamp", "") <= fit_end)

    token = _store_pending({
        "fit_filename": fit_filename,
        "fit_bytes": fit_content,
        "fit_records": fit_payload["records"],
        "apple_raw": apple_raw,
    })

    return {
        "import_token": token,
        "fit_summary": fit_payload["summary"],
        "apple_summary": {
            "point_count": len(apple_raw),
            "first_timestamp": apple_raw[0]["timestamp"] if apple_raw else None,
            "last_timestamp": apple_raw[-1]["timestamp"] if apple_raw else None,
        },
        "estimated_overlap_points": overlap_count,
        "warnings": [] if overlap_count else ["Low overlap between Apple HR and FIT timeline; verify timezone/export range."],
    }


def _run_hr_merge(import_token: str, options: MergeOptions) -> dict:
    payload = _load_pending(import_token, pop=False)
    merged_records, report = merge_fit_with_hr(payload["fit_records"], payload["apple_raw"], options)
    output_name, content, report = render_merged_output_fit(
        payload["fit_filename"], payload["fit_bytes"], merged_records, report
    )
    artifact_token = _store_merged_artifact(output_name, content, report)
    return {
        "artifact_token": artifact_token,
        "download_url": f"/tools/fit-hr-merge/download?token={artifact_token}",
        "report": report,
        "output_format": "fit",
    }




def _parse_bool_flag(value, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"true", "1", "yes", "on"}:
            return True
        if v in {"false", "0", "no", "off"}:
            return False
    raise ValueError("Boolean flag must be true/false")

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
        <pre id='output' class='muted'>Preview results will appear here.</pre>
      </div>
      <script>
        let importToken = null;
        const output = document.getElementById('output');
        document.getElementById('previewForm').addEventListener('submit', async (e) => {
          e.preventDefault();
          const fd = new FormData(e.target);
          const res = await fetch('/api/tools/fit-hr-merge/preview', { method: 'POST', body: fd });
          const data = await res.json();
          if (data.import_token) {
            importToken = data.import_token;
            document.querySelector('#runForm input[name="import_token"]').value = importToken;
          }
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
                options = MergeOptions(
                    tolerance_seconds=int(payload.get("tolerance_seconds", 2)),
                    overwrite_existing_hr=_parse_bool_flag(payload.get("overwrite_existing_hr", False), default=False),
                    ignore_implausible_hr=_parse_bool_flag(payload.get("ignore_implausible_hr", True), default=True),
                    min_hr=int(payload.get("min_hr", 30)),
                    max_hr=int(payload.get("max_hr", 240)),
                )
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
