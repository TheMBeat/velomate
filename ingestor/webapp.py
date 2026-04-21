"""Minimal built-in HTTP server for FIT upload UI + REST endpoints."""

from __future__ import annotations

import cgi
import json
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from time import perf_counter
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

from fit_import import FitImportError
from hr_fit_merge import (
    FitHrMergeError,
    MergeOptions,
)
from hr_merge_service import parse_merge_options, preview_merge, run_merge
from import_service import delete_imported_activity, persist_fit_import, preview_fit_import
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


def _delete_activity(activity_id: int) -> tuple[int, str]:
    return delete_imported_activity(activity_id)


def _handle_fit_preview(filename: str, content: bytes) -> dict:
    parsed = preview_fit_import(filename, content)
    token = _store_pending(parsed)
    return {"import_token": token, "preview": parsed["preview"]}


def _handle_hr_merge_preview(fit_filename: str, fit_content: bytes, apple_content: bytes, apple_source_type: str) -> dict:
    merge_payload, response = preview_merge(fit_filename, fit_content, apple_content, apple_source_type)
    apple_debug = dict(response.get("apple_debug", {}))
    apple_debug["requested_source_type"] = apple_source_type
    apple_debug["apple_bytes"] = len(apple_content)
    apple_debug["apple_text_preview"] = apple_content.decode("utf-8", errors="replace")[:200]
    response["apple_debug"] = apple_debug
    response["parser_diagnostics"] = dict(response.get("parser_diagnostics", {}))
    response["parser_diagnostics"]["apple_debug_present"] = True
    merge_payload["apple_debug"] = apple_debug
    token = _store_pending(merge_payload)
    return {"import_token": token, **response}


def _run_hr_merge(import_token: str, options: MergeOptions) -> dict:
    started = perf_counter()
    payload = _load_pending(import_token, pop=False)
    output_name, content, report = run_merge(payload, options)
    artifact_token = _store_merged_artifact(output_name, content, report)
    return {
        "artifact_token": artifact_token,
        "download_url": f"/tools/fit-hr-merge/download?token={artifact_token}",
        "import_url": "/api/tools/fit-hr-merge/import",
        "report": report,
        "apple_debug": payload.get("apple_debug", {}),
        "output_format": "fit",
        "writer_diagnostics": report.get("writer_diagnostics", {}),
        "timing_ms": {
            "service_total": round((perf_counter() - started) * 1000, 3),
            **report.get("timing_ms", {}),
        },
    }


def _import_merged_artifact(artifact_token: str) -> dict:
    item = _load_merged_artifact(artifact_token)
    parsed = preview_fit_import(item["filename"], item["content"])
    activity_id, sample_count = persist_fit_import(parsed)
    return {
        "status": "imported",
        "activity_id": activity_id,
        "sample_count": sample_count,
        "filename": item["filename"],
        "preview": parsed.get("preview", {}),
        "report": item.get("report", {}),
    }


def _render_upload_page() -> str:
    return """
    <html><head><title>VeloMate FIT Import</title>
    <style>
      body{background:#111827;color:#e5e7eb;font-family:Arial}
      .card{max-width:760px;margin:32px auto;background:#1f2937;border-radius:12px;padding:24px}
      .btn{background:#2563eb;color:#fff;border:0;border-radius:8px;padding:10px 16px}
      .btn-danger{background:#dc2626}
      input{background:#111827;color:#e5e7eb;border:1px solid #374151;border-radius:6px;padding:8px}
    </style>
    </head><body><div class='card'>
    <h1>Import FIT file</h1>
    <form action='/imports/fit/preview' method='post' enctype='multipart/form-data'>
      <input type='file' name='file' accept='.fit' required/><br/><br/>
      <button class='btn' type='submit'>Upload & Preview</button>
    </form>

    <p><a href='/tools/fit-hr-merge' style='color:#93c5fd'>Open Apple HR + FIT merger tool</a></p>

    <hr style='border-color:#374151;margin:24px 0' />

    <h2>Delete imported activity</h2>
    <form id='deleteForm'>
      <input type='number' name='activity_id' placeholder='Activity ID' min='1' required />
      <button class='btn btn-danger' type='submit'>Delete activity</button>
    </form>
    <pre id='deleteOutput' style='white-space:pre-wrap;background:#111827;border:1px solid #374151;padding:12px;border-radius:8px;margin-top:12px;'></pre>

    <script>
      document.getElementById('deleteForm').addEventListener('submit', async (e) => {
        e.preventDefault();
        const output = document.getElementById('deleteOutput');
        try {
          const fd = new FormData(e.target);
          const activityId = Number(fd.get('activity_id'));
          const res = await fetch('/api/activities/delete', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ activity_id: activityId })
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || `Delete failed with status ${res.status}`);
          output.textContent = JSON.stringify(data, null, 2);
        } catch (error) {
          output.textContent = `Delete error: ${error.message}`;
        }
      });
    </script>
    </div></body></html>
    """


def _render_hr_merge_page() -> str:
    return """
    <html><head><title>VeloMate Apple HR + FIT Merger</title>
    <style>
      body{background:#111827;color:#e5e7eb;font-family:Arial;margin:0;padding:20px}
      .card{max-width:920px;margin:20px auto;background:#1f2937;border-radius:12px;padding:20px}
      .grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
      .btn{background:#2563eb;color:#fff;border:0;border-radius:8px;padding:10px 14px;margin-top:10px;margin-right:8px}
      .btn-secondary{background:#059669}
      .btn-danger{background:#dc2626}
      .muted{color:#9ca3af}
      .success{color:#86efac}
      .debug-panel{margin-top:12px;border:1px solid #374151;border-radius:8px;background:#111827}
      .debug-panel summary{cursor:pointer;padding:10px 12px;font-weight:600}
      .debug-panel pre{margin:0 12px 12px 12px}
      input,select{background:#111827;color:#e5e7eb;border:1px solid #374151;border-radius:6px;padding:6px}
      pre{white-space:pre-wrap;background:#111827;border:1px solid #374151;padding:12px;border-radius:8px}
      #actions{margin-top:12px}
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
          </select>
          <button class='btn' type='submit'>Preview</button>
        </form>
        <hr style='border-color:#374151'/>
        <form id='runForm'>
          <input type='hidden' name='import_token' />
          <div class='grid'>
            <div><label>Overwrite existing FIT HR</label><br/><select name='overwrite_existing_hr'><option value='false'>No</option><option value='true'>Yes</option></select></div>
            <div><label>Ignore implausible HR</label><br/><select name='ignore_implausible_hr'><option value='true'>Yes</option><option value='false'>No</option></select></div>
            <div><label>HR min / max</label><br/><input type='number' name='min_hr' value='30' min='1' max='240'/> <input type='number' name='max_hr' value='240' min='60' max='260'/></div>
          </div>
          <button class='btn' type='submit'>Run Merge</button>
        </form>
        <div id='actions'></div>
        <details id='appleDebugPanel' class='debug-panel'>
          <summary>Apple Parse Debug</summary>
          <pre id='appleDebugOutput' class='muted'>No parser debug available yet.</pre>
        </details>
        <pre id='output' class='muted'>Preview results will appear here.</pre>
      </div>
      <script>
        let importToken = null;
        let artifactToken = null;
        let importedActivityId = null;

        const output = document.getElementById('output');
        const appleDebugOutput = document.getElementById('appleDebugOutput');
        const appleDebugPanel = document.getElementById('appleDebugPanel');
        const actions = document.getElementById('actions');

        const clearActions = () => { actions.innerHTML = ''; };

        const renderAppleDebug = (data) => {
          const appleDebug = data.apple_debug || null;
          if (!appleDebug) {
            appleDebugOutput.textContent = 'No parser debug available for this response.';
            return;
          }
          appleDebugOutput.textContent = JSON.stringify(appleDebug, null, 2);
          appleDebugPanel.open = true;
        };

        const renderDeleteButton = (activityId) => {
          const deleteBtn = document.createElement('button');
          deleteBtn.type = 'button';
          deleteBtn.className = 'btn btn-danger';
          deleteBtn.textContent = `Delete activity ${activityId}`;

          deleteBtn.addEventListener('click', async () => {
            try {
              const res = await fetch('/api/activities/delete', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ activity_id: activityId })
              });
              const data = await res.json();
              if (!res.ok) throw new Error(data.error || `Delete failed with status ${res.status}`);
              output.textContent = JSON.stringify(data, null, 2);
            } catch (error) {
              output.textContent = `Delete error: ${error.message}`;
            }
          });

          actions.appendChild(deleteBtn);
        };

        const renderActions = (data) => {
          clearActions();
          if (!data || !data.artifact_token) return;

          artifactToken = data.artifact_token;

          const download = document.createElement('a');
          download.href = data.download_url;
          download.textContent = 'Download merged output';
          download.className = 'btn';
          download.style.display = 'inline-block';
          download.style.textDecoration = 'none';
          download.style.color = '#fff';
          actions.appendChild(download);

          const importBtn = document.createElement('button');
          importBtn.type = 'button';
          importBtn.className = 'btn btn-secondary';
          importBtn.textContent = 'Import merged FIT';

          importBtn.addEventListener('click', async () => {
            try {
              const res = await fetch('/api/tools/fit-hr-merge/import', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ artifact_token: artifactToken })
              });
              const data = await res.json();
              if (!res.ok) throw new Error(data.error || `Import failed with status ${res.status}`);
              output.textContent = JSON.stringify(data, null, 2);
              importedActivityId = data.activity_id;
              renderDeleteButton(importedActivityId);
            } catch (error) {
              output.textContent = `Import error: ${error.message}`;
            }
          });

          actions.appendChild(importBtn);
        };

        document.getElementById('previewForm').addEventListener('submit', async (e) => {
          e.preventDefault();
          clearActions();
          artifactToken = null;
          importedActivityId = null;

          try {
            const fd = new FormData(e.target);
            const res = await fetch('/api/tools/fit-hr-merge/preview', { method: 'POST', body: fd });
            const data = await res.json();
            if (!res.ok) throw new Error(data.error || `Preview failed with status ${res.status}`);

            if (data.import_token) {
              importToken = data.import_token;
              document.querySelector('#runForm input[name="import_token"]').value = importToken;
            }

            renderAppleDebug(data);
            output.textContent = JSON.stringify(data, null, 2);
          } catch (error) {
            output.textContent = `Preview error: ${error.message}`;
          }
        });

        document.getElementById('runForm').addEventListener('submit', async (e) => {
          e.preventDefault();
          clearActions();

          try {
            const form = new FormData(e.target);
            const token = form.get('import_token') || importToken;
            if (!token) throw new Error('import_token missing: run preview first');

            const payload = {
              import_token: token,
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
            if (!res.ok) throw new Error(data.error || `Run failed with status ${res.status}`);

            renderAppleDebug(data);
            output.textContent = JSON.stringify(data, null, 2);
            renderActions(data);
          } catch (error) {
            output.textContent = `Run error: ${error.message}`;
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
            form = cgi.FieldStorage(
                fp=self.rfile,
                headers=self.headers,
                environ={"REQUEST_METHOD": "POST", "CONTENT_TYPE": self.headers.get("Content-Type", "")},
            )
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
            trace_id = str(uuid4())
            form = cgi.FieldStorage(
                fp=self.rfile,
                headers=self.headers,
                environ={"REQUEST_METHOD": "POST", "CONTENT_TYPE": self.headers.get("Content-Type", "")},
            )
            fit_item = form["fit_file"] if "fit_file" in form else None
            apple_item = form["apple_file"] if "apple_file" in form else None
            source_type = form.getfirst("apple_source_type", "auto")
            if fit_item is None or apple_item is None:
                self._json(400, {"error": "Missing fit_file or apple_file", "trace_id": trace_id})
                return
            try:
                result = _handle_hr_merge_preview(
                    getattr(fit_item, "filename", ""),
                    fit_item.file.read(),
                    apple_item.file.read(),
                    source_type,
                )
            except (FitImportError, FitHrMergeError, ValueError) as exc:
                self._json(400, {"error": str(exc), "trace_id": trace_id})
                return
            result["trace_id"] = trace_id
            self._json(200, result)
            return

        if self.path == "/api/tools/fit-hr-merge/run":
            trace_id = str(uuid4())
            raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            try:
                payload = json.loads(raw or b"{}")
            except json.JSONDecodeError:
                self._json(400, {"error": "Invalid JSON body", "trace_id": trace_id})
                return
            if not isinstance(payload, dict):
                self._json(400, {"error": "JSON body must be an object", "trace_id": trace_id})
                return
            try:
                options = parse_merge_options(payload)
                result = _run_hr_merge(payload.get("import_token", ""), options)
            except KeyError:
                self._json(404, {"error": "Unknown or expired import token", "trace_id": trace_id})
                return
            except (FitHrMergeError, FitImportError, ValueError) as exc:
                self._json(400, {"error": str(exc), "trace_id": trace_id})
                return
            result["trace_id"] = trace_id
            if result.get("artifact_token"):
                result["filename"] = _load_merged_artifact(result["artifact_token"]).get("filename")
            self._json(200, result)
            return

        if self.path == "/api/tools/fit-hr-merge/import":
            trace_id = str(uuid4())
            raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            try:
                payload = json.loads(raw or b"{}")
            except json.JSONDecodeError:
                self._json(400, {"error": "Invalid JSON body", "trace_id": trace_id})
                return
            if not isinstance(payload, dict):
                self._json(400, {"error": "JSON body must be an object", "trace_id": trace_id})
                return

            artifact_token = payload.get("artifact_token", "")
            if not artifact_token:
                self._json(400, {"error": "artifact_token missing", "trace_id": trace_id})
                return

            try:
                result = _import_merged_artifact(artifact_token)
            except KeyError:
                self._json(404, {"error": "Unknown artifact token", "trace_id": trace_id})
                return
            except (FitImportError, ValueError) as exc:
                self._json(400, {"error": str(exc), "trace_id": trace_id})
                return

            result["trace_id"] = trace_id
            self._json(200, result)
            return

        if self.path == "/api/activities/delete":
            trace_id = str(uuid4())
            raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            try:
                payload = json.loads(raw or b"{}")
            except json.JSONDecodeError:
                self._json(400, {"error": "Invalid JSON body", "trace_id": trace_id})
                return
            if not isinstance(payload, dict):
                self._json(400, {"error": "JSON body must be an object", "trace_id": trace_id})
                return

            try:
                activity_id = int(payload.get("activity_id", 0))
            except (TypeError, ValueError):
                self._json(400, {"error": "activity_id must be an integer", "trace_id": trace_id})
                return

            if activity_id <= 0:
                self._json(400, {"error": "activity_id must be > 0", "trace_id": trace_id})
                return

            try:
                deleted_id, deleted_name = _delete_activity(activity_id)
            except KeyError:
                self._json(404, {"error": "Activity not found", "trace_id": trace_id})
                return
            except ValueError as exc:
                self._json(400, {"error": str(exc), "trace_id": trace_id})
                return

            self._json(
                200,
                {
                    "status": "deleted",
                    "activity_id": deleted_id,
                    "name": deleted_name,
                    "trace_id": trace_id,
                },
            )
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
                html = f"""
                <body style='background:#111827;color:#86efac;font-family:Arial;padding:30px;'>
                  <h2>Import complete</h2>
                  <p>Activity {activity_id} saved ({sample_count} samples).</p>
                  <form method="post" action="/api/activities/delete" onsubmit="event.preventDefault(); fetch('/api/activities/delete', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body:JSON.stringify({{activity_id:{activity_id}}})}}).then(r=>r.json()).then(d=>document.getElementById('deleteResult').textContent=JSON.stringify(d,null,2)).catch(e=>document.getElementById('deleteResult').textContent=e.message);">
                    <button style='background:#dc2626;color:#fff;border:0;border-radius:8px;padding:10px 16px;' type='submit'>Delete this activity</button>
                  </form>
                  <pre id="deleteResult" style="white-space:pre-wrap;background:#111827;border:1px solid #374151;padding:12px;border-radius:8px;margin-top:12px;"></pre>
                  <p><a href='/imports/fit'>Import another file</a></p>
                </body>
                """
                self._send(200, html.encode("utf-8"))
            return

        self._send(404, b"Not found", "text/plain")


def run_server(host: str, port: int, debug: bool = False) -> None:
    """Run HTTP server."""
    if debug:
        print("[web] Debug mode enabled")

    server = ThreadingHTTPServer((host, port), _Handler)
    print(f"[web] Server running on http://{host}:{port}")
    server.serve_forever()
