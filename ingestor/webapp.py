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
        "report": report,
        "apple_debug": payload.get("apple_debug", {}),
        "output_format": "fit",
        "writer_diagnostics": report.get("writer_diagnostics", {}),
        "timing_ms": {
            "service_total": round((perf_counter() - started) * 1000, 3),
            **report.get("timing_ms", {}),
        },
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
        const createRunId = () => {
          if (window.crypto && typeof window.crypto.randomUUID === 'function') return window.crypto.randomUUID();
          return `run-${Date.now()}-${Math.random().toString(16).slice(2)}`;
        };
        const pad = (num) => String(num).padStart(2, '0');
        const tracePrefix = (runId, stepId, level='TRACE') => `[FIT-HR-Merge][${level}][${runId}][STEP ${pad(stepId)}]`;
        const excerpt = (value) => String(value ?? '').slice(0, 400);
        const traceError = (runId, stepId, title, error, extras={}) => {
          const details = {
            error_name: error?.name || 'Error',
            message: error?.message || String(error),
            stack: excerpt(error?.stack || ''),
            http_status: extras.http_status ?? null,
            response_body_excerpt: excerpt(extras.response_body_excerpt || '')
          };
          console.error(`${tracePrefix(runId, stepId, 'ERROR')} ${title} | ${Object.entries(details).map(([k,v])=>`${k}=${JSON.stringify(v)}`).join(', ')}`);
        };
        const logStart = (runId, stepId, title, meta={}) => {
          console.time(`${runId}-step-${pad(stepId)}`);
          console.log(`${tracePrefix(runId, stepId)} ${title} | phase=start, ${Object.entries(meta).map(([k,v])=>`${k}=${JSON.stringify(v)}`).join(', ')}`);
        };
        const logSuccess = (runId, stepId, title, meta={}) => {
          console.timeEnd(`${runId}-step-${pad(stepId)}`);
          console.log(`${tracePrefix(runId, stepId)} ${title} | phase=success, ${Object.entries(meta).map(([k,v])=>`${k}=${JSON.stringify(v)}`).join(', ')}`);
        };

        const renderAppleDebug = (data) => {
          const appleDebug = data.apple_debug || null;
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
            fallback_workout_id: appleDebug.fallback_workout_id ?? null,
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
          const runId = createRunId();
          console.groupCollapsed(`[FIT-HR-Merge][PREVIEW][${runId}]`);
          try {
            logStart(runId, 1, 'UI input validation started');
            const fitFile = e.target.querySelector('input[name="fit_file"]').files?.[0];
            const appleFile = e.target.querySelector('input[name="apple_file"]').files?.[0];
            if (!fitFile || !appleFile) throw new Error('Missing fit_file or apple_file');
            logSuccess(runId, 1, 'UI input validation started', { fit_name: fitFile.name, apple_name: appleFile.name });
            logStart(runId, 2, 'UI input validation passed');
            logSuccess(runId, 2, 'UI input validation passed');
            logStart(runId, 3, 'build FormData started');
            const fd = new FormData(e.target);
            logSuccess(runId, 3, 'build FormData started');
            logStart(runId, 4, 'build FormData done (fit_name, apple_name, apple_source_type, sizes)', {
              fit_name: fitFile.name, apple_name: appleFile.name, apple_source_type: fd.get('apple_source_type'),
              fit_size: fitFile.size, apple_size: appleFile.size
            });
            fd.append('run_id', runId);
            logSuccess(runId, 4, 'build FormData done (fit_name, apple_name, apple_source_type, sizes)');
            logStart(runId, 5, 'send preview request started (endpoint, method)', { endpoint: '/api/tools/fit-hr-merge/preview', method: 'POST' });
            const started = performance.now();
            const res = await fetch('/api/tools/fit-hr-merge/preview', { method: 'POST', body: fd });
            logSuccess(runId, 5, 'send preview request started (endpoint, method)');
            logStart(runId, 6, 'preview response received (status, duration_ms)');
            const rawBody = await res.text();
            const durationMs = Math.round(performance.now() - started);
            logSuccess(runId, 6, 'preview response received (status, duration_ms)', { status: res.status, duration_ms: durationMs });
            logStart(runId, 7, 'preview response json parse started');
            let data;
            try { data = JSON.parse(rawBody || '{}'); } catch (err) { traceError(runId, 7, 'preview response json parse started', err, { http_status: res.status, response_body_excerpt: rawBody }); throw err; }
            logSuccess(runId, 7, 'preview response json parse started');
            logStart(runId, 8, 'preview response json parse done');
            logSuccess(runId, 8, 'preview response json parse done', { trace_id: data.trace_id || null, run_id: data.run_id || runId });
            if (!res.ok) throw new Error(data.error || `Preview failed with status ${res.status}`);
            logStart(runId, 9, 'preview payload sanity check started');
            if (!data.apple_summary || !data.fit_summary) throw new Error('Missing apple_summary or fit_summary in preview response');
            logSuccess(runId, 9, 'preview payload sanity check started');
            logStart(runId, 10, 'preview payload sanity check done (point_count, overlap, warnings_count)');
            logSuccess(runId, 10, 'preview payload sanity check done (point_count, overlap, warnings_count)', {
              point_count: data.apple_summary?.point_count ?? 0,
              overlap: data.estimated_overlap_points ?? 0,
              warnings_count: (data.warnings || []).length
            });
            logStart(runId, 11, 'apple_debug render started');
            renderAppleDebug(data);
            logSuccess(runId, 11, 'apple_debug render started');
            logStart(runId, 12, 'apple_debug render done');
            logSuccess(runId, 12, 'apple_debug render done');
            if (data.import_token) {
              importToken = data.import_token;
              document.querySelector('#runForm input[name="import_token"]').value = importToken;
            }
            output.textContent = JSON.stringify(data, null, 2);
            logStart(runId, 13, 'preview UI render done');
            console.table([{run_id: runId, import_token: data.import_token || null, warnings: (data.warnings || []).length, point_count: data.apple_summary?.point_count ?? 0}]);
            logSuccess(runId, 13, 'preview UI render done', { import_token: data.import_token || null });
          } catch (error) {
            traceError(runId, 13, 'preview UI render done', error);
            output.textContent = `Preview error: ${error.message}`;
          } finally {
            console.groupEnd();
          }
        });
        document.getElementById('runForm').addEventListener('submit', async (e) => {
          e.preventDefault();
          const runId = createRunId();
          console.groupCollapsed(`[FIT-HR-Merge][RUN][${runId}]`);
          try {
            logStart(runId, 14, 'run form validation started');
            const form = new FormData(e.target);
            const token = form.get('import_token') || importToken;
            if (!token) throw new Error('import_token missing: run preview first');
            logSuccess(runId, 14, 'run form validation started', { import_token: token });
            logStart(runId, 15, 'run form validation passed');
            logSuccess(runId, 15, 'run form validation passed');
            logStart(runId, 16, 'build run payload started');
            const payload = {
              run_id: runId,
              import_token: token,
              overwrite_existing_hr: form.get('overwrite_existing_hr') === 'true',
              ignore_implausible_hr: form.get('ignore_implausible_hr') !== 'false',
              min_hr: Number(form.get('min_hr') || 30),
              max_hr: Number(form.get('max_hr') || 240)
            };
            logSuccess(runId, 16, 'build run payload started');
            logStart(runId, 17, 'build run payload done (import_token, options)', {
              import_token: payload.import_token,
              options: { overwrite_existing_hr: payload.overwrite_existing_hr, ignore_implausible_hr: payload.ignore_implausible_hr, min_hr: payload.min_hr, max_hr: payload.max_hr}
            });
            logSuccess(runId, 17, 'build run payload done (import_token, options)');
            logStart(runId, 18, 'send run request started');
            const started = performance.now();
            const res = await fetch('/api/tools/fit-hr-merge/run', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload) });
            logSuccess(runId, 18, 'send run request started');
            logStart(runId, 19, 'run response received (status, duration_ms)');
            const rawBody = await res.text();
            logSuccess(runId, 19, 'run response received (status, duration_ms)', { status: res.status, duration_ms: Math.round(performance.now() - started) });
            logStart(runId, 20, 'run response json parse started');
            let data;
            try { data = JSON.parse(rawBody || '{}'); } catch (err) { traceError(runId, 20, 'run response json parse started', err, { http_status: res.status, response_body_excerpt: rawBody }); throw err; }
            logSuccess(runId, 20, 'run response json parse started');
            logStart(runId, 21, 'run response json parse done');
            logSuccess(runId, 21, 'run response json parse done', { trace_id: data.trace_id || null, run_id: data.run_id || runId });
            if (!res.ok) throw new Error(data.error || `Run failed with status ${res.status}`);
            logStart(runId, 22, 'run report sanity check started');
            if (!data.report) throw new Error('run response missing report');
            logSuccess(runId, 22, 'run report sanity check started');
            logStart(runId, 23, 'run report sanity check done (matched, written, patched, coverage, missing_after)');
            const rpt = data.report || {};
            logSuccess(runId, 23, 'run report sanity check done (matched, written, patched, coverage, missing_after)', {
              matched: rpt.hr_points_matched ?? 0, written: rpt.hr_points_written ?? 0,
              patched: rpt.fit_records_patched_in_binary ?? 0, coverage: rpt.coverage_pct ?? 0,
              missing_after: rpt.records_missing_hr_after_merge ?? 0
            });
            renderAppleDebug(data);
            output.textContent = JSON.stringify(data, null, 2);
            logStart(runId, 24, 'download link render started');
            if (data.download_url) {
              const existing = document.getElementById('fitHrMergeDownloadLink');
              if (existing) existing.remove();
              const a = document.createElement('a');
              a.id = 'fitHrMergeDownloadLink';
              a.href = data.download_url;
              a.textContent = 'Download merged output';
              a.style.color = '#86efac';
              a.dataset.filename = data.filename || 'merged_hr.fit';
              a.addEventListener('click', async (evt) => {
                evt.preventDefault();
                const dlRunId = createRunId();
                console.groupCollapsed(`[FIT-HR-Merge][DOWNLOAD][${dlRunId}]`);
                try {
                  logStart(dlRunId, 26, 'download started (artifact_token/url)', { artifact_token: data.artifact_token || null, url: data.download_url });
                  const response = await fetch(data.download_url);
                  logSuccess(dlRunId, 26, 'download started (artifact_token/url)');
                  logStart(dlRunId, 27, 'download response received (status, content-length, content-type)');
                  logSuccess(dlRunId, 27, 'download response received (status, content-length, content-type)', {
                    status: response.status, content_length: response.headers.get('content-length') || null, content_type: response.headers.get('content-type') || null
                  });
                  if (!response.ok) throw new Error(`Download failed with status ${response.status}`);
                  logStart(dlRunId, 28, 'blob creation started');
                  const blob = await response.blob();
                  logSuccess(dlRunId, 28, 'blob creation started');
                  logStart(dlRunId, 29, 'blob creation done');
                  logSuccess(dlRunId, 29, 'blob creation done', { blob_size: blob.size });
                  const blobUrl = URL.createObjectURL(blob);
                  const save = document.createElement('a');
                  const filename = data.filename || 'merged_hr.fit';
                  save.href = blobUrl;
                  save.download = filename;
                  logStart(dlRunId, 30, 'file save triggered (filename)', { filename });
                  save.click();
                  logSuccess(dlRunId, 30, 'file save triggered (filename)', { filename });
                  setTimeout(() => URL.revokeObjectURL(blobUrl), 2000);
                } catch (error) {
                  traceError(dlRunId, 30, 'file save triggered (filename)', error);
                } finally {
                  console.groupEnd();
                }
              });
              output.parentNode.appendChild(a);
            }
            logSuccess(runId, 24, 'download link render started', { artifact_token: data.artifact_token || null, filename: data.filename || null });
            logStart(runId, 25, 'download link render done');
            console.table([{ run_id: runId, artifact_token: data.artifact_token || null, written: data.report?.hr_points_written ?? 0, patched: data.report?.fit_records_patched_in_binary ?? 0 }]);
            logSuccess(runId, 25, 'download link render done');
          } catch (error) {
            traceError(runId, 25, 'download link render done', error);
            output.textContent = `Run error: ${error.message}`;
          } finally {
            console.groupEnd();
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
            trace_id = str(uuid4())
            form = cgi.FieldStorage(fp=self.rfile, headers=self.headers, environ={"REQUEST_METHOD": "POST", "CONTENT_TYPE": self.headers.get("Content-Type", "")})
            fit_item = form["fit_file"] if "fit_file" in form else None
            apple_item = form["apple_file"] if "apple_file" in form else None
            source_type = form.getfirst("apple_source_type", "auto")
            run_id = form.getfirst("run_id", "") or str(uuid4())
            if fit_item is None or apple_item is None:
                self._json(400, {"error": "Missing fit_file or apple_file", "trace_id": trace_id, "run_id": run_id})
                return
            started = perf_counter()
            try:
                result = _handle_hr_merge_preview(
                    getattr(fit_item, "filename", ""),
                    fit_item.file.read(),
                    apple_item.file.read(),
                    source_type,
                )
            except (FitImportError, FitHrMergeError, ValueError) as exc:
                self._json(400, {"error": str(exc), "trace_id": trace_id, "run_id": run_id})
                return
            result["trace_id"] = trace_id
            result["run_id"] = run_id
            result["timing_ms"] = {
                **result.get("timing_ms", {}),
                "http_handler_total": round((perf_counter() - started) * 1000, 3),
            }
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
            run_id = payload.get("run_id", "") or str(uuid4())
            started = perf_counter()
            try:
                options = parse_merge_options(payload)
                result = _run_hr_merge(payload.get("import_token", ""), options)
            except KeyError:
                self._json(404, {"error": "Unknown or expired import token", "trace_id": trace_id, "run_id": run_id})
                return
            except (FitHrMergeError, FitImportError, ValueError) as exc:
                self._json(400, {"error": str(exc), "trace_id": trace_id, "run_id": run_id})
                return
            result["trace_id"] = trace_id
            result["run_id"] = run_id
            if result.get("artifact_token"):
                result["filename"] = _load_merged_artifact(result["artifact_token"]).get("filename")
            result["timing_ms"] = {
                **result.get("timing_ms", {}),
                "http_handler_total": round((perf_counter() - started) * 1000, 3),
            }
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
