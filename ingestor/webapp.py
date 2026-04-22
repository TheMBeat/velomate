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
    <html><head><title>VeloMate</title>
    <style>
      :root{--bg:#0f172a;--card:#1e293b;--muted:#94a3b8;--text:#e2e8f0;--line:#334155;--blue:#2563eb;--green:#16a34a;--red:#dc2626}
      *{box-sizing:border-box}
      body{margin:0;background:linear-gradient(160deg,#0b1220 0%,#0f172a 100%);color:var(--text);font-family:Arial,sans-serif}
      .shell{max-width:980px;margin:24px auto;padding:0 16px}
      .nav{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px}
      .chip{display:inline-block;padding:7px 10px;background:#111827;border:1px solid var(--line);border-radius:999px;color:var(--muted);font-size:12px}
      .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px}
      .card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:18px}
      .hero{padding:20px;margin-bottom:16px}
      h1,h2,h3{margin:0 0 10px 0}
      p{margin:0 0 14px 0;color:var(--muted);line-height:1.45}
      .btn{cursor:pointer;background:var(--blue);color:#fff;border:0;border-radius:9px;padding:10px 14px;font-weight:600}
      .btn-danger{background:var(--red)}
      .btn:disabled{opacity:.65;cursor:not-allowed}
      input{width:100%;background:#0f172a;color:var(--text);border:1px solid var(--line);border-radius:8px;padding:10px}
      .status{display:none;margin-top:12px;padding:10px;border:1px solid var(--line);border-radius:10px;background:#0f172a}
      .status.active{display:block}
      .spinner{width:16px;height:16px;display:inline-block;border:2px solid #1d4ed8;border-top-color:#bfdbfe;border-radius:50%;animation:spin 1s linear infinite;margin-right:8px}
      .progress{margin-top:10px;height:6px;background:#111827;border-radius:8px;overflow:hidden}
      .progress > span{display:block;height:100%;width:35%;background:linear-gradient(90deg,#1d4ed8,#60a5fa);animation:slide 1.4s ease-in-out infinite}
      .success{color:#86efac}
      .danger{color:#fca5a5}
      pre{white-space:pre-wrap;background:#0f172a;border:1px solid var(--line);padding:12px;border-radius:8px;margin-top:8px}
      @keyframes spin{to{transform:rotate(360deg)}}
      @keyframes slide{0%{transform:translateX(-120%)}100%{transform:translateX(360%)}}
    </style>
    </head><body><div class='shell'>
      <div class='card hero'>
        <h1>VeloMate</h1>
        <p>Import, merge und verwalte FIT-Aktivitäten in einem konsistenten Workflow.</p>
        <div class='nav'>
          <span class='chip'>Übersicht (geplant)</span>
          <span class='chip'>Import</span>
          <span class='chip'>Merge</span>
          <span class='chip'>Aktivitäten</span>
          <span class='chip'>Analyse / Grafana (später)</span>
        </div>
      </div>
      <div class='grid'>
        <section class='card'>
          <h2>FIT importieren</h2>
          <p>Datei wählen, Vorschau prüfen, danach importieren.</p>
          <form action='/imports/fit/preview' method='post' enctype='multipart/form-data'>
            <input type='file' name='file' accept='.fit' required/><br/><br/>
            <button class='btn' type='submit'>Datei auswählen & Vorschau</button>
          </form>
        </section>
        <section class='card'>
          <h2>Apple HR + FIT mergen</h2>
          <p>Heart-Rate aus Apple exportieren und direkt in FIT einfügen.</p>
          <a href='/tools/fit-hr-merge' class='btn' style='display:inline-block;text-decoration:none'>Merge-Tool öffnen</a>
        </section>
        <section class='card'>
          <h2>Aktivität löschen</h2>
          <p>Aktivität-ID eingeben, bestätigen und sicher löschen.</p>
          <form id='deleteForm'>
            <input type='number' name='activity_id' placeholder='Activity ID' min='1' required />
            <div style='margin-top:10px'>
              <button id='deleteBtn' class='btn btn-danger' type='submit'>Delete activity</button>
            </div>
          </form>
          <div id='deleteStatus' class='status'>
            <div><span class='spinner'></span><strong id='deleteStatusText'>Löschen wird vorbereitet</strong></div>
            <div class='progress'><span></span></div>
          </div>
          <pre id='deleteOutput'>Noch kein Löschvorgang gestartet.</pre>
        </section>
      </div>

    <script>
      const deleteForm = document.getElementById('deleteForm');
      const deleteBtn = document.getElementById('deleteBtn');
      const deleteStatus = document.getElementById('deleteStatus');
      const deleteStatusText = document.getElementById('deleteStatusText');
      const deleteOutput = document.getElementById('deleteOutput');
      const renderSafeText = (element, value) => {
        element.textContent = String(value ?? '');
      };

      const setDeleteLoading = (active, message='') => {
        deleteBtn.disabled = active;
        deleteStatus.classList.toggle('active', active);
        if (message) deleteStatusText.textContent = message;
      };

      document.getElementById('deleteForm').addEventListener('submit', async (e) => {
        e.preventDefault();
        const fd = new FormData(deleteForm);
        const activityId = Number(fd.get('activity_id'));
        if (!confirm(`Aktivität ${activityId} wirklich löschen?`)) return;
        try {
          setDeleteLoading(true, 'Aktivität wird gelöscht');
          const res = await fetch('/api/activities/delete', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ activity_id: activityId })
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || `Delete failed with status ${res.status}`);
          renderSafeText(deleteOutput, `Erfolgreich gelöscht: Activity ${data.activity_id}\\n\\n${JSON.stringify(data, null, 2)}`);
        } catch (error) {
          renderSafeText(deleteOutput, `Delete error: ${error.message}`);
        } finally {
          setDeleteLoading(false);
        }
      });
    </script>
    </div></body></html>
    """


def _render_hr_merge_page() -> str:
    return """
    <html><head><title>VeloMate Apple HR + FIT Merger</title>
    <style>
      :root{--bg:#0f172a;--card:#1e293b;--line:#334155;--text:#e2e8f0;--muted:#94a3b8;--blue:#2563eb;--green:#16a34a;--red:#dc2626}
      *{box-sizing:border-box}
      body{background:linear-gradient(160deg,#0b1220 0%,#0f172a 100%);color:var(--text);font-family:Arial,sans-serif;margin:0;padding:20px}
      .card{max-width:980px;margin:16px auto;background:var(--card);border:1px solid var(--line);border-radius:14px;padding:20px}
      .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:14px}
      .btn{cursor:pointer;background:var(--blue);color:#fff;border:0;border-radius:8px;padding:10px 14px;margin-top:10px;margin-right:8px;font-weight:600}
      .btn-link{display:inline-block;text-decoration:none}
      .btn-secondary{background:var(--green)}
      .btn-danger{background:var(--red)}
      .btn:disabled{opacity:.65;cursor:not-allowed}
      .muted{color:var(--muted)}
      .success{color:#86efac}
      .danger{color:#fca5a5}
      .debug-panel{margin-top:12px;border:1px solid var(--line);border-radius:8px;background:#0f172a}
      .debug-panel summary{cursor:pointer;padding:10px 12px;font-weight:600}
      .debug-panel pre{margin:0 12px 12px 12px}
      input,select{width:100%;background:#0f172a;color:var(--text);border:1px solid var(--line);border-radius:6px;padding:8px}
      pre{white-space:pre-wrap;background:#0f172a;border:1px solid var(--line);padding:12px;border-radius:8px}
      .summary{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-top:12px}
      .metric{background:#0f172a;border:1px solid var(--line);padding:10px;border-radius:10px}
      .metric .label{display:block;color:var(--muted);font-size:12px;margin-bottom:4px}
      .metric .value{font-size:16px;font-weight:700}
      .status{display:none;margin:12px 0;padding:12px;border:1px solid var(--line);background:#0f172a;border-radius:10px}
      .status.active{display:block}
      .spinner{width:16px;height:16px;display:inline-block;border:2px solid #1d4ed8;border-top-color:#bfdbfe;border-radius:50%;animation:spin 1s linear infinite;margin-right:8px;vertical-align:-3px}
      .progress{margin-top:10px;height:6px;background:#111827;border-radius:8px;overflow:hidden}
      .progress > span{display:block;height:100%;width:35%;background:linear-gradient(90deg,#1d4ed8,#60a5fa);animation:slide 1.4s ease-in-out infinite}
      #actions{margin-top:12px}
      @keyframes spin{to{transform:rotate(360deg)}}
      @keyframes slide{0%{transform:translateX(-120%)}100%{transform:translateX(360%)}}
    </style>
    </head><body>
      <div class='card'>
        <h1>Apple HR + FIT Merger</h1>
        <p class='muted'>Ablauf: Datei wählen → Preview → Merge → Import. FIT-Timeline bleibt führend.</p>
        <p><a href='/imports/fit' style='color:#93c5fd'>← zurück zur Startseite</a></p>
        <div>
          <a href='/imports/fit' class='btn btn-link'>Neue Datei wählen</a>
        </div>
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
        <div id='previewStatus' class='status'>
          <div><span class='spinner'></span><strong id='previewStatusText'>FIT wird gelesen</strong></div>
          <div class='progress'><span></span></div>
          <button id='previewCancelBtn' type='button' class='btn btn-danger'>Preview abbrechen</button>
        </div>
        <hr style='border-color:#374151'/>
        <form id='runForm'>
          <input type='hidden' name='import_token' />
          <div class='grid'>
            <div><label>Overwrite existing FIT HR</label><br/><select name='overwrite_existing_hr'><option value='false'>No</option><option value='true'>Yes</option></select></div>
            <div><label>Ignore implausible HR</label><br/><select name='ignore_implausible_hr'><option value='true'>Yes</option><option value='false'>No</option></select></div>
            <div><label>HR min / max</label><br/><input type='number' name='min_hr' value='30' min='1' max='240'/> <input type='number' name='max_hr' value='240' min='60' max='260'/></div>
          </div>
          <button id='runBtn' class='btn' type='submit'>Run Merge</button>
        </form>
        <div id='runStatus' class='status'>
          <div><span class='spinner'></span><strong id='runStatusText'>Daten werden zusammengeführt</strong></div>
          <div class='progress'><span></span></div>
          <button id='runCancelBtn' type='button' class='btn btn-danger'>Merge abbrechen</button>
        </div>
        <div id='importStatus' class='status'>
          <div><span class='spinner'></span><strong id='importStatusText'>FIT wird gelesen</strong></div>
          <div class='progress'><span></span></div>
          <button id='importCancelBtn' type='button' class='btn btn-danger'>Import abbrechen</button>
        </div>
        <div id='summary' class='summary'></div>
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
        let previewController = null;
        let runController = null;
        let importController = null;

        const output = document.getElementById('output');
        const appleDebugOutput = document.getElementById('appleDebugOutput');
        const appleDebugPanel = document.getElementById('appleDebugPanel');
        const actions = document.getElementById('actions');
        const summary = document.getElementById('summary');
        const previewStatus = document.getElementById('previewStatus');
        const previewStatusText = document.getElementById('previewStatusText');
        const runStatus = document.getElementById('runStatus');
        const runStatusText = document.getElementById('runStatusText');
        const importStatus = document.getElementById('importStatus');
        const importStatusText = document.getElementById('importStatusText');
        const previewButton = document.querySelector('#previewForm button[type="submit"]');
        const runButton = document.getElementById('runBtn');
        const previewCancelBtn = document.getElementById('previewCancelBtn');
        const runCancelBtn = document.getElementById('runCancelBtn');
        const importCancelBtn = document.getElementById('importCancelBtn');

        const renderSafeText = (element, value) => {
          element.textContent = String(value ?? '');
        };
        const clearActions = () => { actions.replaceChildren(); };
        const clearSummary = () => { summary.replaceChildren(); };
        const renderMetric = (label, value) => {
          const metric = document.createElement('div');
          metric.className = 'metric';
          const labelEl = document.createElement('span');
          labelEl.className = 'label';
          renderSafeText(labelEl, label);
          const valueEl = document.createElement('span');
          valueEl.className = 'value';
          renderSafeText(valueEl, value ?? '—');
          metric.appendChild(labelEl);
          metric.appendChild(valueEl);
          return metric;
        };
        const renderMetrics = (items) => {
          clearSummary();
          for (const [label, value] of items) {
            summary.appendChild(renderMetric(label, value));
          }
        };
        const renderJsonOutput = (element, prefix, payload) => {
          const json = JSON.stringify(payload, null, 2);
          renderSafeText(element, prefix ? `${prefix}\\n\\n${json}` : json);
        };
        const setLoading = (section, active, message='') => {
          section.classList.toggle('active', active);
          if (message) {
            if (section === previewStatus) previewStatusText.textContent = message;
            if (section === runStatus) runStatusText.textContent = message;
            if (section === importStatus) importStatusText.textContent = message;
          }
        };

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
            if (!confirm(`Aktivität ${activityId} wirklich löschen?`)) return;
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

        const renderPreviewSummary = (data) => {
          const p = data.preview || {};
          renderMetrics([
            ['Start', p.start_time || '—'],
            ['Dauer (s)', p.duration_s],
            ['Distanz (m)', p.distance_m],
            ['Samples', p.sample_count],
            ['Apple HR Punkte', data.apple_points_total],
            ['Apple Quelle', data.apple_source_type || 'auto']
          ]);
        };

        const renderRunSummary = (data) => {
          const r = data.report || {};
          renderMetrics([
            ['HR-Punkte geschrieben', r.hr_points_written],
            ['Coverage', r.coverage_ratio],
            ['Calories', r.calories_applied],
            ['Temperatur', r.temperature_applied],
            ['Output', data.filename || 'merged.fit'],
            ['Trace', data.trace_id || '—']
          ]);
        };

        const renderImportSummary = (data) => {
          const p = data.preview || {};
          renderMetrics([
            ['Activity-ID', data.activity_id],
            ['Distanz (m)', p.distance_m],
            ['Dauer (s)', p.duration_s],
            ['Höhenmeter (m)', p.elevation_m ?? p.total_ascent_m],
            ['Samples', data.sample_count],
            ['Datei', data.filename]
          ]);
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
              importBtn.disabled = true;
              setLoading(importStatus, true, 'FIT wird gelesen');
              const res = await fetch('/api/tools/fit-hr-merge/import', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ artifact_token: artifactToken }),
                signal: (importController = new AbortController()).signal
              });
              importStatusText.textContent = 'Aktivität wird gespeichert';
              const data = await res.json();
              if (!res.ok) throw new Error(data.error || `Import failed with status ${res.status}`);
              importStatusText.textContent = 'Streams werden geschrieben';
              renderJsonOutput(output, 'Import erfolgreich.', data);
              importedActivityId = data.activity_id;
              renderImportSummary(data);
              renderDeleteButton(importedActivityId);
            } catch (error) {
              if (error.name === 'AbortError') {
                renderSafeText(output, 'Import abgebrochen. Du kannst den Import erneut starten.');
              } else {
                renderSafeText(output, `Import error: ${error.message}`);
              }
            } finally {
              importController = null;
              setLoading(importStatus, false);
              importBtn.disabled = false;
            }
          });

          actions.appendChild(importBtn);
        };

        document.getElementById('previewForm').addEventListener('submit', async (e) => {
          e.preventDefault();
          clearActions();
          clearSummary();
          artifactToken = null;
          importedActivityId = null;

          try {
            previewButton.disabled = true;
            setLoading(previewStatus, true, 'FIT wird gelesen');
            const fd = new FormData(e.target);
            const controller = new AbortController();
            previewController = controller;
            const res = await fetch('/api/tools/fit-hr-merge/preview', { method: 'POST', body: fd, signal: controller.signal });
            previewStatusText.textContent = 'Apple-Daten werden analysiert';
            const data = await res.json();
            if (!res.ok) throw new Error(data.error || `Preview failed with status ${res.status}`);
            previewStatusText.textContent = 'Vorschau wird erstellt';

            if (data.import_token) {
              importToken = data.import_token;
              document.querySelector('#runForm input[name="import_token"]').value = importToken;
            }

            renderAppleDebug(data);
            renderPreviewSummary(data);
            renderJsonOutput(output, '', data);
          } catch (error) {
            if (error.name === 'AbortError') {
              renderSafeText(output, 'Preview abgebrochen.');
            } else {
              renderSafeText(output, `Preview error: ${error.message}`);
            }
          } finally {
            previewController = null;
            setLoading(previewStatus, false);
            previewButton.disabled = false;
          }
        });

        document.getElementById('runForm').addEventListener('submit', async (e) => {
          e.preventDefault();
          clearActions();

          try {
            runButton.disabled = true;
            setLoading(runStatus, true, 'Daten werden zusammengeführt');
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

            const controller = new AbortController();
            runController = controller;
            const res = await fetch('/api/tools/fit-hr-merge/run', {
              method: 'POST',
              headers: {'Content-Type':'application/json'},
              body: JSON.stringify(payload),
              signal: controller.signal
            });
            runStatusText.textContent = 'Herzfrequenz wird interpoliert';
            const data = await res.json();
            if (!res.ok) throw new Error(data.error || `Run failed with status ${res.status}`);
            runStatusText.textContent = 'FIT wird geschrieben';

            renderAppleDebug(data);
            renderRunSummary(data);
            renderJsonOutput(output, '', data);
            renderActions(data);
          } catch (error) {
            if (error.name === 'AbortError') {
              renderSafeText(output, 'Merge abgebrochen.');
            } else {
              renderSafeText(output, `Run error: ${error.message}`);
            }
          } finally {
            runController = null;
            setLoading(runStatus, false);
            runButton.disabled = false;
          }
        });

        previewCancelBtn.addEventListener('click', () => {
          if (previewController) previewController.abort();
          setLoading(previewStatus, false);
          previewButton.disabled = false;
        });

        runCancelBtn.addEventListener('click', () => {
          if (runController) runController.abort();
          setLoading(runStatus, false);
          runButton.disabled = false;
        });

        importCancelBtn.addEventListener('click', () => {
          if (importController) importController.abort();
          setLoading(importStatus, false);
        });
      </script>
    </body></html>
    """


def _render_preview_page(token: str, preview: dict) -> str:
    return f"""
    <html><body style='margin:0;background:#0f172a;color:#e2e8f0;font-family:Arial,sans-serif'>
    <div style='max-width:840px;margin:24px auto;padding:0 16px;'>
      <div style='background:#1e293b;border:1px solid #334155;border-radius:14px;padding:20px'>
        <h2 style='margin-top:0'>FIT Preview: {preview['source_file_name']}</h2>
        <p style='color:#94a3b8'>Schritt 2/3: Vorschau prüfen und Import bestätigen.</p>
        <div style='margin-bottom:12px;display:flex;gap:10px;flex-wrap:wrap'>
          <a href='/imports/fit' style='display:inline-block;background:#2563eb;color:#fff;text-decoration:none;border-radius:8px;padding:10px 14px;font-weight:600'>Neue Datei wählen</a>
          <a href='/imports/fit' style='display:inline-block;background:#1f2937;color:#e2e8f0;text-decoration:none;border:1px solid #334155;border-radius:8px;padding:10px 14px;font-weight:600'>Zur Startseite</a>
        </div>
        <div style='display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px'>
          <div style='background:#0f172a;border:1px solid #334155;border-radius:10px;padding:10px'><small style='color:#94a3b8'>Start</small><div>{preview['start_time']}</div></div>
          <div style='background:#0f172a;border:1px solid #334155;border-radius:10px;padding:10px'><small style='color:#94a3b8'>Ende</small><div>{preview['end_time']}</div></div>
          <div style='background:#0f172a;border:1px solid #334155;border-radius:10px;padding:10px'><small style='color:#94a3b8'>Dauer (s)</small><div>{preview['duration_s']}</div></div>
          <div style='background:#0f172a;border:1px solid #334155;border-radius:10px;padding:10px'><small style='color:#94a3b8'>Distanz (m)</small><div>{preview['distance_m']}</div></div>
          <div style='background:#0f172a;border:1px solid #334155;border-radius:10px;padding:10px'><small style='color:#94a3b8'>Höhenmeter (m)</small><div>{preview.get('elevation_m', preview.get('total_ascent_m', '—'))}</div></div>
          <div style='background:#0f172a;border:1px solid #334155;border-radius:10px;padding:10px'><small style='color:#94a3b8'>Samples</small><div>{preview['sample_count']}</div></div>
        </div>
        <details style='margin-top:12px;border:1px solid #334155;border-radius:10px;padding:8px 12px;background:#0f172a'>
          <summary style='cursor:pointer'>Optional: Debug JSON</summary>
          <pre style='white-space:pre-wrap'>{json.dumps(preview, indent=2)}</pre>
        </details>
        <form action='/imports/fit/confirm' method='post' style='margin-top:14px'>
          <input type='hidden' name='import_token' value='{token}' />
          <button id='confirmBtn' style='background:#2563eb;color:#fff;border:0;border-radius:8px;padding:10px 16px;font-weight:600' type='submit'>Jetzt importieren</button>
          <button id='cancelBtn' style='display:none;background:#dc2626;color:#fff;border:0;border-radius:8px;padding:10px 16px;font-weight:600;margin-left:8px' type='button'>Import abbrechen</button>
          <span id='loading' style='display:none;margin-left:8px;color:#93c5fd'>Import läuft…</span>
        </form>
        <pre id='importOutput' style='white-space:pre-wrap;background:#0f172a;border:1px solid #334155;padding:12px;border-radius:8px;margin-top:12px;'>Noch kein Import gestartet.</pre>
      </div>
    </div>
    <script>
      const form = document.querySelector('form[action="/imports/fit/confirm"]');
      const confirmBtn = document.getElementById('confirmBtn');
      const cancelBtn = document.getElementById('cancelBtn');
      const loading = document.getElementById('loading');
      const importOutput = document.getElementById('importOutput');
      let importController = null;
      const renderSafeText = (element, value) => {{
        element.textContent = String(value ?? '');
      }};

      form.addEventListener('submit', async (event) => {{
        event.preventDefault();
        confirmBtn.disabled = true;
        cancelBtn.style.display = 'inline-block';
        loading.style.display = 'inline';
        importController = new AbortController();
        try {{
          const payload = {{ import_token: '{token}' }};
          const res = await fetch('/api/imports/fit/confirm', {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify(payload),
            signal: importController.signal
          }});
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || `Import failed with status ${{res.status}}`);
          renderSafeText(importOutput, `Import erfolgreich. Activity ${{data.activity_id}} (${{data.sample_count}} Samples).`);
          window.location.href = '/imports/fit';
        }} catch (error) {{
          if (error.name === 'AbortError') {{
            renderSafeText(importOutput, 'Import abgebrochen.');
          }} else {{
            renderSafeText(importOutput, `Import error: ${{error.message}}`);
          }}
        }} finally {{
          importController = null;
          confirmBtn.disabled = false;
          cancelBtn.style.display = 'none';
          loading.style.display = 'none';
        }}
      }});

      cancelBtn.addEventListener('click', () => {{
        if (importController) importController.abort();
      }});
    </script>
    </body></html>
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
                <body style='margin:0;background:#0f172a;color:#e2e8f0;font-family:Arial,sans-serif;padding:24px;'>
                  <div style='max-width:760px;margin:0 auto;background:#1e293b;border:1px solid #334155;border-radius:14px;padding:20px;'>
                    <h2 style='color:#86efac;margin-top:0'>Import complete</h2>
                    <p>Activity <strong>{activity_id}</strong> gespeichert ({sample_count} Samples).</p>
                    <div style='display:flex;gap:10px;flex-wrap:wrap;margin-top:12px'>
                      <a href='/imports/fit' style='display:inline-block;background:#2563eb;color:#fff;text-decoration:none;border-radius:8px;padding:10px 14px;font-weight:600'>Weiterer Import</a>
                      <a href='/tools/fit-hr-merge' style='display:inline-block;background:#1f2937;color:#e2e8f0;text-decoration:none;border:1px solid #334155;border-radius:8px;padding:10px 14px;font-weight:600'>Zurück zum Merge</a>
                      <a href='/imports/fit' style='display:inline-block;background:#1f2937;color:#e2e8f0;text-decoration:none;border:1px solid #334155;border-radius:8px;padding:10px 14px;font-weight:600'>Zur Startseite</a>
                      <button id='deleteBtn' style='background:#dc2626;color:#fff;border:0;border-radius:8px;padding:10px 16px;font-weight:600' type='button'>Diese Aktivität löschen</button>
                    </div>
                    <pre id="deleteResult" style="white-space:pre-wrap;background:#0f172a;border:1px solid #334155;padding:12px;border-radius:8px;margin-top:12px;">Noch kein Löschvorgang gestartet.</pre>
                  </div>
                  <script>
                    document.getElementById('deleteBtn').addEventListener('click', async () => {{
                      if (!confirm('Aktivität {activity_id} wirklich löschen?')) return;
                      try {{
                        const r = await fetch('/api/activities/delete', {{
                          method: 'POST',
                          headers: {{'Content-Type':'application/json'}},
                          body: JSON.stringify({{activity_id:{activity_id}}})
                        }});
                        const d = await r.json();
                        if (!r.ok) throw new Error(d.error || 'Delete failed');
                        document.getElementById('deleteResult').textContent = 'Erfolgreich gelöscht: Activity {activity_id}\\n\\n' + JSON.stringify(d, null, 2);
                      }} catch (e) {{
                        document.getElementById('deleteResult').textContent = 'Delete error: ' + e.message;
                      }}
                    }});
                  </script>
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
