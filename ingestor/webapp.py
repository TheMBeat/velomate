"""Minimal built-in HTTP server for FIT upload UI + REST endpoints."""

from __future__ import annotations

import cgi
import json
import uuid
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

from db import get_connection, upsert_activity, upsert_streams
from fit_import import FitImportError, parse_fit_bytes
from fitness import recalculate_fitness

_PENDING_IMPORTS: dict[str, dict] = {}
_PENDING_TTL = timedelta(minutes=30)


def _purge_pending() -> None:
    now = datetime.now(timezone.utc)
    expired = [token for token, payload in _PENDING_IMPORTS.items() if now - payload["created_at"] > _PENDING_TTL]
    for token in expired:
        _PENDING_IMPORTS.pop(token, None)


def _store_pending(parsed: dict) -> str:
    _purge_pending()
    token = str(uuid.uuid4())
    _PENDING_IMPORTS[token] = {"created_at": datetime.now(timezone.utc), "parsed": parsed}
    return token


def _save_import(token: str) -> tuple[int, int]:
    pending = _PENDING_IMPORTS.get(token)
    if not pending:
        raise KeyError("Unknown or expired import token")
    parsed = pending["parsed"]
    conn = get_connection()
    try:
        activity_id, streams_preserved = upsert_activity(conn, parsed["activity"])
        if not streams_preserved:
            upsert_streams(conn, activity_id, parsed["streams"])
        recalculate_fitness(conn)
    finally:
        conn.close()
    _PENDING_IMPORTS.pop(token, None)
    return activity_id, len(parsed["streams"])


def _handle_fit_preview(filename: str, content: bytes) -> dict:
    if not filename:
        raise FitImportError("Missing file")
    if not filename.lower().endswith(".fit"):
        raise FitImportError("Only .fit files are supported")
    parsed = parse_fit_bytes(content, filename)
    token = _store_pending(parsed)
    return {"import_token": token, "preview": parsed["preview"]}


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
    </div></body></html>
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
        if self.path == "/" or self.path == "/imports/fit":
            self._send(200, _render_upload_page().encode("utf-8"))
            return
        if self.path == "/health":
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

        if self.path in ("/api/imports/fit/confirm", "/imports/fit/confirm"):
            if self.path.startswith("/api/"):
                raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                payload = json.loads(raw or b"{}")
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


def run_server(host: str, port: int) -> None:
    server = ThreadingHTTPServer((host, port), _Handler)
    server.serve_forever()
