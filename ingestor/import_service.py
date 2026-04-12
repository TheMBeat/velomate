"""FIT import use-cases (no web/CLI framework coupling)."""

from __future__ import annotations

from db import get_connection
from fit_import import FitImportError, import_fit_payload, parse_fit_bytes


def preview_fit_import(filename: str, content: bytes) -> dict:
    if not filename:
        raise FitImportError("Missing file")
    if not filename.lower().endswith(".fit"):
        raise FitImportError("Only .fit files are supported")
    return parse_fit_bytes(content, filename)


def persist_fit_import(parsed: dict) -> tuple[int, int]:
    conn = get_connection()
    try:
        return import_fit_payload(conn, parsed, run_fitness_recalc=True)
    finally:
        conn.close()
