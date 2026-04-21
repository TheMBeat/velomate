"""FIT import use-cases (no web/CLI framework coupling)."""

from __future__ import annotations

from db import delete_activity, get_connection
from fit_import import FitImportError, import_fit_payload, parse_fit_bytes
from fitness import recalculate_fitness


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


def delete_imported_activity(activity_id: int) -> tuple[int, str]:
    conn = get_connection()
    try:
        deleted = delete_activity(conn, activity_id)
        if deleted is None:
            raise KeyError("Activity not found")
        recalculate_fitness(conn)
        return deleted
    finally:
        conn.close()
