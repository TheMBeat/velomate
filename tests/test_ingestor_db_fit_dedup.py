"""Regression tests for FIT upload dedup persistence behavior in ingestor/db.py."""

from datetime import datetime, timezone
from pathlib import Path
import sys
from unittest.mock import MagicMock

# Mock psycopg2 before importing ingestor db module
sys.modules.setdefault("psycopg2", MagicMock())
sys.modules.setdefault("psycopg2.extras", MagicMock())

_ingestor_dir = Path(__file__).resolve().parent.parent / "ingestor"
if str(_ingestor_dir) not in sys.path:
    sys.path.insert(0, str(_ingestor_dir))

import db as ingestor_db


def _mock_conn_with_cursor(fetchone_return=(1,)):
    conn = MagicMock()
    cursor_ctx = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = fetchone_return
    cursor_ctx.__enter__.return_value = cur
    cursor_ctx.__exit__.return_value = False
    conn.cursor.return_value = cursor_ctx
    return conn, cur


def test_create_schema_adds_source_unique_index():
    conn, cur = _mock_conn_with_cursor()
    ingestor_db.create_schema(conn)
    executed_sql = cur.execute.call_args.args[0]
    assert "idx_activities_source_unique" in executed_sql
    assert "ON activities(source_system, source_external_id)" in executed_sql


def test_insert_uses_source_conflict_target_for_fit_dedup():
    conn, cur = _mock_conn_with_cursor(fetchone_return=(42,))
    data = {
        "strava_id": None,
        "name": "upload.fit",
        "date": "2026-01-01T12:00:00+00:00",
        "distance_m": 1000.0,
        "duration_s": 300,
        "elevation_m": 0.0,
        "avg_hr": None,
        "max_hr": None,
        "avg_power": None,
        "max_power": None,
        "avg_cadence": None,
        "avg_speed_kmh": 12.0,
        "calories": None,
        "suffer_score": None,
        "device": "fit_upload",
        "source_system": "fit_upload",
        "source_external_id": "sha256",
        "source_file_name": "upload.fit",
        "is_indoor": False,
        "sport_type": "cycling_outdoor",
    }

    activity_id = ingestor_db._do_insert(conn, data, datetime.now(timezone.utc))
    executed_sql = cur.execute.call_args.args[0]

    assert activity_id == 42
    assert "ON CONFLICT (source_system, source_external_id)" in executed_sql
    assert "source_system" in executed_sql
    assert "source_external_id" in executed_sql


def test_insert_uses_strava_conflict_target_when_strava_id_present():
    conn, cur = _mock_conn_with_cursor(fetchone_return=(7,))
    data = {
        "strava_id": 999,
        "name": "strava ride",
        "date": "2026-01-01T12:00:00+00:00",
        "distance_m": 1000.0,
        "duration_s": 300,
        "elevation_m": 0.0,
        "avg_hr": None,
        "max_hr": None,
        "avg_power": None,
        "max_power": None,
        "avg_cadence": None,
        "avg_speed_kmh": 12.0,
        "calories": None,
        "suffer_score": None,
        "device": "unknown",
        "source_system": None,
        "source_external_id": None,
        "source_file_name": None,
        "is_indoor": False,
        "sport_type": "cycling_outdoor",
    }

    ingestor_db._do_insert(conn, data, datetime.now(timezone.utc))
    executed_sql = cur.execute.call_args.args[0]
    assert "ON CONFLICT (strava_id)" in executed_sql
