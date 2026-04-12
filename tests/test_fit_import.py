"""Tests for FIT parsing and FIT upload flow."""

import io
from datetime import datetime, timezone
from pathlib import Path
import sys
from unittest.mock import MagicMock, patch

import pytest

# Mock DB deps before importing ingestor modules
sys.modules.setdefault("psycopg2", MagicMock())
sys.modules.setdefault("psycopg2.extras", MagicMock())
sys.modules.setdefault("fitparse", MagicMock())

_ingestor_dir = Path(__file__).resolve().parent.parent / "ingestor"
if str(_ingestor_dir) not in sys.path:
    sys.path.insert(0, str(_ingestor_dir))

import fit_import
import webapp


class _Field:
    def __init__(self, name, value):
        self.name = name
        self.value = value


class _FakeFit:
    def __init__(self, records):
        self._records = records

    def parse(self):
        return None

    def get_messages(self, name):
        assert name == "record"
        return self._records


def _record(ts, hr=None, power=None, cadence=None, speed=8.0, distance=1000.0, lat=None, lng=None):
    return [
        _Field("timestamp", ts),
        _Field("distance", distance),
        _Field("speed", speed),
        _Field("heart_rate", hr),
        _Field("power", power),
        _Field("cadence", cadence),
        _Field("position_lat", lat),
        _Field("position_long", lng),
        _Field("altitude", 100.0),
    ]


def test_parse_valid_fit_with_hr():
    start = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    records = [_record(start, hr=140, power=220, cadence=85, distance=0.0), _record(start.replace(second=10), hr=145, power=230, cadence=88, distance=120.0)]
    with patch("fit_import.FitFile", return_value=_FakeFit(records)):
        parsed = fit_import.parse_fit_bytes(b"valid-fit", "ride.fit")

    assert parsed["preview"]["has_heart_rate"] is True
    assert parsed["preview"]["has_power"] is True
    assert parsed["activity"]["source_system"] == "fit_upload"


def test_parse_valid_fit_without_hr():
    start = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    records = [_record(start, power=220, cadence=85, distance=0.0), _record(start.replace(second=20), power=225, cadence=87, distance=240.0)]
    with patch("fit_import.FitFile", return_value=_FakeFit(records)):
        parsed = fit_import.parse_fit_bytes(b"valid-fit-no-hr", "ride_no_hr.fit")

    assert parsed["preview"]["has_heart_rate"] is False
    assert parsed["activity"]["avg_hr"] is None


def test_parse_invalid_fit_raises():
    broken = MagicMock()
    broken.parse.side_effect = RuntimeError("bad")
    with patch("fit_import.FitFile", return_value=broken):
        with pytest.raises(fit_import.FitImportError):
            fit_import.parse_fit_bytes(b"broken", "bad.fit")


def test_upload_preview_success():
    parsed = {"preview": {"source_file_name": "upload.fit"}, "activity": {"source_system": "fit_upload"}, "streams": [{"time_offset": 0}]}
    with patch("webapp.parse_fit_bytes", return_value=parsed):
        resp = webapp._handle_fit_preview("upload.fit", b"ok")
    assert "import_token" in resp
    assert resp["preview"]["source_file_name"] == "upload.fit"


def test_upload_invalid_fit_handling():
    with patch("webapp.parse_fit_bytes", side_effect=fit_import.FitImportError("Could not parse FIT file")):
        with pytest.raises(fit_import.FitImportError):
            webapp._handle_fit_preview("broken.fit", b"broken")


def test_persistence_and_source_tagging():
    token = webapp._store_pending({"preview": {}, "activity": {"name": "upload.fit", "source_system": "fit_upload", "strava_id": None}, "streams": [{"time_offset": 0}, {"time_offset": 1}]})

    mock_conn = MagicMock()
    with (
        patch("webapp.get_connection", return_value=mock_conn),
        patch("webapp.import_fit_payload", return_value=(123, 2)) as import_payload,
    ):
        activity_id, sample_count = webapp._save_import(token)

    assert activity_id == 123
    assert sample_count == 2
    assert import_payload.call_args.args[1]["activity"]["source_system"] == "fit_upload"


def test_save_import_token_consumed_once():
    token = webapp._store_pending({"preview": {}, "activity": {"name": "upload.fit", "source_system": "fit_upload", "strava_id": None}, "streams": []})

    mock_conn = MagicMock()
    with (
        patch("webapp.get_connection", return_value=mock_conn),
        patch("webapp.import_fit_payload", return_value=(10, 0)),
    ):
        webapp._save_import(token)

    with pytest.raises(KeyError):
        webapp._save_import(token)


def test_save_import_rejects_expired_token():
    expired = datetime.now(timezone.utc) - webapp._PENDING_TTL - webapp.timedelta(seconds=1)
    token = "expired-token"
    with webapp._PENDING_IMPORTS_LOCK:
        webapp._PENDING_IMPORTS[token] = {"created_at": expired, "parsed": {"preview": {}, "activity": {"source_system": "fit_upload"}, "streams": []}}

    with pytest.raises(KeyError):
        webapp._save_import(token)

    with webapp._PENDING_IMPORTS_LOCK:
        assert token not in webapp._PENDING_IMPORTS


def test_api_confirm_invalid_json_returns_400():
    handler = webapp._Handler.__new__(webapp._Handler)
    handler.path = "/api/imports/fit/confirm"
    bad_payload = b"{"
    handler.headers = {"Content-Length": str(len(bad_payload))}
    handler.rfile = io.BytesIO(bad_payload)
    handler.wfile = io.BytesIO()
    handler.command = "POST"
    handler.request_version = "HTTP/1.1"

    with patch.object(handler, "_json") as send_json:
        handler.do_POST()

    send_json.assert_called_once_with(400, {"error": "Invalid JSON body"})


def test_api_merge_run_rejects_non_object_json_body():
    handler = webapp._Handler.__new__(webapp._Handler)
    handler.path = "/api/tools/fit-hr-merge/run"
    payload = b"[]"
    handler.headers = {"Content-Length": str(len(payload))}
    handler.rfile = io.BytesIO(payload)
    handler.wfile = io.BytesIO()
    handler.command = "POST"
    handler.request_version = "HTTP/1.1"

    with patch.object(handler, "_json") as send_json:
        handler.do_POST()

    send_json.assert_called_once_with(400, {"error": "JSON body must be an object"})


def test_api_merge_run_parses_string_boolean_flags():
    handler = webapp._Handler.__new__(webapp._Handler)
    handler.path = "/api/tools/fit-hr-merge/run"
    payload = b'{"import_token":"t","overwrite_existing_hr":"false","ignore_implausible_hr":"false"}'
    handler.headers = {"Content-Length": str(len(payload))}
    handler.rfile = io.BytesIO(payload)
    handler.wfile = io.BytesIO()
    handler.command = "POST"
    handler.request_version = "HTTP/1.1"

    with (
        patch.object(handler, "_json") as send_json,
        patch("webapp._run_hr_merge", return_value={"ok": True}) as run_merge,
    ):
        handler.do_POST()

    options = run_merge.call_args.args[1]
    assert options.overwrite_existing_hr is False
    assert options.ignore_implausible_hr is False
    send_json.assert_called_once_with(200, {"ok": True})
