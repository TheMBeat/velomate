"""Tests for FIT importer model mapping and optional HR behavior."""

import sys
from pathlib import Path

# Add ingestor/ to path (no __init__.py)
_ingestor_dir = Path(__file__).resolve().parent.parent / "ingestor"
if str(_ingestor_dir) not in sys.path:
    sys.path.insert(0, str(_ingestor_dir))

import fit_importer  # noqa: E402


class _Field:
    def __init__(self, name, value):
        self.name = name
        self.value = value


class _Msg:
    def __init__(self, name, values):
        self.name = name
        self.fields = [_Field(k, v) for k, v in values.items()]


class _FitObj:
    def __init__(self, messages):
        self.messages = messages


class _FitCls:
    @staticmethod
    def from_bytes(_raw):
        import datetime as dt

        t0 = dt.datetime(2026, 4, 11, 7, 1, 6, tzinfo=dt.timezone.utc)
        t1 = dt.datetime(2026, 4, 11, 7, 1, 8, tzinfo=dt.timezone.utc)
        return _FitObj([
            _Msg("record", {"timestamp": t0, "heart_rate": 126, "power": 220, "distance": 10.0}),
            _Msg("record", {"timestamp": t1, "heart_rate": None, "power": 230, "distance": 20.0}),
        ])


def test_parse_fit_to_activity_keeps_hr_optional(monkeypatch):
    monkeypatch.setattr(fit_importer, "_load_fit_tool", lambda: _FitCls)
    activity = fit_importer.parse_fit_to_activity(b"fit", file_name="ride.fit")

    assert activity.name == "ride"
    assert activity.samples[0].hr == 126
    assert activity.samples[1].hr is None
    assert activity.avg_hr == 126
    assert activity.max_hr == 126


def test_to_db_streams_shape(monkeypatch):
    monkeypatch.setattr(fit_importer, "_load_fit_tool", lambda: _FitCls)
    activity = fit_importer.parse_fit_to_activity(b"fit", file_name="ride.fit")
    streams = fit_importer.to_db_streams(activity.samples)
    assert streams[0]["time_offset"] == 0
    assert streams[1]["time_offset"] == 2
    assert "hr" in streams[1]
