"""Tests for Apple Health HR normalization helpers."""

from pathlib import Path
import sys
from unittest.mock import MagicMock

sys.modules.setdefault("psycopg2", MagicMock())
sys.modules.setdefault("psycopg2.extras", MagicMock())

_ingestor_dir = Path(__file__).resolve().parent.parent / "ingestor"
if str(_ingestor_dir) not in sys.path:
    sys.path.insert(0, str(_ingestor_dir))

import apple_hr


def test_parse_auto_health_export_json_wrapper():
    payload = '{"heartRateData":[{"date":"2026-04-11 09:01:06 +0200","Avg":126,"units":"bpm"}]}'
    rows = apple_hr.parse_apple_hr_json(payload)
    assert rows == [{"timestamp": "2026-04-11T07:01:06Z", "hr": 126}]


def test_parse_top_level_single_object_json():
    payload = '{"timestamp":"2026-04-10T12:01:05Z","hr":142}'
    rows = apple_hr.parse_apple_hr_json(payload)
    assert rows == [{"timestamp": "2026-04-10T12:01:05Z", "hr": 142}]


def test_parse_auto_health_export_nested_workouts_json():
    payload = '{"data":{"workouts":[{"heartRateData":[{"date":"2026-04-11 09:01:06 +0200","Avg":126,"Min":120,"Max":130,"units":"bpm"}]}]}}'
    rows = apple_hr.parse_apple_hr_json(payload)
    assert rows == [{"timestamp": "2026-04-11T07:01:06Z", "hr": 126}]


def test_parse_csv_and_normalize_bounds_and_duplicates():
    payload = "timestamp,hr\n2026-04-10T12:00:00Z,150\n2026-04-10T12:00:00Z,151\n2026-04-10T12:00:01Z,20\n"
    rows = apple_hr.parse_apple_hr_csv(payload)
    normalized = apple_hr.normalize_hr_series(rows, min_hr=30, max_hr=240)
    assert normalized == [{"timestamp": "2026-04-10T12:00:00Z", "hr": 151}]
