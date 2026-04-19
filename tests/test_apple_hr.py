"""Tests for Apple Health HR parser behavior."""

from pathlib import Path
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock

sys.modules.setdefault("psycopg2", MagicMock())
sys.modules.setdefault("psycopg2.extras", MagicMock())

_ingestor_dir = Path(__file__).resolve().parent.parent / "ingestor"
if str(_ingestor_dir) not in sys.path:
    sys.path.insert(0, str(_ingestor_dir))

import apple_hr


def test_parse_data_workouts_avg_field_and_utc_conversion():
    payload = """{
      "data": {
        "workouts": [
          {
            "start": "2026-04-11 09:00:16 +0200",
            "end": "2026-04-11 10:04:51 +0200",
            "heartRateData": [{"date": "2026-04-11 09:01:06 +0200", "Avg": 126, "Min": 120, "Max": 130}]
          }
        ]
      }
    }"""
    parsed = apple_hr.parse_apple_hr_json_with_debug(payload)
    assert parsed["samples"] == [{"timestamp": "2026-04-11T07:01:06Z", "hr": 126}]
    assert parsed["source_type"] == "json"


def test_overlap_based_workout_selection_has_priority():
    payload = """{
      "data": {
        "workouts": [
          {
            "id": "w1",
            "start": "2026-04-11 05:00:00 +0000",
            "end": "2026-04-11 06:00:00 +0000",
            "heartRateData": [{"date":"2026-04-11 05:10:00 +0000","Avg":111}]
          },
          {
            "id": "w2",
            "start": "2026-04-11 07:00:00 +0000",
            "end": "2026-04-11 08:00:00 +0000",
            "heartRateData": [{"date":"2026-04-11 07:10:00 +0000","Avg":140}]
          }
        ]
      }
    }"""
    parsed = apple_hr.parse_apple_hr_json_with_debug(
        payload,
        fit_start=datetime(2026, 4, 11, 7, 5, tzinfo=timezone.utc),
        fit_end=datetime(2026, 4, 11, 7, 55, tzinfo=timezone.utc),
    )
    assert parsed["samples"] == [{"timestamp": "2026-04-11T07:10:00Z", "hr": 140}]
    assert parsed["debug"]["selected_workout_id"] == "w2"


def test_fallback_selection_uses_most_parseable_points_when_no_overlap():
    payload = """{
      "data": {
        "workouts": [
          {
            "id": "w1",
            "start": "2026-04-11 01:00:00 +0000",
            "end": "2026-04-11 02:00:00 +0000",
            "heartRateData": [{"date":"bad-date","Avg":111}]
          },
          {
            "id": "w2",
            "start": "2026-04-11 03:00:00 +0000",
            "end": "2026-04-11 04:00:00 +0000",
            "heartRateData": [
              {"date":"2026-04-11 03:10:00 +0000","Avg":135},
              {"date":"2026-04-11 03:20:00 +0000","Avg":136}
            ]
          }
        ]
      }
    }"""
    parsed = apple_hr.parse_apple_hr_json_with_debug(
        payload,
        fit_start=datetime(2026, 4, 11, 7, 0, tzinfo=timezone.utc),
        fit_end=datetime(2026, 4, 11, 8, 0, tzinfo=timezone.utc),
    )
    assert len(parsed["samples"]) == 2
    assert parsed["debug"]["fallback_workout_index"] == 1
    assert parsed["debug"]["selected_workout_id"] == "w2"


def test_overlap_winner_with_zero_parseable_points_falls_back_to_parseable_workout():
    payload = """{
      "data": {
        "workouts": [
          {
            "id": "w_overlap_bad",
            "start": "2026-04-11 07:00:00 +0000",
            "end": "2026-04-11 08:00:00 +0000",
            "heartRateData": [{"date":"bad-date","Avg":150}]
          },
          {
            "id": "w_parseable",
            "start": "2026-04-11 05:00:00 +0000",
            "end": "2026-04-11 06:00:00 +0000",
            "heartRateData": [{"date":"2026-04-11 05:10:00 +0000","Avg":132}]
          }
        ]
      }
    }"""
    parsed = apple_hr.parse_apple_hr_json_with_debug(
        payload,
        fit_start=datetime(2026, 4, 11, 7, 10, tzinfo=timezone.utc),
        fit_end=datetime(2026, 4, 11, 7, 40, tzinfo=timezone.utc),
    )
    assert parsed["samples"] == [{"timestamp": "2026-04-11T05:10:00Z", "hr": 132}]
    assert parsed["debug"]["selected_workout_id"] == "w_overlap_bad"
    assert parsed["debug"]["selected_workout_parseable_point_count"] == 0
    assert parsed["debug"]["fallback_workout_id"] == "w_parseable"


def test_no_parseable_workouts_returns_empty_samples():
    payload = """{
      "data": {
        "workouts": [
          {
            "id": "w1",
            "start": "2026-04-11 07:00:00 +0000",
            "end": "2026-04-11 08:00:00 +0000",
            "heartRateData": [{"date":"bad-date","Avg":126}]
          },
          {
            "id": "w2",
            "start": "2026-04-11 07:00:00 +0000",
            "end": "2026-04-11 08:00:00 +0000",
            "heartRateData": [{"date":"still-bad","Avg":"bad"}]
          }
        ]
      }
    }"""
    parsed = apple_hr.parse_apple_hr_json_with_debug(
        payload,
        fit_start=datetime(2026, 4, 11, 7, 10, tzinfo=timezone.utc),
        fit_end=datetime(2026, 4, 11, 7, 40, tzinfo=timezone.utc),
    )
    assert parsed["samples"] == []


def test_debug_rejection_reasons_are_reported():
    payload = """{
      "heartRateData": [
        {"date":"bad-date","Avg":126},
        {"date":"2026-04-11 09:01:06 +0200","Avg":"bad"},
        {"date":"2026-04-11 09:01:07 +0200","Avg":127}
      ]
    }"""
    parsed = apple_hr.parse_apple_hr_json_with_debug(payload)
    assert parsed["samples"] == [{"timestamp": "2026-04-11T07:01:07Z", "hr": 127}]
    assert parsed["debug"]["raw_heart_rate_entries_found"] == 3
    assert parsed["debug"]["parsed_heart_rate_entries_count"] == 1
    assert parsed["debug"]["rejected_entries_count"] == 2
    assert parsed["debug"]["rejection_reasons"]["Invalid timestamp format: bad-date"] == 1
    assert parsed["debug"]["rejection_reasons"]["Invalid HR value: bad"] == 1


def test_parse_json_accepts_bom_multiple_wrappers_and_field_aliases():
    payload = (
        "\ufeff"
        '{"workouts":[{"id":"wk-1","start":"2026-04-11 07:00:00 +0000","end":"2026-04-11 08:00:00 +0000",'
        '"heartRateData":[{"startDate":"2026-04-11T07:00:10Z","bpm":120},{"timestamp":"2026-04-11T07:00:20Z","heartRate":121}]}]}'
    )
    parsed = apple_hr.parse_apple_hr_json_with_debug(
        payload.lstrip("\ufeff"),
        fit_start=datetime(2026, 4, 11, 7, 0, tzinfo=timezone.utc),
        fit_end=datetime(2026, 4, 11, 8, 0, tzinfo=timezone.utc),
    )

    assert parsed["samples"] == [
        {"timestamp": "2026-04-11T07:00:10Z", "hr": 120},
        {"timestamp": "2026-04-11T07:00:20Z", "hr": 121},
    ]
    assert parsed["debug"]["workouts_found"] == 1


def test_auto_mode_prefers_json_then_falls_back_to_csv():
    json_payload = '{"samples":[{"timestamp":"2026-04-11T07:00:20Z","hr":121}]}'
    csv_payload = "timestamp,hr\n2026-04-11T07:01:06Z,126\n"

    parsed_json = apple_hr.parse_apple_hr_text_details(json_payload, source_type="auto")
    parsed_csv = apple_hr.parse_apple_hr_text_details(csv_payload, source_type="auto")

    assert parsed_json["source_type"] == "json"
    assert parsed_json["samples"][0]["hr"] == 121
    assert parsed_csv["source_type"] == "csv"
    assert parsed_csv["debug"]["parser_mode"] == "auto_csv_fallback"
