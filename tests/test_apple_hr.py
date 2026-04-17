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


def test_parse_auto_health_export_selected_workout_only():
    payload = """{
      "data": {
        "selectedWorkoutId": "w2",
        "workouts": [
          {"id": "w1", "heartRateData": [{"date":"2026-04-11 09:00:00 +0200","Avg":111,"units":"bpm"}]},
          {"id": "w2", "heartRateData": [
            {"date":"2026-04-11 09:01:06 +0200","Avg":126,"units":"bpm"},
            {"date":"2026-04-11 09:01:07 +0200","Avg":127,"units":"bpm"}
          ]}
        ]
      }
    }"""
    parsed = apple_hr.parse_apple_hr_json_with_debug(payload)
    assert parsed["samples"] == [
        {"timestamp": "2026-04-11T07:01:06Z", "hr": 126},
        {"timestamp": "2026-04-11T07:01:07Z", "hr": 127},
    ]
    assert parsed["debug"]["workouts_found"] == 2
    assert parsed["debug"]["selected_workout_has_heart_rate_data"] is True
    assert parsed["debug"]["extracted_hr_points"] == 2


def test_parse_auto_health_export_auto_select_skips_empty_heart_rate_data():
    payload = """{
      "data": {
        "workouts": [
          {"id": "w1", "heartRateData": []},
          {"id": "w2", "heartRateData": [{"date":"2026-04-11 09:01:06 +0200","Avg":126,"units":"bpm"}]}
        ]
      }
    }"""
    parsed = apple_hr.parse_apple_hr_json_with_debug(payload)
    assert parsed["samples"] == [{"timestamp": "2026-04-11T07:01:06Z", "hr": 126}]
    assert parsed["debug"]["selected_workout_index"] == 1
    assert parsed["debug"]["selected_workout_has_heart_rate_data"] is True


def test_parse_auto_health_export_falls_back_when_selected_workout_is_empty():
    payload = """{
      "data": {
        "selectedWorkoutId": "w1",
        "workouts": [
          {"id": "w1", "heartRateData": []},
          {"id": "w2", "heartRateData": [{"date":"2026-04-11 09:01:06 +0200","Avg":126,"units":"bpm"}]}
        ]
      }
    }"""
    parsed = apple_hr.parse_apple_hr_json_with_debug(payload)
    assert parsed["samples"] == [{"timestamp": "2026-04-11T07:01:06Z", "hr": 126}]
    assert parsed["debug"]["selected_workout_index"] == 0
    assert parsed["debug"]["selected_workout_has_heart_rate_data"] is False
    assert parsed["debug"]["fallback_workout_index"] == 1


def test_parse_auto_health_export_unhashable_selector_falls_back():
    payload = """{
      "data": {
        "selectedWorkoutId": {"id": "w1"},
        "workouts": [
          {"id": "w1", "heartRateData": [{"date":"2026-04-11 09:01:06 +0200","Avg":126,"units":"bpm"}]}
        ]
      }
    }"""
    parsed = apple_hr.parse_apple_hr_json_with_debug(payload)
    assert parsed["samples"] == [{"timestamp": "2026-04-11T07:01:06Z", "hr": 126}]
    assert parsed["debug"]["selected_workout_index"] == 0


def test_parse_auto_health_export_falls_back_when_selected_rows_unparseable():
    payload = """{
      "data": {
        "selectedWorkoutId": "w1",
        "workouts": [
          {"id": "w1", "heartRateData": [{"Avg":126,"units":"bpm"}]},
          {"id": "w2", "heartRateData": [{"date":"2026-04-11 09:01:06 +0200","Avg":130,"units":"bpm"}]}
        ]
      }
    }"""
    parsed = apple_hr.parse_apple_hr_json_with_debug(payload)
    assert parsed["samples"] == [{"timestamp": "2026-04-11T07:01:06Z", "hr": 130}]
    assert parsed["debug"]["selected_workout_index"] == 0
    assert parsed["debug"]["selected_workout_parseable_point_count"] == 0
    assert parsed["debug"]["fallback_workout_index"] == 1


def test_falls_through_when_data_workouts_has_no_points_and_wrapper_has_points():
    payload = """{
      "data": {
        "workouts": [
          {"heartRateData": []}
        ]
      },
      "heartRateData": [
        {"date":"2026-04-11 09:01:06 +0200","Avg":126,"units":"bpm"}
      ]
    }"""
    parsed = apple_hr.parse_apple_hr_json_with_debug(payload)
    assert parsed["samples"] == [{"timestamp": "2026-04-11T07:01:06Z", "hr": 126}]
    assert parsed["debug"]["parser_mode"] == "wrapper_list:heartRateData"


def test_falls_through_when_earlier_dict_workout_wrapper_is_empty():
    payload = """{
      "data": {"workouts": [{"heartRateData": []}]},
      "items": {
        "workouts": [
          {"heartRateData": [{"date":"2026-04-11 09:01:06 +0200","Avg":126,"units":"bpm"}]}
        ]
      }
    }"""
    parsed = apple_hr.parse_apple_hr_json_with_debug(payload)
    assert parsed["samples"] == [{"timestamp": "2026-04-11T07:01:06Z", "hr": 126}]
    assert parsed["debug"]["parser_mode"] == "wrapper_dict_workouts:items"


def test_parse_workout_wrapped_under_items_dict():
    payload = """{
      "items": {
        "workouts": [
          {"heartRateData": [{"date":"2026-04-11 09:01:06 +0200","Avg":126,"units":"bpm"}]}
        ]
      }
    }"""
    parsed = apple_hr.parse_apple_hr_json_with_debug(payload)
    assert parsed["samples"] == [{"timestamp": "2026-04-11T07:01:06Z", "hr": 126}]
    assert parsed["debug"]["parser_mode"] == "wrapper_dict_workouts:items"
    assert parsed["debug"]["workouts_found"] == 1


def test_parse_workout_wrapped_under_samples_dict():
    payload = """{
      "samples": {
        "selectedWorkoutIndex": 1,
        "workouts": [
          {"heartRateData": []},
          {"heartRateData": [{"date":"2026-04-11 09:01:06 +0200","Avg":126,"units":"bpm"}]}
        ]
      }
    }"""
    parsed = apple_hr.parse_apple_hr_json_with_debug(payload)
    assert parsed["samples"] == [{"timestamp": "2026-04-11T07:01:06Z", "hr": 126}]
    assert parsed["debug"]["parser_mode"] == "wrapper_dict_workouts:samples"
    assert parsed["debug"]["selected_workout_index"] == 1


def test_malformed_selected_workout_id_treated_as_non_matching():
    payload = """{
      "data": {
        "selectedWorkoutId": ["bad-selector"],
        "workouts": [
          {"id": "w1", "heartRateData": []},
          {"id": "w2", "heartRateData": [{"date":"2026-04-11 09:01:06 +0200","Avg":126,"units":"bpm"}]}
        ]
      }
    }"""
    parsed = apple_hr.parse_apple_hr_json_with_debug(payload)
    assert parsed["samples"] == [{"timestamp": "2026-04-11T07:01:06Z", "hr": 126}]
    assert parsed["debug"]["selected_workout_index"] == 1


def test_parse_apple_hr_json_compat_wrapper_matches_debug_samples():
    payload = '{"heartRateData":[{"date":"2026-04-11 09:01:06 +0200","Avg":126,"units":"bpm"}]}'
    legacy = apple_hr.parse_apple_hr_json(payload)
    detailed = apple_hr.parse_apple_hr_json_with_debug(payload)
    assert legacy == detailed["samples"]


def test_parse_csv_and_normalize_bounds_and_duplicates():
    payload = "timestamp,hr\n2026-04-10T12:00:00Z,150\n2026-04-10T12:00:00Z,151\n2026-04-10T12:00:01Z,20\n"
    rows = apple_hr.parse_apple_hr_csv(payload)
    normalized = apple_hr.normalize_hr_series(rows, min_hr=30, max_hr=240)
    assert normalized == [{"timestamp": "2026-04-10T12:00:00Z", "hr": 151}]
