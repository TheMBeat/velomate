"""Apple Health HR export parsing + normalization helpers.

This module intentionally keeps parsing detached from merge/import logic so it can
be reused by future CLI and web flows.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from io import StringIO


class AppleHrParseError(ValueError):
    """Raised when Apple HR export payload cannot be parsed."""


def _parse_timestamp(raw: str) -> datetime:
    raw = (raw or "").strip()
    if not raw:
        raise AppleHrParseError("Missing timestamp")

    # Support Auto Health Export style: "2026-04-11 09:01:06 +0200"
    for fmt in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            continue

    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise AppleHrParseError(f"Invalid timestamp format: {raw}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_hr_value(raw) -> int:
    if raw in (None, ""):
        raise AppleHrParseError("Missing HR value")
    try:
        return int(round(float(raw)))
    except (TypeError, ValueError) as exc:
        raise AppleHrParseError(f"Invalid HR value: {raw}") from exc


def _sample_from_obj(obj: dict) -> dict | None:
    if not isinstance(obj, dict):
        return None

    ts_raw = obj.get("timestamp") or obj.get("date") or obj.get("time")
    hr_raw = obj.get("hr")

    # Auto Health Export workout JSON uses Avg/Min/Max for heartRateData entries.
    if hr_raw is None:
        hr_raw = obj.get("Avg")

    if ts_raw in (None, "") or hr_raw in (None, ""):
        return None

    return {
        "timestamp": _parse_timestamp(str(ts_raw)).isoformat().replace("+00:00", "Z"),
        "hr": _parse_hr_value(hr_raw),
    }


def _select_workout_index(workouts: list[dict], data_wrapper: dict) -> tuple[int | None, bool]:
    if not workouts:
        return None, False

    selected_index = data_wrapper.get("selectedWorkoutIndex")
    if isinstance(selected_index, int) and 0 <= selected_index < len(workouts):
        return selected_index, True

    selected_workout_id = (
        data_wrapper.get("selectedWorkoutId")
        or data_wrapper.get("selectedWorkoutUUID")
        or data_wrapper.get("workoutId")
        or data_wrapper.get("workoutUUID")
    )
    if selected_workout_id is not None:
        for idx, workout in enumerate(workouts):
            if not isinstance(workout, dict):
                continue
            if selected_workout_id in {
                workout.get("id"),
                workout.get("uuid"),
                workout.get("workoutId"),
                workout.get("workoutUUID"),
            }:
                return idx, True
    return None, False


def _first_workout_with_points(workouts: list[dict]) -> int | None:
    for idx, workout in enumerate(workouts):
        if not isinstance(workout, dict):
            continue
        hr_data = workout.get("heartRateData")
        if isinstance(hr_data, list) and hr_data:
            return idx
    return None


def _iter_json_candidates(payload) -> tuple[list, dict]:
    debug = {
        "parser_mode": "generic",
        "workouts_found": 0,
        "selected_workout_index": None,
        "selected_workout_has_heart_rate_data": False,
        "selected_workout_heart_rate_point_count": 0,
    }

    if isinstance(payload, list):
        debug["parser_mode"] = "json_list"
        return payload, debug
    if isinstance(payload, dict):
        # Handle top-level single sample object directly.
        direct = _sample_from_obj(payload)
        if direct is not None:
            debug["parser_mode"] = "single_sample_object"
            return [payload], debug

        # Explicit Auto Health Export support: data.workouts[].heartRateData[]
        data_wrapper = payload.get("data")
        if isinstance(data_wrapper, dict) and isinstance(data_wrapper.get("workouts"), list):
            workouts = [w for w in data_wrapper.get("workouts", []) if isinstance(w, dict)]
            debug["parser_mode"] = "auto_health_export_data_workouts"
            debug["workouts_found"] = len(workouts)
            selected_idx, selected_explicit = _select_workout_index(workouts, data_wrapper)
            debug["selected_workout_index"] = selected_idx
            if selected_idx is not None:
                hr_data = workouts[selected_idx].get("heartRateData")
                has_hr = isinstance(hr_data, list) and len(hr_data) > 0
                debug["selected_workout_has_heart_rate_data"] = has_hr
                debug["selected_workout_heart_rate_point_count"] = len(hr_data) if isinstance(hr_data, list) else 0
                if has_hr:
                    return hr_data, debug
                # Explicitly selected workout may be empty/corrupt. Fall back to any
                # sibling workout that actually contains HR samples.
                fallback_idx = _first_workout_with_points(workouts)
                if fallback_idx is not None and fallback_idx != selected_idx:
                    debug["fallback_workout_index"] = fallback_idx
                    return workouts[fallback_idx]["heartRateData"], debug
            if not selected_explicit:
                fallback_idx = _first_workout_with_points(workouts)
                if fallback_idx is not None:
                    debug["selected_workout_index"] = fallback_idx
                    debug["selected_workout_has_heart_rate_data"] = True
                    hr_data = workouts[fallback_idx].get("heartRateData")
                    debug["selected_workout_heart_rate_point_count"] = len(hr_data) if isinstance(hr_data, list) else 0
                    return hr_data, debug
            return [], debug

        # Common wrappers.
        for key in ("heartRateData", "heart_rate", "samples", "data", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                debug["parser_mode"] = f"wrapper_list:{key}"
                return value, debug
    return [], debug


def parse_apple_hr_json(text: str) -> list[dict]:
    return parse_apple_hr_json_with_debug(text)["samples"]


def parse_apple_hr_json_with_debug(text: str) -> dict:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise AppleHrParseError("Invalid JSON payload") from exc

    candidates, debug = _iter_json_candidates(payload)
    out: list[dict] = []
    for obj in candidates:
        sample = _sample_from_obj(obj)
        if sample is not None:
            out.append(sample)

    debug["extracted_hr_points"] = len(out)
    print(
        "[apple_hr] JSON parse debug: "
        f"mode={debug['parser_mode']}, workouts_found={debug['workouts_found']}, "
        f"selected_has_heartRateData={debug['selected_workout_has_heart_rate_data']}, "
        f"extracted_hr_points={debug['extracted_hr_points']}"
    )
    return {"samples": out, "debug": debug}


def parse_apple_hr_csv(text: str) -> list[dict]:
    reader = csv.DictReader(StringIO(text))
    out: list[dict] = []
    for row in reader:
        sample = _sample_from_obj(row)
        if sample is not None:
            out.append(sample)
    return out


def normalize_hr_series(series: list[dict], min_hr: int = 30, max_hr: int = 240) -> list[dict]:
    normalized: dict[str, int] = {}
    for item in series:
        if not isinstance(item, dict):
            continue
        ts = item.get("timestamp")
        hr = item.get("hr")
        if ts in (None, "") or hr is None:
            continue
        try:
            value = int(hr)
        except (TypeError, ValueError):
            continue
        if value < min_hr or value > max_hr:
            continue
        normalized[str(ts)] = value

    return [{"timestamp": ts, "hr": normalized[ts]} for ts in sorted(normalized.keys())]
