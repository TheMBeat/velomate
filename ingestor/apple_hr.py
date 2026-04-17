"""Apple Health HR export parsing + normalization helpers.

This module intentionally keeps parsing detached from merge/import logic so it can
be reused by future CLI and web flows.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from io import StringIO
from typing import Any


class AppleHrParseError(ValueError):
    """Raised when Apple HR export payload cannot be parsed."""


WORKOUT_WRAPPER_KEYS = ("samples", "items", "heart_rate", "heartRateData")
DIRECT_LIST_WRAPPER_KEYS = ("heartRateData", "heart_rate", "samples", "data", "items")
WORKOUT_ID_KEYS = ("id", "uuid", "workoutId", "workoutUUID")
WORKOUT_SELECTOR_KEYS = ("selectedWorkoutId", "selectedWorkoutUUID", "workoutId", "workoutUUID")


def _empty_debug(parser_mode: str = "generic") -> dict[str, Any]:
    return {
        "parser_mode": parser_mode,
        "workouts_found": 0,
        "selected_workout_index": None,
        "selected_workout_id": None,
        "selected_workout_has_heart_rate_data": False,
        "selected_workout_heart_rate_point_count": 0,
        "selected_workout_parseable_point_count": 0,
        "fallback_workout_index": None,
        "fallback_workout_id": None,
        "raw_heart_rate_entries_found": 0,
        "parsed_heart_rate_entries_count": 0,
        "rejected_entries_count": 0,
        "rejection_reasons": {},
        "extracted_hr_points": 0,
    }


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


def _rejection_reason(exc: Exception) -> str:
    message = str(exc).strip()
    return message or exc.__class__.__name__


def _parse_samples(candidates: list[Any]) -> tuple[list[dict], int, dict[str, int]]:
    samples: list[dict] = []
    rejection_reasons: dict[str, int] = {}
    rejected = 0
    for obj in candidates:
        try:
            sample = _sample_from_obj(obj)
        except AppleHrParseError as exc:
            rejected += 1
            reason = _rejection_reason(exc)
            rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1
            continue
        if sample is not None:
            samples.append(sample)
        else:
            rejected += 1
            reason = "Missing timestamp or HR value"
            rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1
    return samples, rejected, rejection_reasons


def _parseable_hr_point_count(hr_data: Any) -> int:
    if not isinstance(hr_data, list):
        return 0
    parsed, _, _ = _parse_samples(hr_data)
    return len(parsed)


def _workout_identifier(workout: dict) -> Any:
    for key in WORKOUT_ID_KEYS:
        value = workout.get(key)
        if value not in (None, ""):
            return value
    return None


def _find_matching_workout_index(workouts: list[dict], wrapper: dict) -> tuple[int | None, bool]:
    if not workouts:
        return None, False

    selected_index = wrapper.get("selectedWorkoutIndex")
    if isinstance(selected_index, int) and not isinstance(selected_index, bool):
        if 0 <= selected_index < len(workouts):
            return selected_index, True
        return None, False

    selected_workout_id: Any = None
    selected_specified = False
    for key in WORKOUT_SELECTOR_KEYS:
        if key in wrapper:
            selected_workout_id = wrapper.get(key)
            selected_specified = True
            break

    if selected_specified:
        if isinstance(selected_workout_id, (dict, list, set, tuple)):
            return None, False
        for idx, workout in enumerate(workouts):
            if not isinstance(workout, dict):
                continue
            for workout_key in WORKOUT_ID_KEYS:
                if selected_workout_id == workout.get(workout_key):
                    return idx, True
        return None, False

    return None, False


def _first_workout_with_points(workouts: list[dict], skip_index: int | None = None) -> int | None:
    for idx, workout in enumerate(workouts):
        if skip_index is not None and idx == skip_index:
            continue
        if not isinstance(workout, dict):
            continue
        if _parseable_hr_point_count(workout.get("heartRateData")) > 0:
            return idx
    return None


def _first_workout_with_raw_entries(workouts: list[dict], skip_index: int | None = None) -> int | None:
    for idx, workout in enumerate(workouts):
        if skip_index is not None and idx == skip_index:
            continue
        if not isinstance(workout, dict):
            continue
        hr_data = workout.get("heartRateData")
        if isinstance(hr_data, list) and len(hr_data) > 0:
            return idx
    return None


def _discover_workout_candidates(wrapper: Any, parser_mode: str) -> tuple[list, dict] | None:
    if not isinstance(wrapper, dict):
        return None

    workouts_raw = wrapper.get("workouts")
    if not isinstance(workouts_raw, list):
        return None

    workouts = [w for w in workouts_raw if isinstance(w, dict)]
    debug = _empty_debug(parser_mode)
    debug["workouts_found"] = len(workouts)

    selected_idx, selected_explicit = _find_matching_workout_index(workouts, wrapper)
    debug["selected_workout_index"] = selected_idx

    if selected_idx is not None:
        selected_workout = workouts[selected_idx]
        hr_data = selected_workout.get("heartRateData")
        parseable_count = _parseable_hr_point_count(hr_data)
        debug["selected_workout_id"] = _workout_identifier(selected_workout)
        debug["selected_workout_heart_rate_point_count"] = len(hr_data) if isinstance(hr_data, list) else 0
        debug["selected_workout_parseable_point_count"] = parseable_count
        debug["selected_workout_has_heart_rate_data"] = isinstance(hr_data, list) and len(hr_data) > 0
        if parseable_count > 0:
            return hr_data, debug

        fallback_idx = _first_workout_with_points(workouts, skip_index=selected_idx)
        if fallback_idx is not None:
            debug["fallback_workout_index"] = fallback_idx
            debug["fallback_workout_id"] = _workout_identifier(workouts[fallback_idx])
            return workouts[fallback_idx]["heartRateData"], debug
        if isinstance(hr_data, list) and len(hr_data) > 0:
            return hr_data, debug

    if not selected_explicit:
        fallback_idx = _first_workout_with_points(workouts)
        if fallback_idx is not None:
            hr_data = workouts[fallback_idx].get("heartRateData")
            debug["selected_workout_index"] = fallback_idx
            debug["selected_workout_id"] = _workout_identifier(workouts[fallback_idx])
            debug["selected_workout_has_heart_rate_data"] = True
            debug["selected_workout_heart_rate_point_count"] = len(hr_data) if isinstance(hr_data, list) else 0
            debug["selected_workout_parseable_point_count"] = _parseable_hr_point_count(hr_data)
            return hr_data, debug
        fallback_idx = _first_workout_with_raw_entries(workouts)
        if fallback_idx is not None:
            hr_data = workouts[fallback_idx].get("heartRateData")
            debug["selected_workout_index"] = fallback_idx
            debug["selected_workout_id"] = _workout_identifier(workouts[fallback_idx])
            debug["selected_workout_has_heart_rate_data"] = True
            debug["selected_workout_heart_rate_point_count"] = len(hr_data) if isinstance(hr_data, list) else 0
            debug["selected_workout_parseable_point_count"] = 0
            return hr_data, debug

    return [], debug


def _iter_json_candidates(payload: Any) -> tuple[list, dict]:
    if isinstance(payload, list):
        return payload, _empty_debug("json_list")

    if not isinstance(payload, dict):
        return [], _empty_debug()

    direct = _sample_from_obj(payload)
    if direct is not None:
        return [payload], _empty_debug("single_sample_object")

    first_workout_debug = None
    first_unparseable_candidates: tuple[list, dict] | None = None
    result = _discover_workout_candidates(payload.get("data"), parser_mode="auto_health_export_data_workouts")
    if result is not None:
        if result[0] and _parseable_hr_point_count(result[0]) > 0:
            return result
        if result[0] and first_unparseable_candidates is None:
            first_unparseable_candidates = result
        first_workout_debug = result[1]

    for key in WORKOUT_WRAPPER_KEYS:
        wrapper = payload.get(key)
        result = _discover_workout_candidates(wrapper, parser_mode=f"wrapper_dict_workouts:{key}")
        if result is not None:
            if result[0] and _parseable_hr_point_count(result[0]) > 0:
                return result
            if result[0] and first_unparseable_candidates is None:
                first_unparseable_candidates = result
            if first_workout_debug is None:
                first_workout_debug = result[1]
    for key in DIRECT_LIST_WRAPPER_KEYS:
        value = payload.get(key)
        if isinstance(value, list):
            list_result = (value, _empty_debug(parser_mode=f"wrapper_list:{key}"))
            if _parseable_hr_point_count(value) > 0:
                return list_result
            if first_unparseable_candidates is None:
                first_unparseable_candidates = list_result

    if first_unparseable_candidates is not None:
        return first_unparseable_candidates
    if first_workout_debug is not None:
        return [], first_workout_debug
    return [], _empty_debug()


def _parse_json_payload_with_debug(text: str) -> tuple[list[dict], dict]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise AppleHrParseError("Invalid JSON payload") from exc

    candidates, debug = _iter_json_candidates(payload)
    samples, rejected, rejection_reasons = _parse_samples(candidates)
    debug["raw_heart_rate_entries_found"] = len(candidates)
    debug["parsed_heart_rate_entries_count"] = len(samples)
    debug["rejected_entries_count"] = rejected
    debug["rejection_reasons"] = rejection_reasons
    debug["extracted_hr_points"] = len(samples)
    return samples, debug


def parse_apple_hr_json(text: str) -> list[dict]:
    return parse_apple_hr_json_with_debug(text)["samples"]


def parse_apple_hr_json_with_debug(text: str) -> dict:
    samples, debug = _parse_json_payload_with_debug(text)
    print(
        "[apple_hr] JSON parse debug: "
        f"mode={debug['parser_mode']}, workouts_found={debug['workouts_found']}, "
        f"selected_idx={debug['selected_workout_index']}, "
        f"selected_id={debug['selected_workout_id']}, "
        f"selected_has_heartRateData={debug['selected_workout_has_heart_rate_data']}, "
        f"raw_entries={debug['raw_heart_rate_entries_found']}, "
        f"parsed_entries={debug['parsed_heart_rate_entries_count']}, "
        f"rejected_entries={debug['rejected_entries_count']}"
    )
    if debug["rejected_entries_count"] > 0:
        print(f"[apple_hr] JSON parse rejections: {debug['rejection_reasons']}")
    return {"samples": samples, "debug": debug}


def parse_apple_hr_csv(text: str) -> list[dict]:
    reader = csv.DictReader(StringIO(text))
    samples, _rejected, _reasons = _parse_samples(list(reader))
    return samples


def parse_apple_hr_csv_with_debug(text: str) -> tuple[list[dict], dict[str, Any]]:
    reader = csv.DictReader(StringIO(text))
    raw_rows = list(reader)
    samples, rejected, rejection_reasons = _parse_samples(raw_rows)
    debug = _empty_debug("csv")
    debug["raw_heart_rate_entries_found"] = len(raw_rows)
    debug["parsed_heart_rate_entries_count"] = len(samples)
    debug["rejected_entries_count"] = rejected
    debug["rejection_reasons"] = rejection_reasons
    debug["extracted_hr_points"] = len(samples)
    return samples, debug


def parse_apple_hr_text_details(text: str, source_type: str = "auto") -> dict:
    mode = (source_type or "auto").lower()
    if mode == "json":
        parsed = parse_apple_hr_json_with_debug(text)
        parsed["source_type"] = "json"
        return parsed
    if mode == "csv":
        samples, debug = parse_apple_hr_csv_with_debug(text)
        return {
            "samples": samples,
            "source_type": "csv",
            "debug": debug,
        }
    if mode == "auto":
        stripped = text.lstrip()
        inferred_mode = "json" if stripped.startswith(("{", "[")) else "csv"
        return parse_apple_hr_text_details(text, source_type=inferred_mode)
    raise AppleHrParseError("Unsupported Apple source type")


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
