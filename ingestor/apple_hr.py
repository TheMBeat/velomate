"""Apple Health HR export parsing + normalization helpers."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from io import StringIO
from typing import Any


class AppleHrParseError(ValueError):
    """Raised when Apple HR export payload cannot be parsed."""


WORKOUT_WRAPPER_KEYS = ("data", "samples", "items", "heart_rate", "heartRateData")
DIRECT_LIST_WRAPPER_KEYS = ("heartRateData", "heart_rate", "samples", "data", "items")
WORKOUT_ID_KEYS = ("id", "uuid", "workoutId", "workoutUUID")


def _empty_debug(parser_mode: str = "generic") -> dict[str, Any]:
    return {
        "parser_mode": parser_mode,
        "top_level_keys": [],
        "data_keys": [],
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


def _parse_hr_value(raw: Any) -> int:
    if raw in (None, ""):
        raise AppleHrParseError("Missing HR value")
    try:
        return int(round(float(raw)))
    except (TypeError, ValueError) as exc:
        raise AppleHrParseError(f"Invalid HR value: {raw}") from exc


def _sample_from_obj(obj: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(obj, dict):
        return None

    ts_raw = obj.get("timestamp") or obj.get("date") or obj.get("time") or obj.get("startDate")
    hr_raw = obj.get("hr")
    if hr_raw is None:
        hr_raw = obj.get("Avg")
    if hr_raw is None:
        hr_raw = obj.get("value")
    if hr_raw is None:
        hr_raw = obj.get("bpm")
    if hr_raw is None:
        hr_raw = obj.get("heartRate")
    if isinstance(hr_raw, dict):
        hr_raw = hr_raw.get("value", hr_raw.get("bpm"))

    if ts_raw in (None, "") or hr_raw in (None, ""):
        return None

    return {
        "timestamp": _parse_timestamp(str(ts_raw)).isoformat().replace("+00:00", "Z"),
        "hr": _parse_hr_value(hr_raw),
    }


def _parse_samples(candidates: list[Any]) -> tuple[list[dict[str, Any]], int, dict[str, int]]:
    samples: list[dict[str, Any]] = []
    rejection_reasons: dict[str, int] = {}
    rejected = 0

    for obj in candidates:
        try:
            sample = _sample_from_obj(obj)
        except AppleHrParseError as exc:
            rejected += 1
            reason = str(exc).strip() or exc.__class__.__name__
            rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1
            continue

        if sample is None:
            rejected += 1
            reason = "Missing timestamp or HR value"
            rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1
            continue
        samples.append(sample)

    return samples, rejected, rejection_reasons


def _workout_identifier(workout: dict[str, Any]) -> Any:
    for key in WORKOUT_ID_KEYS:
        value = workout.get(key)
        if value not in (None, ""):
            return value
    return None


def _parse_workout_bounds(workout: dict[str, Any]) -> tuple[datetime | None, datetime | None]:
    start_raw = workout.get("start")
    end_raw = workout.get("end")
    if start_raw in (None, "") or end_raw in (None, ""):
        return None, None
    try:
        return _parse_timestamp(str(start_raw)), _parse_timestamp(str(end_raw))
    except AppleHrParseError:
        return None, None


def _parseable_hr_point_count(hr_data: Any) -> int:
    if not isinstance(hr_data, list):
        return 0
    parsed, _, _ = _parse_samples(hr_data)
    return len(parsed)


def _compute_overlap_seconds(
    workout_start: datetime,
    workout_end: datetime,
    fit_start: datetime,
    fit_end: datetime,
) -> float:
    overlap_start = max(workout_start, fit_start)
    overlap_end = min(workout_end, fit_end)
    return max(0.0, (overlap_end - overlap_start).total_seconds())


def _select_workout_by_overlap(
    workouts: list[dict[str, Any]],
    fit_start: datetime | None,
    fit_end: datetime | None,
) -> tuple[int | None, float | None]:
    if fit_start is None or fit_end is None:
        return None, None

    best_index = None
    best_overlap = -1.0
    computed_any = False

    for idx, workout in enumerate(workouts):
        w_start, w_end = _parse_workout_bounds(workout)
        if w_start is None or w_end is None:
            continue
        computed_any = True
        overlap = _compute_overlap_seconds(w_start, w_end, fit_start, fit_end)
        if overlap > best_overlap:
            best_overlap = overlap
            best_index = idx

    if not computed_any:
        return None, None
    return best_index, best_overlap


def _select_workout_by_parseable_points(workouts: list[dict[str, Any]]) -> int | None:
    best_index = None
    best_count = 0
    for idx, workout in enumerate(workouts):
        count = _parseable_hr_point_count(workout.get("heartRateData"))
        if count > best_count:
            best_count = count
            best_index = idx
    return best_index


def _set_selected_workout_debug(debug: dict[str, Any], workouts: list[dict[str, Any]], index: int | None) -> int:
    if index is None or index < 0 or index >= len(workouts):
        debug["selected_workout_index"] = None
        debug["selected_workout_id"] = None
        debug["selected_workout_has_heart_rate_data"] = False
        debug["selected_workout_heart_rate_point_count"] = 0
        debug["selected_workout_parseable_point_count"] = 0
        return 0

    selected = workouts[index]
    hr_data = selected.get("heartRateData")
    parseable_count = _parseable_hr_point_count(hr_data)
    debug["selected_workout_index"] = index
    debug["selected_workout_id"] = _workout_identifier(selected)
    debug["selected_workout_has_heart_rate_data"] = isinstance(hr_data, list) and len(hr_data) > 0
    debug["selected_workout_heart_rate_point_count"] = len(hr_data) if isinstance(hr_data, list) else 0
    debug["selected_workout_parseable_point_count"] = parseable_count
    return parseable_count


def _discover_workouts(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    for key in WORKOUT_WRAPPER_KEYS:
        wrapper = payload.get(key) if key != "data" else payload.get("data")
        if isinstance(wrapper, dict) and isinstance(wrapper.get("workouts"), list):
            workouts = [w for w in wrapper["workouts"] if isinstance(w, dict)]
            return workouts, "auto_health_export_data_workouts" if key == "data" else f"wrapper_dict_workouts:{key}"

    if isinstance(payload.get("workouts"), list):
        return [w for w in payload["workouts"] if isinstance(w, dict)], "top_level_workouts"
    return [], "generic"


def _iter_json_candidates(
    payload: Any,
    *,
    fit_start: datetime | None,
    fit_end: datetime | None,
) -> tuple[list[Any], dict[str, Any]]:
    if isinstance(payload, list):
        debug = _empty_debug("json_list")
        debug["top_level_keys"] = []
        return payload, debug
    if not isinstance(payload, dict):
        return [], _empty_debug()

    direct = _sample_from_obj(payload)
    if direct is not None:
        debug = _empty_debug("single_sample_object")
        debug["top_level_keys"] = sorted(payload.keys())
        return [payload], debug

    workouts, parser_mode = _discover_workouts(payload)
    if workouts:
        debug = _empty_debug(parser_mode)
        debug["top_level_keys"] = sorted(payload.keys())
        if isinstance(payload.get("data"), dict):
            debug["data_keys"] = sorted(payload["data"].keys())
        debug["workouts_found"] = len(workouts)

        overlap_idx, overlap_seconds = _select_workout_by_overlap(workouts, fit_start, fit_end)
        has_overlap_selection = overlap_idx is not None and overlap_seconds is not None and overlap_seconds > 0

        if has_overlap_selection:
            overlap_parseable_count = _set_selected_workout_debug(debug, workouts, overlap_idx)
            if overlap_parseable_count > 0:
                selected = workouts[overlap_idx]
                hr_data = selected.get("heartRateData")
                return hr_data if isinstance(hr_data, list) else [], debug

        fallback_idx = _select_workout_by_parseable_points(workouts)
        if fallback_idx is not None:
            if has_overlap_selection:
                debug["fallback_workout_index"] = fallback_idx
                debug["fallback_workout_id"] = _workout_identifier(workouts[fallback_idx])
            else:
                _set_selected_workout_debug(debug, workouts, fallback_idx)
                debug["fallback_workout_index"] = fallback_idx
                debug["fallback_workout_id"] = _workout_identifier(workouts[fallback_idx])

            selected = workouts[fallback_idx]
            hr_data = selected.get("heartRateData")
            return hr_data if isinstance(hr_data, list) else [], debug
        return [], debug

    for key in DIRECT_LIST_WRAPPER_KEYS:
        value = payload.get(key)
        if isinstance(value, list):
            debug = _empty_debug(f"wrapper_list:{key}")
            debug["top_level_keys"] = sorted(payload.keys())
            if key == "data":
                debug["data_keys"] = sorted(payload.keys())
            return value, debug

    return [], _empty_debug()


def _parse_json_payload_with_debug(
    text: str,
    *,
    fit_start: datetime | None,
    fit_end: datetime | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise AppleHrParseError("Invalid JSON payload") from exc

    candidates, debug = _iter_json_candidates(payload, fit_start=fit_start, fit_end=fit_end)
    samples, rejected, rejection_reasons = _parse_samples(candidates)
    debug["raw_heart_rate_entries_found"] = len(candidates)
    debug["parsed_heart_rate_entries_count"] = len(samples)
    debug["rejected_entries_count"] = rejected
    debug["rejection_reasons"] = rejection_reasons
    debug["extracted_hr_points"] = len(samples)
    return samples, debug


def parse_apple_hr_json(text: str) -> list[dict[str, Any]]:
    return parse_apple_hr_json_with_debug(text)["samples"]


def parse_apple_hr_json_with_debug(
    text: str,
    *,
    fit_start: datetime | None = None,
    fit_end: datetime | None = None,
) -> dict[str, Any]:
    samples, debug = _parse_json_payload_with_debug(text, fit_start=fit_start, fit_end=fit_end)
    return {"samples": samples, "debug": debug, "source_type": "json"}


def parse_apple_hr_csv(text: str) -> list[dict[str, Any]]:
    reader = csv.DictReader(StringIO(text))
    samples, _, _ = _parse_samples(list(reader))
    return samples


def parse_apple_hr_csv_with_debug(text: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
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


def parse_apple_hr_text_details(
    text: str,
    source_type: str = "auto",
    *,
    fit_start: datetime | None = None,
    fit_end: datetime | None = None,
) -> dict[str, Any]:
    mode = (source_type or "auto").lower()
    if mode == "json":
        return parse_apple_hr_json_with_debug(text, fit_start=fit_start, fit_end=fit_end)
    if mode == "csv":
        samples, debug = parse_apple_hr_csv_with_debug(text)
        return {"samples": samples, "source_type": "csv", "debug": debug}
    if mode == "auto":
        stripped = text.lstrip()
        inferred_mode = "json" if stripped.startswith(("{", "[")) else "csv"
        return parse_apple_hr_text_details(text, source_type=inferred_mode, fit_start=fit_start, fit_end=fit_end)
    raise AppleHrParseError("Unsupported Apple source type")


def normalize_hr_series(series: list[dict[str, Any]], min_hr: int = 30, max_hr: int = 240) -> list[dict[str, Any]]:
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
