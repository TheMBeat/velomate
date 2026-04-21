#!/usr/bin/env python3
"""
FIT + Apple Health merger core.

Injects:
- record.heart_rate
- record.power
- record.temperature
- session/lap avg_heart_rate, max_heart_rate
- session/lap avg_power, max_power
- session/lap total_calories

Uses fit-tool builder-based rewrite.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import numpy as np
from fit_tool.definition_message import DefinitionMessage
from fit_tool.fit_file import FitFile
from fit_tool.fit_file_builder import FitFileBuilder
from fit_tool.profile.messages.lap_message import LapMessage
from fit_tool.profile.messages.record_message import RecordMessage
from fit_tool.profile.messages.session_message import SessionMessage


FIT_EPOCH = datetime(1989, 12, 31, tzinfo=timezone.utc).timestamp()


class FitHrMergeError(ValueError):
    """Raised when FIT/Apple merge cannot be performed."""


@dataclass(slots=True)
class MergeOptions:
    overwrite_existing_hr: bool = False
    ignore_implausible_hr: bool = True
    min_hr: int = 30
    max_hr: int = 240


def _parse_health_date(value: str) -> float:
    value = value.strip()
    m = re.match(r"^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}) ([+-]\d{2})(\d{2})$", value)
    if m:
        iso = f"{m[1]}T{m[2]}{m[3]}:{m[4]}"
        return datetime.fromisoformat(iso).timestamp()
    return datetime.fromisoformat(value.replace(" ", "T")).timestamp()


def _fit_ts_to_unix(fit_ts: Any) -> float | None:
    """
    Robuste Konvertierung verschiedener fit-tool Timestamp-Repräsentationen
    auf Unix-Sekunden.
    """
    if fit_ts is None:
        return None

    if isinstance(fit_ts, datetime):
        if fit_ts.tzinfo is None:
            fit_ts = fit_ts.replace(tzinfo=timezone.utc)
        return fit_ts.astimezone(timezone.utc).timestamp()

    try:
        value = float(fit_ts)
    except (TypeError, ValueError) as exc:
        raise FitHrMergeError(f"Unsupported FIT timestamp value: {fit_ts!r}") from exc

    # Unix ms
    if value > 1_500_000_000_000:
        return value / 1000.0

    # FIT ms since 1989-12-31
    if value > 1_000_000_000_000:
        return FIT_EPOCH + (value / 1000.0)

    # Unix seconds
    if value > 1_500_000_000:
        return value

    # FIT seconds since 1989-12-31
    return FIT_EPOCH + value


def _clone_message_to_growable(msg: Any) -> Any:
    new_msg = type(msg)()

    for field in getattr(msg, "fields", []):
        if getattr(field, "size", 0) <= 0:
            continue

        name = field.name
        try:
            value = getattr(msg, name)
        except Exception:
            continue

        if value is None:
            continue

        try:
            setattr(new_msg, name, value)
        except Exception:
            pass

    return new_msg


def _find_matching_workout(workouts: list[dict[str, Any]], fit_start: float, fit_end: float) -> tuple[dict[str, Any], dict]:
    debug = {
        "workouts_found": len(workouts),
        "selected_workout_index": None,
        "selected_workout_id": None,
        "fallback_workout_index": None,
        "fallback_workout_id": None,
        "selected_workout_has_heart_rate_data": False,
        "selected_workout_heart_rate_point_count": 0,
        "selected_workout_parseable_point_count": 0,
    }

    for idx, workout in enumerate(workouts):
        try:
            ws = _parse_health_date(workout["start"])
            we = _parse_health_date(workout.get("end", ""))
            if ws <= fit_start + 120 and we >= fit_end - 120:
                debug["selected_workout_index"] = idx
                debug["selected_workout_id"] = workout.get("id")
                hr_data = workout.get("heartRateData", [])
                debug["selected_workout_has_heart_rate_data"] = bool(hr_data)
                debug["selected_workout_heart_rate_point_count"] = len(hr_data)
                return workout, debug
        except Exception:
            continue

    workout = workouts[0]
    debug["fallback_workout_index"] = 0
    debug["fallback_workout_id"] = workout.get("id")
    hr_data = workout.get("heartRateData", [])
    debug["selected_workout_has_heart_rate_data"] = bool(hr_data)
    debug["selected_workout_heart_rate_point_count"] = len(hr_data)
    return workout, debug


def _extract_calories_kcal(workout: dict[str, Any]) -> int | None:
    burned = workout.get("activeEnergyBurned")
    if burned and burned.get("qty") is not None:
        qty = float(burned["qty"])
        unit = str(burned.get("units", "kJ")).lower()
        if "kj" in unit:
            return int(round(qty / 4.184))
        return int(round(qty))

    active_energy = workout.get("activeEnergy", [])
    if active_energy:
        values = [float(item["qty"]) for item in active_energy if item.get("qty") is not None]
        if values:
            return int(round(sum(values) / 4.184))

    return None


def _load_health_data_from_json_text(text: str, fit_start: float, fit_end: float) -> tuple[dict[str, Any], dict]:
    try:
        jdata = json.loads(text)
    except json.JSONDecodeError as exc:
        raise FitHrMergeError("Apple file is not valid JSON") from exc

    top_level_keys = sorted(jdata.keys()) if isinstance(jdata, dict) else []
    data_node = jdata.get("data", {}) if isinstance(jdata, dict) else {}
    data_keys = sorted(data_node.keys()) if isinstance(data_node, dict) else []

    workouts = data_node.get("workouts", []) if isinstance(data_node, dict) else []
    if not workouts:
        raise FitHrMergeError("Keine Workouts im Apple-Export gefunden.")

    workout, debug = _find_matching_workout(workouts, fit_start, fit_end)

    hr_raw_all = workout.get("heartRateData", [])
    raw_heart_rate_entries_found = len(hr_raw_all)
    parsed_hr = []
    rejected = 0
    rejection_reasons: dict[str, int] = {}

    for entry in hr_raw_all:
        try:
            ts = _parse_health_date(entry["date"])
            avg = int(entry["Avg"])
            parsed_hr.append((ts, avg))
        except Exception:
            rejected += 1
            rejection_reasons["unparseable_hr_entry"] = rejection_reasons.get("unparseable_hr_entry", 0) + 1

    hr_raw = sorted(parsed_hr, key=lambda x: x[0])

    if not hr_raw:
        raise FitHrMergeError("Keine parsebaren heartRateData im Workout gefunden.")

    energy_raw = sorted(
        [
            (_parse_health_date(entry["date"]), float(entry["qty"]))
            for entry in workout.get("activeEnergy", [])
            if "date" in entry and "qty" in entry
        ],
        key=lambda x: x[0],
    )

    pwr_ts: list[float] = []
    pwr_vals: list[float] = []

    if len(energy_raw) >= 2:
        ts_e = np.array([x[0] for x in energy_raw], dtype=float)
        kj_e = np.array([x[1] for x in energy_raw], dtype=float)
        half_window = 5.0

        for t in ts_e:
            mask = (ts_e >= t - half_window) & (ts_e <= t + half_window)
            if mask.sum() < 2:
                continue
            dt = ts_e[mask][-1] - ts_e[mask][0]
            if dt <= 0:
                continue

            metabolic_watts = kj_e[mask].sum() * 1000.0 / dt
            pwr_ts.append(float(t))
            pwr_vals.append(float(metabolic_watts * 0.25))

    temp_c: float | None = None
    temperature = workout.get("temperature")
    if temperature and temperature.get("qty") is not None:
        qty = float(temperature["qty"])
        unit = str(temperature.get("units", "degC")).upper()
        temp_c = qty if "C" in unit else (qty - 32.0) * 5.0 / 9.0

    calories = _extract_calories_kcal(workout)

    hr_vals_all = [value for _, value in hr_raw]
    pwr_arr = np.array(pwr_vals, dtype=float) if pwr_vals else np.array([0.0], dtype=float)

    apple_debug = {
        "requested_source_type": "json",
        "detected_source_type": "json",
        "parser_mode": "health_auto_export_json",
        "top_level_keys": top_level_keys,
        "data_keys": data_keys,
        "raw_heart_rate_entries_found": raw_heart_rate_entries_found,
        "parsed_heart_rate_entries_count": len(hr_raw),
        "rejected_entries_count": rejected,
        "rejection_reasons": rejection_reasons,
        "sample_preview": hr_raw[:5],
        **debug,
    }
    apple_debug["selected_workout_parseable_point_count"] = len(hr_raw)

    return {
        "hr_ts": np.array([x[0] for x in hr_raw], dtype=float),
        "hr_vals": np.array(hr_vals_all, dtype=float),
        "pwr_ts": np.array(pwr_ts, dtype=float),
        "pwr_vals": pwr_arr,
        "temp_c": temp_c,
        "calories": calories,
        "avg_hr": int(round(float(np.mean(hr_vals_all)))),
        "max_hr": int(max(hr_vals_all)),
        "avg_pwr": int(round(float(np.mean(pwr_arr)))),
        "max_pwr": int(round(float(np.max(pwr_arr)))),
    }, apple_debug


def _parse_apple_content(apple_content: bytes, apple_source_type: str, fit_start: float, fit_end: float) -> tuple[dict[str, Any], dict]:
    text = apple_content.decode("utf-8", errors="replace")
    source_type = (apple_source_type or "auto").lower()

    if source_type not in {"auto", "json"}:
        raise FitHrMergeError("Aktuell wird nur JSON als Apple-Quelle unterstützt.")

    return _load_health_data_from_json_text(text, fit_start, fit_end)


def _extract_fit_summary(fit: FitFile) -> tuple[dict[str, Any], list[tuple[float, Any]]]:
    rec_records: list[tuple[float, Any]] = []

    for record in fit.records:
        msg = record.message
        if isinstance(msg, RecordMessage):
            unix_ts = _fit_ts_to_unix(msg.timestamp)
            if unix_ts is not None:
                rec_records.append((unix_ts, record))

    if not rec_records:
        raise FitHrMergeError("Keine Record-Messages in der FIT-Datei gefunden.")

    ts_list = [ts for ts, _ in rec_records]
    fit_start = min(ts_list)
    fit_end = max(ts_list)

    return {
        "start_time": datetime.fromtimestamp(fit_start, tz=timezone.utc).isoformat(),
        "end_time": datetime.fromtimestamp(fit_end, tz=timezone.utc).isoformat(),
        "duration_s": max(int(fit_end - fit_start), 0),
        "record_count": len(rec_records),
    }, rec_records


def preview_fit_hr_merge(
    fit_filename: str,
    fit_content: bytes,
    apple_content: bytes,
    apple_source_type: str,
) -> tuple[dict, dict]:
    try:
        fit = FitFile.from_bytes(fit_content)
    except Exception as exc:
        raise FitHrMergeError("FIT-Datei konnte nicht gelesen werden.") from exc

    fit_summary, rec_records = _extract_fit_summary(fit)
    ts_list = [ts for ts, _ in rec_records]
    fit_start = min(ts_list)
    fit_end = max(ts_list)

    health_data, apple_debug = _parse_apple_content(apple_content, apple_source_type, fit_start, fit_end)

    overlap_points = int(
        np.sum((health_data["hr_ts"] >= fit_start) & (health_data["hr_ts"] <= fit_end))
    )

    response = {
        "fit_summary": {
            **fit_summary,
            "source_file_name": fit_filename,
        },
        "apple_summary": {
            "point_count": int(len(health_data["hr_ts"])),
            "avg_hr": health_data["avg_hr"],
            "max_hr": health_data["max_hr"],
            "avg_power": health_data["avg_pwr"],
            "max_power": health_data["max_pwr"],
            "calories": health_data["calories"],
            "has_temperature": health_data["temp_c"] is not None,
        },
        "estimated_overlap_points": overlap_points,
        "warnings": [],
        "apple_debug": apple_debug,
        "parser_diagnostics": {
            "apple_debug_present": True,
        },
    }

    merge_payload = {
        "fit_filename": fit_filename,
        "fit_content": fit_content,
        "apple_content": apple_content,
        "apple_source_type": apple_source_type,
        "fit_summary": response["fit_summary"],
        "apple_summary": response["apple_summary"],
        "apple_debug": apple_debug,
    }

    return merge_payload, response


def merge_fit_hr_payload(payload: dict, options: MergeOptions) -> tuple[str, bytes, dict]:
    fit_filename = payload["fit_filename"]
    fit_content = payload["fit_content"]
    apple_content = payload["apple_content"]
    apple_source_type = payload.get("apple_source_type", "auto")

    try:
        fit = FitFile.from_bytes(fit_content)
    except Exception as exc:
        raise FitHrMergeError("FIT-Datei konnte nicht gelesen werden.") from exc

    fit_summary, rec_records = _extract_fit_summary(fit)
    ts_list = [ts for ts, _ in rec_records]
    fit_start = min(ts_list)
    fit_end = max(ts_list)

    health_data, apple_debug = _parse_apple_content(apple_content, apple_source_type, fit_start, fit_end)

    ts_arr = np.array(ts_list, dtype=float)

    hr_interp = np.interp(
        ts_arr,
        health_data["hr_ts"],
        health_data["hr_vals"],
        left=np.nan,
        right=np.nan,
    )

    if len(health_data["pwr_ts"]) > 1:
        pwr_interp = np.interp(
            ts_arr,
            health_data["pwr_ts"],
            health_data["pwr_vals"],
            left=np.nan,
            right=np.nan,
        )
    else:
        pwr_interp = np.full(len(ts_list), health_data["avg_pwr"], dtype=float)

    valid_hr = np.isfinite(hr_interp)
    if not valid_hr.any():
        raise FitHrMergeError(
            "No time overlap between FIT record timestamps and Apple HR data. "
            "Timestamp conversion is likely wrong."
        )

    temp_int = int(round(health_data["temp_c"])) if health_data["temp_c"] is not None else None

    builder = FitFileBuilder(auto_define=True, min_string_size=50)

    hr_points_written = 0
    fit_records_patched_in_binary = 0
    records_missing_hr_after_merge = 0
    record_index = 0

    for record in fit.records:
        msg = record.message

        if isinstance(msg, DefinitionMessage):
            continue

        new_msg = _clone_message_to_growable(msg)

        if isinstance(new_msg, RecordMessage):
            existing_hr = getattr(new_msg, "heart_rate", None)

            hr_value = hr_interp[record_index]
            pwr_value = pwr_interp[record_index]

            if np.isfinite(hr_value):
                target_hr = int(np.clip(round(hr_value), 0, 255))
                if options.ignore_implausible_hr and not (options.min_hr <= target_hr <= options.max_hr):
                    if existing_hr in (None, 0):
                        records_missing_hr_after_merge += 1
                else:
                    if options.overwrite_existing_hr or existing_hr in (None, 0):
                        new_msg.heart_rate = target_hr
                        hr_points_written += 1
            else:
                if existing_hr in (None, 0):
                    records_missing_hr_after_merge += 1

            if np.isfinite(pwr_value):
                target_pwr = int(np.clip(round(pwr_value), 0, 65535))
                new_msg.power = target_pwr

            if temp_int is not None:
                new_msg.temperature = int(np.clip(temp_int, -128, 127))

            fit_records_patched_in_binary += 1

            final_hr = getattr(new_msg, "heart_rate", None)
            if final_hr in (None, 0):
                records_missing_hr_after_merge += 1

            record_index += 1

        elif isinstance(new_msg, (SessionMessage, LapMessage)):
            new_msg.avg_heart_rate = int(np.clip(health_data["avg_hr"], 0, 255))
            new_msg.max_heart_rate = int(np.clip(health_data["max_hr"], 0, 255))
            new_msg.avg_power = int(np.clip(health_data["avg_pwr"], 0, 65535))
            new_msg.max_power = int(np.clip(health_data["max_pwr"], 0, 65535))
            if health_data["calories"] is not None:
                new_msg.total_calories = int(np.clip(health_data["calories"], 0, 65535))

        builder.add(new_msg)

    new_fit = builder.build()
    output_content = new_fit.to_bytes()

    base_name = os.path.basename(fit_filename)
    stem, ext = os.path.splitext(base_name)
    output_name = f"{stem}_merged{ext or '.fit'}"

    matched = int(np.sum((health_data["hr_ts"] >= fit_start) & (health_data["hr_ts"] <= fit_end)))
    coverage_pct = round((hr_points_written / len(rec_records)) * 100.0, 2) if rec_records else 0.0

    report = {
        "fit_summary": fit_summary,
        "apple_debug": apple_debug,
        "hr_points_matched": matched,
        "hr_points_written": hr_points_written,
        "fit_records_patched_in_binary": fit_records_patched_in_binary,
        "coverage_pct": coverage_pct,
        "records_missing_hr_after_merge": records_missing_hr_after_merge,
        "writer_diagnostics": {
            "builder_mode": "fit_tool_builder",
            "output_size_bytes": len(output_content),
        },
        "timing_ms": {},
        "summary": {
            "avg_hr": health_data["avg_hr"],
            "max_hr": health_data["max_hr"],
            "avg_power": health_data["avg_pwr"],
            "max_power": health_data["max_pwr"],
            "calories": health_data["calories"],
            "temperature_c": health_data["temp_c"],
        },
    }

    return output_name, output_content, report
