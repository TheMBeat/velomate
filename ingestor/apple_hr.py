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


def _iter_json_candidates(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        # Handle top-level single sample object directly.
        direct = _sample_from_obj(payload)
        if direct is not None:
            return [payload]

        # Common wrappers.
        for key in ("heartRateData", "heart_rate", "samples", "data", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return []


def parse_apple_hr_json(text: str) -> list[dict]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise AppleHrParseError("Invalid JSON payload") from exc

    out: list[dict] = []
    for obj in _iter_json_candidates(payload):
        sample = _sample_from_obj(obj)
        if sample is not None:
            out.append(sample)
    return out


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
