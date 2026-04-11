from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from io import StringIO
from typing import Iterable

from velomate.tools.hr_models import HrPoint


def parse_apple_hr(raw: bytes, source_type: str = "auto", ignore_implausible: bool = True, min_hr: int = 30, max_hr: int = 240) -> list[HrPoint]:
    source_type = (source_type or "auto").lower()
    if source_type == "json":
        return parse_apple_json(raw, ignore_implausible, min_hr, max_hr)
    if source_type == "csv":
        return parse_apple_csv(raw, ignore_implausible, min_hr, max_hr)

    raw_trim = raw.lstrip()
    if raw_trim.startswith(b"{") or raw_trim.startswith(b"["):
        return parse_apple_json(raw, ignore_implausible, min_hr, max_hr)
    return parse_apple_csv(raw, ignore_implausible, min_hr, max_hr)


def parse_apple_json(raw: bytes, ignore_implausible: bool = True, min_hr: int = 30, max_hr: int = 240) -> list[HrPoint]:
    data = json.loads(raw.decode("utf-8"))
    entries = _extract_json_entries(data)

    parsed: list[HrPoint] = []
    for item in entries:
        ts = item.get("timestamp") or item.get("date") or item.get("time")
        hr = item.get("Avg") or item.get("hr") or item.get("value") or item.get("bpm")
        if ts is None or hr is None:
            continue
        try:
            parsed.append(HrPoint(timestamp=_parse_ts(str(ts)), hr=int(hr)))
        except Exception:
            continue

    return _normalize(parsed, ignore_implausible, min_hr, max_hr)


def parse_apple_csv(raw: bytes, ignore_implausible: bool = True, min_hr: int = 30, max_hr: int = 240) -> list[HrPoint]:
    text = raw.decode("utf-8")
    reader = csv.DictReader(StringIO(text))
    parsed: list[HrPoint] = []
    for row in reader:
        ts = row.get("timestamp") or row.get("date") or row.get("time")
        hr = row.get("hr") or row.get("heart_rate") or row.get("value") or row.get("bpm")
        if ts is None or hr is None:
            continue
        try:
            parsed.append(HrPoint(timestamp=_parse_ts(ts), hr=int(float(hr))))
        except Exception:
            continue

    return _normalize(parsed, ignore_implausible, min_hr, max_hr)


def _extract_json_entries(data) -> list[dict]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]

    if not isinstance(data, dict):
        return []

    # Real Auto Health Export workout format: workouts[].heartRateData[]
    workouts = data.get("workouts")
    if isinstance(workouts, list):
        points: list[dict] = []
        for workout in workouts:
            if not isinstance(workout, dict):
                continue
            hr_data = workout.get("heartRateData")
            if isinstance(hr_data, list):
                points.extend(x for x in hr_data if isinstance(x, dict))
        if points:
            return points

    direct = data.get("heartRateData") or data.get("heart_rate") or data.get("samples") or data.get("data")
    if isinstance(direct, list):
        return [x for x in direct if isinstance(x, dict)]

    return []


def _parse_ts(raw: str) -> datetime:
    value = raw.strip()
    if "T" not in value and ("+" in value or "-" in value[10:]) and value.count(":") >= 2:
        for fmt in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%d %H:%M:%S %Z"):
            try:
                dt = datetime.strptime(value, fmt)
                return dt.astimezone(timezone.utc)
            except ValueError:
                continue

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize(points: Iterable[HrPoint], ignore_implausible: bool, min_hr: int, max_hr: int) -> list[HrPoint]:
    by_ts: dict[datetime, int] = {}
    for p in points:
        if ignore_implausible and (p.hr < min_hr or p.hr > max_hr):
            continue
        by_ts[p.timestamp] = p.hr
    return [HrPoint(timestamp=ts, hr=by_ts[ts]) for ts in sorted(by_ts)]
