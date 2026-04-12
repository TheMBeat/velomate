from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass
from datetime import datetime

from velomate.tools.hr_models import FitRecord, HrPoint


@dataclass(frozen=True)
class MergeOptions:
    tolerance_seconds: int = 2
    overwrite_existing: bool = False


def merge_fit_with_hr(input_fit_records: list[FitRecord], normalized_hr_series: list[HrPoint], options: MergeOptions) -> tuple[list[FitRecord], dict]:
    """Pure merge function used by both web and CLI surfaces.

    Rules:
    - FIT records define canonical timeline.
    - Apple HR points are bounded to FIT start/end and nearest-matched in UTC.
    - No new FIT samples are created.
    """
    if not input_fit_records:
        return [], {
            "total_apple_hr_points": len(normalized_hr_series),
            "total_fit_records": 0,
            "hr_points_matched": 0,
            "hr_points_written": 0,
            "coverage_pct": 0.0,
            "first_hr_timestamp_used": None,
            "last_hr_timestamp_used": None,
            "gaps_over_30s": 0,
            "preserved_existing_hr": 0,
        }

    fit_start = input_fit_records[0].timestamp
    fit_end = input_fit_records[-1].timestamp
    bounded_hr = [p for p in normalized_hr_series if fit_start <= p.timestamp <= fit_end]
    hr_timestamps = [p.timestamp for p in bounded_hr]

    used_hr_times: list[datetime] = []
    merged: list[FitRecord] = []
    matched = 0
    written = 0
    preserved = 0

    for rec in input_fit_records:
        nearest = _nearest_hr(rec.timestamp, bounded_hr, hr_timestamps, options.tolerance_seconds)
        if nearest is None:
            merged.append(rec)
            continue

        matched += 1
        used_hr_times.append(nearest.timestamp)
        if rec.heart_rate is None or options.overwrite_existing:
            merged.append(FitRecord(timestamp=rec.timestamp, heart_rate=nearest.hr))
            written += 1
        else:
            merged.append(rec)
            preserved += 1

    gap_count = sum(1 for a, b in zip(used_hr_times, used_hr_times[1:]) if (b - a).total_seconds() > 30)
    coverage = round((written / len(input_fit_records) * 100.0), 2)

    report = {
        "total_apple_hr_points": len(normalized_hr_series),
        "total_fit_records": len(input_fit_records),
        "hr_points_matched": matched,
        "hr_points_written": written,
        "coverage_pct": coverage,
        "first_hr_timestamp_used": used_hr_times[0].isoformat() if used_hr_times else None,
        "last_hr_timestamp_used": used_hr_times[-1].isoformat() if used_hr_times else None,
        "gaps_over_30s": gap_count,
        "preserved_existing_hr": preserved,
    }
    return merged, report


def _nearest_hr(
    target_ts: datetime,
    hr_points: list[HrPoint],
    hr_timestamps: list[datetime],
    tolerance_seconds: int,
) -> HrPoint | None:
    if not hr_points:
        return None
    idx = bisect_left(hr_timestamps, target_ts)
    candidates: list[HrPoint] = []
    if idx < len(hr_points):
        candidates.append(hr_points[idx])
    if idx > 0:
        candidates.append(hr_points[idx - 1])
    if not candidates:
        return None

    closest = min(candidates, key=lambda p: abs((p.timestamp - target_ts).total_seconds()))
    if abs((closest.timestamp - target_ts).total_seconds()) > tolerance_seconds:
        return None
    return closest
