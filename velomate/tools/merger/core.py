from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from velomate.tools.hr_models import FitRecord, HrPoint
from velomate.tools.merger.matching import MatchOptions, find_match


@dataclass(frozen=True)
class MergeOptions:
    tolerance_seconds: int = 2
    overwrite_existing: bool = False
    strategy: str = "nearest"


def merge_fit_with_hr(input_fit_records: list[FitRecord], normalized_hr_series: list[HrPoint], options: MergeOptions) -> tuple[list[FitRecord], dict]:
    """Pure merge function reusable across web/CLI integrations."""
    if not input_fit_records:
        return [], _empty_report(len(normalized_hr_series))

    fit_start = input_fit_records[0].timestamp
    fit_end = input_fit_records[-1].timestamp
    bounded_hr = [p for p in normalized_hr_series if fit_start <= p.timestamp <= fit_end]
    hr_timestamps = [p.timestamp for p in bounded_hr]

    merged: list[FitRecord] = []
    used_hr_times: list[datetime] = []
    matched = 0
    written = 0
    preserved = 0

    match_options = MatchOptions(tolerance_seconds=options.tolerance_seconds, strategy=options.strategy)

    for rec in input_fit_records:
        nearest = find_match(rec.timestamp, bounded_hr, hr_timestamps, match_options)
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

    return merged, _report(len(normalized_hr_series), len(input_fit_records), matched, written, preserved, used_hr_times)


def _empty_report(total_apple_hr_points: int) -> dict:
    return {
        "total_apple_hr_points": total_apple_hr_points,
        "total_fit_records": 0,
        "hr_points_matched": 0,
        "hr_points_written": 0,
        "coverage_pct": 0.0,
        "first_hr_timestamp_used": None,
        "last_hr_timestamp_used": None,
        "gaps_over_30s": 0,
        "preserved_existing_hr": 0,
    }


def _report(total_apple: int, total_fit: int, matched: int, written: int, preserved: int, used_hr_times: list[datetime]) -> dict:
    gap_count = sum(1 for a, b in zip(used_hr_times, used_hr_times[1:]) if (b - a).total_seconds() > 30)
    coverage = round((written / total_fit * 100.0), 2) if total_fit else 0.0
    return {
        "total_apple_hr_points": total_apple,
        "total_fit_records": total_fit,
        "hr_points_matched": matched,
        "hr_points_written": written,
        "coverage_pct": coverage,
        "first_hr_timestamp_used": used_hr_times[0].isoformat() if used_hr_times else None,
        "last_hr_timestamp_used": used_hr_times[-1].isoformat() if used_hr_times else None,
        "gaps_over_30s": gap_count,
        "preserved_existing_hr": preserved,
    }
