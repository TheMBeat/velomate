from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass
from datetime import datetime

from velomate.tools.hr_models import HrPoint


@dataclass(frozen=True)
class MatchOptions:
    tolerance_seconds: int = 2
    strategy: str = "nearest"


def find_match(target_ts: datetime, hr_points: list[HrPoint], hr_timestamps: list[datetime], options: MatchOptions) -> HrPoint | None:
    if options.strategy == "nearest":
        return _nearest_match(target_ts, hr_points, hr_timestamps, options.tolerance_seconds)
    if options.strategy == "interpolate":  # reserved extension point
        raise NotImplementedError("Interpolation strategy is not implemented in MVP.")
    raise ValueError(f"Unknown match strategy: {options.strategy}")


def _nearest_match(
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
