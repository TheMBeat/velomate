"""HR sample matching strategies for FIT record enrichment."""

from __future__ import annotations

from datetime import datetime


def nearest_neighbor(target: datetime, candidates: list[dict], tolerance_seconds: int) -> dict | None:
    """Return nearest Apple HR sample within tolerance, else None."""
    best = None
    best_delta = None
    for sample in candidates:
        delta = abs((sample["timestamp"] - target).total_seconds())
        if delta <= tolerance_seconds and (best_delta is None or delta < best_delta):
            best = sample
            best_delta = delta
    return best


MATCHING_STRATEGIES = {
    "nearest": nearest_neighbor,
}
