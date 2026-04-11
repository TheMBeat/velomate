"""Shared internal activity/sample model for imports (Strava, FIT, future enrichments)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ActivitySample:
    """Canonical per-sample record.

    `hr` is optional by design to support sparse HR streams and future Apple HR enrichment.
    """

    timestamp: datetime
    time_offset: int
    hr: int | None = None
    power: int | None = None
    cadence: int | None = None
    speed_kmh: float | None = None
    altitude_m: float | None = None
    lat: float | None = None
    lng: float | None = None


@dataclass(frozen=True)
class ImportedActivity:
    name: str
    date: datetime
    duration_s: int
    distance_m: float
    elevation_m: float
    avg_hr: int | None
    max_hr: int | None
    avg_power: int | None
    max_power: int | None
    avg_cadence: int | None
    avg_speed_kmh: float | None
    calories: int | None
    device: str
    source: str
    source_id: str | None
    strava_type: str | None
    trainer: bool
    samples: list[ActivitySample]
