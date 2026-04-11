from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class HrPoint:
    timestamp: datetime
    hr: int


@dataclass(frozen=True)
class FitRecord:
    timestamp: datetime
    heart_rate: int | None = None
