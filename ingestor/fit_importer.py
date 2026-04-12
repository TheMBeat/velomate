"""FIT importer that maps FIT records to the internal activity/sample model."""

from __future__ import annotations

from datetime import timezone
from pathlib import Path

from activity_model import ActivitySample, ImportedActivity


class FitImportError(RuntimeError):
    pass


def _load_fit_tool():
    try:
        from fit_tool.fit_file import FitFile  # type: ignore
    except Exception as exc:  # pragma: no cover - env dependent
        raise FitImportError("FIT import requires optional dependency 'fit-tool'.") from exc
    return FitFile


def parse_fit_to_activity(raw: bytes, file_name: str = "") -> ImportedActivity:
    fit_file = _load_fit_tool().from_bytes(raw)

    records = []
    for msg in fit_file.messages:
        if msg.name != "record":
            continue
        values = {f.name: f.value for f in msg.fields}
        ts = values.get("timestamp")
        if ts is None:
            continue
        records.append(values)

    if not records:
        raise FitImportError("No FIT record samples found.")

    samples: list[ActivitySample] = []
    start_ts = records[0]["timestamp"].astimezone(timezone.utc)
    for rec in records:
        ts = rec["timestamp"].astimezone(timezone.utc)
        offset = int((ts - start_ts).total_seconds())
        latlng = rec.get("position_lat")
        lng = rec.get("position_long")
        samples.append(
            ActivitySample(
                timestamp=ts,
                time_offset=offset,
                hr=_to_int(rec.get("heart_rate")),
                power=_to_int(rec.get("power")),
                cadence=_to_int(rec.get("cadence")),
                speed_kmh=_to_speed_kmh(rec.get("speed")),
                altitude_m=_to_float(rec.get("altitude")),
                lat=_to_float(latlng),
                lng=_to_float(lng),
            )
        )

    end_ts = samples[-1].timestamp
    duration = max(0, int((end_ts - start_ts).total_seconds()))

    hr_values = [s.hr for s in samples if s.hr is not None]
    power_values = [s.power for s in samples if s.power is not None]
    cadence_values = [s.cadence for s in samples if s.cadence is not None]
    speed_values = [s.speed_kmh for s in samples if s.speed_kmh is not None]
    alt_values = [s.altitude_m for s in samples if s.altitude_m is not None]

    distance_m = _to_float(records[-1].get("distance")) or 0.0
    elevation_m = 0.0
    if alt_values:
        elevation_m = max(0.0, max(alt_values) - min(alt_values))

    name = Path(file_name).stem if file_name else "FIT Import"
    return ImportedActivity(
        name=name,
        date=start_ts,
        duration_s=duration,
        distance_m=distance_m,
        elevation_m=elevation_m,
        avg_hr=_avg(hr_values),
        max_hr=max(hr_values) if hr_values else None,
        avg_power=_avg(power_values),
        max_power=max(power_values) if power_values else None,
        avg_cadence=_avg(cadence_values),
        avg_speed_kmh=round(sum(speed_values) / len(speed_values), 2) if speed_values else None,
        calories=None,
        device="fit_import",
        source="fit",
        source_id=None,
        strava_type="Ride",
        trainer=False,
        samples=samples,
    )


def to_db_activity_payload(activity: ImportedActivity) -> dict:
    return {
        "strava_id": None,
        "name": activity.name,
        "date": activity.date.isoformat(),
        "distance_m": activity.distance_m,
        "duration_s": activity.duration_s,
        "elevation_m": activity.elevation_m,
        "avg_hr": activity.avg_hr,
        "max_hr": activity.max_hr,
        "avg_power": activity.avg_power,
        "max_power": activity.max_power,
        "avg_cadence": activity.avg_cadence,
        "avg_speed_kmh": activity.avg_speed_kmh,
        "calories": activity.calories,
        "suffer_score": None,
        "device": activity.device,
        "strava_type": activity.strava_type,
        "trainer": activity.trainer,
    }


def to_db_streams(samples: list[ActivitySample]) -> list[dict]:
    return [
        {
            "time_offset": s.time_offset,
            "hr": s.hr,
            "power": s.power,
            "cadence": s.cadence,
            "speed_kmh": s.speed_kmh,
            "altitude_m": s.altitude_m,
            "lat": s.lat,
            "lng": s.lng,
        }
        for s in samples
    ]


def _avg(values: list[int]) -> int | None:
    if not values:
        return None
    return int(round(sum(values) / len(values)))


def _to_int(v):
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _to_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_speed_kmh(v):
    as_float = _to_float(v)
    if as_float is None:
        return None
    return round(as_float * 3.6, 2)
