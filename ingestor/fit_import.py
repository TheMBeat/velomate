"""FIT file parsing and mapping to VeloMate activity/stream models."""

from __future__ import annotations

import hashlib
from datetime import timezone
from io import BytesIO

from fitparse import FitFile


class FitImportError(ValueError):
    """Raised when a FIT file cannot be parsed into activity data."""


def _semicircles_to_degrees(value):
    if value is None:
        return None
    return value * (180.0 / (2 ** 31))


def _avg(values: list[float | int]) -> int | None:
    if not values:
        return None
    return int(round(sum(values) / len(values)))


def parse_fit_bytes(file_bytes: bytes, filename: str = "upload.fit") -> dict:
    """Parse FIT bytes and return preview + DB-ready activity/streams payloads."""
    if not file_bytes:
        raise FitImportError("Empty file")

    digest = hashlib.sha256(file_bytes).hexdigest()

    try:
        fit = FitFile(BytesIO(file_bytes))
        fit.parse()
    except Exception as exc:  # fitparse raises multiple concrete errors
        raise FitImportError("Could not parse FIT file") from exc

    records = []
    for msg in fit.get_messages("record"):
        fields = {field.name: field.value for field in msg}
        ts = fields.get("timestamp")
        if ts is None:
            continue

        records.append(
            {
                "timestamp": ts.astimezone(timezone.utc),
                "distance_m": fields.get("distance"),
                "speed_mps": fields.get("speed"),
                "power": fields.get("power"),
                "cadence": fields.get("cadence"),
                "hr": fields.get("heart_rate"),
                "altitude_m": fields.get("altitude"),
                "lat": _semicircles_to_degrees(fields.get("position_lat")),
                "lng": _semicircles_to_degrees(fields.get("position_long")),
            }
        )

    if not records:
        raise FitImportError("No FIT record samples found")

    records.sort(key=lambda r: r["timestamp"])
    start = records[0]["timestamp"]
    end = records[-1]["timestamp"]
    duration_s = max(int((end - start).total_seconds()), 0)

    streams = []
    power_values, hr_values, cadence_values = [], [], []
    max_distance = 0.0
    has_gps = False
    has_speed = False

    for rec in records:
        offset = max(int((rec["timestamp"] - start).total_seconds()), 0)
        speed_mps = rec["speed_mps"]
        speed_kmh = round(speed_mps * 3.6, 2) if speed_mps is not None else None
        has_speed = has_speed or speed_kmh is not None
        has_gps = has_gps or (rec["lat"] is not None and rec["lng"] is not None)

        if rec["power"] is not None:
            power_values.append(rec["power"])
        if rec["hr"] is not None:
            hr_values.append(rec["hr"])
        if rec["cadence"] is not None:
            cadence_values.append(rec["cadence"])
        if rec["distance_m"] is not None:
            max_distance = max(max_distance, float(rec["distance_m"]))

        streams.append(
            {
                "time_offset": offset,
                "hr": rec["hr"],
                "power": rec["power"],
                "cadence": rec["cadence"],
                "speed_kmh": speed_kmh,
                "altitude_m": rec["altitude_m"],
                "lat": rec["lat"],
                "lng": rec["lng"],
            }
        )

    activity = {
        "strava_id": None,
        "name": filename,
        "date": start.isoformat(),
        "distance_m": max_distance,
        "duration_s": duration_s,
        "elevation_m": 0.0,
        "avg_hr": _avg(hr_values),
        "max_hr": max(hr_values) if hr_values else None,
        "avg_power": _avg(power_values),
        "max_power": max(power_values) if power_values else None,
        "avg_cadence": _avg(cadence_values),
        "avg_speed_kmh": round((max_distance / duration_s) * 3.6, 2) if duration_s and max_distance else 0.0,
        "calories": None,
        "suffer_score": None,
        "device": "fit_upload",
        "strava_type": "Ride",
        "trainer": False,
        "source_system": "fit_upload",
        "source_external_id": digest,
        "source_file_name": filename,
    }

    preview = {
        "start_time": start.isoformat(),
        "end_time": end.isoformat(),
        "duration_s": duration_s,
        "distance_m": round(max_distance, 2),
        "has_gps_track": has_gps,
        "has_speed": has_speed,
        "has_cadence": bool(cadence_values),
        "has_power": bool(power_values),
        "has_heart_rate": bool(hr_values),
        "sample_count": len(streams),
        "source_file_name": filename,
    }

    return {"preview": preview, "activity": activity, "streams": streams}


def import_fit_payload(conn, parsed: dict, *, run_fitness_recalc: bool = True) -> tuple[int, int]:
    """Persist a parsed FIT payload and return (activity_id, sample_count)."""
    from db import upsert_activity, upsert_streams
    from fitness import recalculate_fitness

    activity_id, streams_preserved = upsert_activity(conn, parsed["activity"])
    if not streams_preserved:
        upsert_streams(conn, activity_id, parsed["streams"])
    if run_fitness_recalc:
        recalculate_fitness(conn)
    return activity_id, len(parsed["streams"])
