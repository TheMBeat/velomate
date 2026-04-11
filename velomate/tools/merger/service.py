from __future__ import annotations

import base64

from velomate.tools.fit_io import parse_fit_records, write_fit_with_hr
from velomate.tools.merger.apple_parser import parse_apple_hr
from velomate.tools.merger.core import MergeOptions, merge_fit_with_hr


def build_preview(fit_raw: bytes, hr_raw: bytes, source_type: str = "auto") -> dict:
    fit_records, _ = parse_fit_records(fit_raw)
    hr_series = parse_apple_hr(hr_raw, source_type=source_type)
    if not fit_records:
        raise ValueError("FIT file has no record messages.")

    fit_start = fit_records[0].timestamp
    fit_end = fit_records[-1].timestamp
    overlap_points = [p for p in hr_series if fit_start <= p.timestamp <= fit_end]

    warnings = []
    if hr_series and not overlap_points:
        warnings.append("No Apple HR points overlap the FIT activity window. Check timezone/export.")
    elif overlap_points and len(overlap_points) < max(5, len(fit_records) * 0.05):
        warnings.append("Overlap looks weak (<5% of FIT record count).")

    return {
        "fit_summary": {
            "start": fit_start.isoformat(),
            "end": fit_end.isoformat(),
            "duration": str(fit_end - fit_start),
            "sample_count": len(fit_records),
            "existing_hr": any(r.heart_rate is not None for r in fit_records),
        },
        "apple_summary": {
            "point_count": len(hr_series),
            "first_timestamp": hr_series[0].timestamp.isoformat() if hr_series else None,
            "last_timestamp": hr_series[-1].timestamp.isoformat() if hr_series else None,
        },
        "estimated_overlap_points": len(overlap_points),
        "warnings": warnings,
        "fit_b64": base64.b64encode(fit_raw).decode("ascii"),
        "hr_b64": base64.b64encode(hr_raw).decode("ascii"),
    }


def execute_merge(payload: dict) -> dict:
    fit_raw = base64.b64decode(payload["fit_b64"])
    hr_raw = base64.b64decode(payload["hr_b64"])
    source_type = payload.get("source_type", "auto")

    fit_records, fit_obj = parse_fit_records(fit_raw)
    hr_series = parse_apple_hr(
        hr_raw,
        source_type=source_type,
        ignore_implausible=bool(payload.get("ignore_implausible", True)),
        min_hr=int(payload.get("min_hr", 30)),
        max_hr=int(payload.get("max_hr", 240)),
    )

    options = MergeOptions(
        tolerance_seconds=int(payload.get("tolerance_seconds", 2)),
        overwrite_existing=bool(payload.get("overwrite_existing", False)),
        strategy=payload.get("strategy", "nearest"),
    )
    merged_records, report = merge_fit_with_hr(fit_records, hr_series, options)
    merged_bytes = write_fit_with_hr(fit_obj, merged_records)

    return {
        "filename": payload.get("output_name") or "ride_merged_hr.fit",
        "merged_fit_b64": base64.b64encode(merged_bytes).decode("ascii"),
        "report": report,
    }
