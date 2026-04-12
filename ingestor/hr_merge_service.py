"""Apple HR + FIT merge use-cases and request option parsing."""

from __future__ import annotations

from hr_fit_merge import (
    FitHrMergeError,
    MergeOptions,
    merge_fit_with_hr,
    parse_apple_hr_payload,
    parse_fit_records_for_merge,
    render_merged_output_fit,
)


def parse_bool_flag(value, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"true", "1", "yes", "on"}:
            return True
        if v in {"false", "0", "no", "off"}:
            return False
    raise ValueError("Boolean flag must be true/false")


def parse_merge_options(payload: dict) -> MergeOptions:
    return MergeOptions(
        tolerance_seconds=int(payload.get("tolerance_seconds", 2)),
        overwrite_existing_hr=parse_bool_flag(payload.get("overwrite_existing_hr", False), default=False),
        ignore_implausible_hr=parse_bool_flag(payload.get("ignore_implausible_hr", True), default=True),
        min_hr=int(payload.get("min_hr", 30)),
        max_hr=int(payload.get("max_hr", 240)),
    )


def preview_merge(fit_filename: str, fit_content: bytes, apple_content: bytes, apple_source_type: str) -> dict:
    if not fit_filename.lower().endswith(".fit"):
        raise FitHrMergeError("FIT input must end with .fit")

    fit_payload = parse_fit_records_for_merge(fit_content)
    apple_raw = parse_apple_hr_payload(apple_content, source_type=apple_source_type)

    fit_start = fit_payload["summary"]["start_time"]
    fit_end = fit_payload["summary"]["end_time"]
    overlap_count = sum(1 for row in apple_raw if fit_start <= row.get("timestamp", "") <= fit_end)

    return {
        "fit_filename": fit_filename,
        "fit_bytes": fit_content,
        "fit_records": fit_payload["records"],
        "apple_raw": apple_raw,
    }, {
        "fit_summary": fit_payload["summary"],
        "apple_summary": {
            "point_count": len(apple_raw),
            "first_timestamp": apple_raw[0]["timestamp"] if apple_raw else None,
            "last_timestamp": apple_raw[-1]["timestamp"] if apple_raw else None,
        },
        "estimated_overlap_points": overlap_count,
        "warnings": [] if overlap_count else ["Low overlap between Apple HR and FIT timeline; verify timezone/export range."],
    }


def run_merge(payload: dict, options: MergeOptions) -> tuple[str, bytes, dict]:
    merged_records, report = merge_fit_with_hr(payload["fit_records"], payload["apple_raw"], options)
    return render_merged_output_fit(payload["fit_filename"], payload["fit_bytes"], merged_records, report)
