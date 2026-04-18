"""Apple HR + FIT merge use-cases and request option parsing."""

from __future__ import annotations

from hr_fit_merge import (
    FitHrMergeError,
    MergeOptions,
    merge_fit_with_hr,
    parse_apple_hr_payload_details,
    parse_fit_records_for_merge,
    render_merged_output_fit,
)

def _build_apple_debug_response(apple_parsed: dict) -> dict:
    debug = dict(apple_parsed.get("debug", {}))
    samples = list(apple_parsed.get("samples", []))
    debug["detected_source_type"] = apple_parsed.get("source_type")
    debug["sample_preview"] = samples[:5]
    return debug


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
        matching_strategy=str(payload.get("matching_strategy", "linear")),
    )


def preview_merge(fit_filename: str, fit_content: bytes, apple_content: bytes, apple_source_type: str) -> dict:
    if not fit_filename.lower().endswith(".fit"):
        raise FitHrMergeError("FIT input must end with .fit")

    fit_payload = parse_fit_records_for_merge(fit_content)
    apple_parsed = parse_apple_hr_payload_details(
        apple_content,
        source_type=apple_source_type,
        fit_start_time=fit_payload["summary"]["start_time"],
        fit_end_time=fit_payload["summary"]["end_time"],
    )
    apple_raw = apple_parsed["samples"]
    apple_debug = _build_apple_debug_response(apple_parsed)

    fit_start = fit_payload["summary"]["start_time"]
    fit_end = fit_payload["summary"]["end_time"]
    overlap_count = sum(1 for row in apple_raw if fit_start <= row.get("timestamp", "") <= fit_end)
    print(
        "[fit_hr_merge.preview] Apple parse: "
        f"detected_source_type={apple_debug.get('detected_source_type')}, "
        f"parser_mode={apple_debug.get('parser_mode')}, "
        f"workouts_found={apple_debug.get('workouts_found', 0)}, "
        f"selected_has_heartRateData={apple_debug.get('selected_workout_has_heart_rate_data', False)}, "
        f"selected_index={apple_debug.get('selected_workout_index')}, "
        f"selected_id={apple_debug.get('selected_workout_id')}, "
        f"selected_has_heartRateData={apple_debug.get('selected_workout_has_heart_rate_data', False)}, "
        f"raw_entries={apple_debug.get('raw_heart_rate_entries_found', 0)}, "
        f"parsed_entries={apple_debug.get('parsed_heart_rate_entries_count', len(apple_raw))}, "
        f"rejected_entries={apple_debug.get('rejected_entries_count', 0)}, "
        f"extracted_hr_points={len(apple_raw)}, overlap_points={overlap_count}"
    )
    if apple_debug.get("detected_source_type") != apple_debug.get("parser_mode"):
        print(
            "[fit_hr_merge.preview] WARNING: detected_source_type does not match parser_mode "
            f"({apple_debug.get('detected_source_type')} vs {apple_debug.get('parser_mode')})"
        )

    return {
        "fit_filename": fit_filename,
        "fit_bytes": fit_content,
        "fit_records": fit_payload["records"],
        "apple_raw": apple_raw,
        "apple_debug": apple_debug,
    }, {
        "fit_summary": fit_payload["summary"],
        "apple_debug": apple_debug,
        "apple_summary": {
            "point_count": len(apple_raw),
            "first_timestamp": apple_raw[0]["timestamp"] if apple_raw else None,
            "last_timestamp": apple_raw[-1]["timestamp"] if apple_raw else None,
        },
        "estimated_overlap_points": overlap_count,
        "warnings": _preview_warnings(point_count=len(apple_raw), overlap_count=overlap_count, fit_count=fit_payload["summary"]["sample_count"]),
    }


def _preview_warnings(*, point_count: int, overlap_count: int, fit_count: int) -> list[str]:
    if point_count == 0:
        return ["No Apple HR points extracted"]
    if overlap_count == 0:
        return ["No overlap with FIT timeline"]
    if overlap_count < fit_count:
        return ["Partial HR coverage"]
    return []


def run_merge(payload: dict, options: MergeOptions) -> tuple[str, bytes, dict]:
    merged_records, report = merge_fit_with_hr(payload["fit_records"], payload["apple_raw"], options)
    return render_merged_output_fit(payload["fit_filename"], payload["fit_bytes"], merged_records, report)
