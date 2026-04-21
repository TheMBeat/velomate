"""Service layer around hr_fit_merge core."""

from __future__ import annotations

from hr_fit_merge import (
    FitHrMergeError,
    MergeOptions,
    merge_fit_hr_payload,
    preview_fit_hr_merge,
)


def parse_merge_options(payload: dict) -> MergeOptions:
    if not isinstance(payload, dict):
        raise ValueError("Merge options payload must be an object")

    return MergeOptions(
        overwrite_existing_hr=bool(payload.get("overwrite_existing_hr", False)),
        ignore_implausible_hr=bool(payload.get("ignore_implausible_hr", True)),
        min_hr=int(payload.get("min_hr", 30)),
        max_hr=int(payload.get("max_hr", 240)),
    )


def preview_merge(
    fit_filename: str,
    fit_content: bytes,
    apple_content: bytes,
    apple_source_type: str,
) -> tuple[dict, dict]:
    if not fit_content:
        raise FitHrMergeError("FIT file is empty")
    if not apple_content:
        raise FitHrMergeError("Apple export file is empty")

    return preview_fit_hr_merge(
        fit_filename=fit_filename,
        fit_content=fit_content,
        apple_content=apple_content,
        apple_source_type=apple_source_type,
    )


def run_merge(payload: dict, options: MergeOptions) -> tuple[str, bytes, dict]:
    if not isinstance(payload, dict):
        raise FitHrMergeError("Merge payload missing or invalid")
    return merge_fit_hr_payload(payload, options)
