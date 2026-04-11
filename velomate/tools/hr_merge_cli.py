from __future__ import annotations

import argparse
import json
from pathlib import Path

from velomate.tools.apple_hr import parse_apple_hr
from velomate.tools.fit_io import parse_fit_records, write_fit_with_hr
from velomate.tools.hr_merge_engine import MergeOptions, merge_fit_with_hr


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Merge Apple Health HR export into a FIT activity file.")
    p.add_argument("--fit", required=True, help="Source FIT file path")
    p.add_argument("--apple-hr", required=True, help="Apple HR export file (.json/.csv)")
    p.add_argument("--source-type", default="auto", choices=["auto", "json", "csv"])
    p.add_argument("--tolerance", type=int, default=2, help="Nearest-neighbor tolerance in seconds")
    p.add_argument("--overwrite-existing-hr", action="store_true")
    p.add_argument("--allow-implausible", action="store_true", help="Disable plausibility filter")
    p.add_argument("--min-hr", type=int, default=30)
    p.add_argument("--max-hr", type=int, default=240)
    p.add_argument("--output", default="ride_merged_hr.fit")
    return p


def main() -> None:
    args = build_parser().parse_args()

    fit_raw = Path(args.fit).read_bytes()
    hr_raw = Path(args.apple_hr).read_bytes()

    fit_records, fit_obj = parse_fit_records(fit_raw)
    hr_series = parse_apple_hr(
        hr_raw,
        source_type=args.source_type,
        ignore_implausible=not args.allow_implausible,
        min_hr=args.min_hr,
        max_hr=args.max_hr,
    )

    merged, report = merge_fit_with_hr(
        fit_records,
        hr_series,
        MergeOptions(tolerance_seconds=args.tolerance, overwrite_existing=args.overwrite_existing_hr),
    )
    out = write_fit_with_hr(fit_obj, merged)
    Path(args.output).write_bytes(out)

    print(json.dumps(report, indent=2))
    print(f"Wrote merged FIT: {args.output}")


if __name__ == "__main__":
    main()
