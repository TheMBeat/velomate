from __future__ import annotations

import argparse
import base64
import json
from pathlib import Path

from velomate.tools.merger.service import execute_merge


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Merge Apple Health HR export into a FIT activity file.")
    p.add_argument("--fit", required=True, help="Source FIT file path")
    p.add_argument("--apple-hr", required=True, help="Apple HR export file (.json/.csv)")
    p.add_argument("--source-type", default="auto", choices=["auto", "json", "csv"])
    p.add_argument("--tolerance", type=int, default=2)
    p.add_argument("--overwrite-existing-hr", action="store_true")
    p.add_argument("--allow-implausible", action="store_true")
    p.add_argument("--min-hr", type=int, default=30)
    p.add_argument("--max-hr", type=int, default=240)
    p.add_argument("--strategy", default="nearest", choices=["nearest"])
    p.add_argument("--output", default="ride_merged_hr.fit")
    return p


def main() -> None:
    args = build_parser().parse_args()
    payload = {
        "fit_b64": base64.b64encode(Path(args.fit).read_bytes()).decode("ascii"),
        "hr_b64": base64.b64encode(Path(args.apple_hr).read_bytes()).decode("ascii"),
        "source_type": args.source_type,
        "tolerance_seconds": args.tolerance,
        "overwrite_existing": args.overwrite_existing_hr,
        "ignore_implausible": not args.allow_implausible,
        "min_hr": args.min_hr,
        "max_hr": args.max_hr,
        "strategy": args.strategy,
        "output_name": args.output,
    }
    result = execute_merge(payload)
    Path(result["filename"]).write_bytes(base64.b64decode(result["merged_fit_b64"]))
    print(json.dumps(result["report"], indent=2))
    print(f"Wrote merged FIT: {result['filename']}")


if __name__ == "__main__":
    main()
