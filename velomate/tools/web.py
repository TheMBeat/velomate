from __future__ import annotations

import base64
import os

from velomate.tools.apple_hr import parse_apple_hr
from velomate.tools.fit_io import FitIoError, parse_fit_records, write_fit_with_hr
from velomate.tools.hr_merge_engine import MergeOptions, merge_fit_with_hr


def create_app():
    try:
        from flask import Flask, jsonify, render_template, request
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Web UI requires Flask. Install with `pip install flask`.") from exc

    app = Flask(
        __name__,
        template_folder=os.path.join(os.path.dirname(__file__), "templates"),
        static_folder=os.path.join(os.path.dirname(__file__), "static"),
    )

    @app.get("/tools/fit-hr-merge")
    def fit_hr_merge_page():
        return render_template("fit_hr_merge.html")

    @app.post("/api/tools/fit-hr-merge/preview")
    def preview():
        fit_file = request.files.get("fit_file")
        hr_file = request.files.get("apple_hr_file")
        source_type = request.form.get("source_type", "auto")
        if not fit_file or not hr_file:
            return jsonify({"error": "Both FIT and Apple HR files are required."}), 400

        try:
            fit_raw = fit_file.read()
            hr_raw = hr_file.read()
            fit_records, _ = parse_fit_records(fit_raw)
            hr_series = parse_apple_hr(hr_raw, source_type=source_type)
        except FitIoError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            return jsonify({"error": f"Failed to parse input files: {exc}"}), 400

        if not fit_records:
            return jsonify({"error": "FIT file has no record messages."}), 400

        fit_start = fit_records[0].timestamp
        fit_end = fit_records[-1].timestamp
        overlap_points = [p for p in hr_series if fit_start <= p.timestamp <= fit_end]

        warnings = []
        if hr_series and not overlap_points:
            warnings.append("No Apple HR points overlap the FIT activity window. Check timezone/export.")
        elif overlap_points and len(overlap_points) < max(5, len(fit_records) * 0.05):
            warnings.append("Overlap looks weak (<5% of FIT record count).")

        return jsonify({
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
        })

    @app.post("/api/tools/fit-hr-merge/run")
    def run_merge():
        payload = request.get_json(silent=True) or {}
        fit_b64 = payload.get("fit_b64")
        hr_b64 = payload.get("hr_b64")
        if not fit_b64 or not hr_b64:
            return jsonify({"error": "Missing preview payload. Please run preview first."}), 400

        options = MergeOptions(
            tolerance_seconds=int(payload.get("tolerance_seconds", 2)),
            overwrite_existing=bool(payload.get("overwrite_existing", False)),
        )
        ignore_implausible = bool(payload.get("ignore_implausible", True))
        min_hr = int(payload.get("min_hr", 30))
        max_hr = int(payload.get("max_hr", 240))
        source_type = payload.get("source_type", "auto")

        try:
            fit_raw = base64.b64decode(fit_b64)
            hr_raw = base64.b64decode(hr_b64)
            fit_records, fit_obj = parse_fit_records(fit_raw)
            hr_series = parse_apple_hr(hr_raw, source_type=source_type, ignore_implausible=ignore_implausible, min_hr=min_hr, max_hr=max_hr)
            merged_records, report = merge_fit_with_hr(fit_records, hr_series, options)
            merged_bytes = write_fit_with_hr(fit_obj, merged_records)
        except FitIoError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            return jsonify({"error": f"Merge failed: {exc}"}), 400

        filename = payload.get("output_name") or "ride_merged_hr.fit"
        return jsonify({
            "filename": filename,
            "merged_fit_b64": base64.b64encode(merged_bytes).decode("ascii"),
            "report": report,
        })

    return app


def main() -> None:  # pragma: no cover
    create_app().run(debug=True, port=8080)


if __name__ == "__main__":  # pragma: no cover
    main()
