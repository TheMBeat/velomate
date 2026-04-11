from __future__ import annotations

import os

from velomate.tools.fit_io import FitIoError
from velomate.tools.merger.service import build_preview, execute_merge


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
        if not fit_file or not hr_file:
            return jsonify({"error": "Both FIT and Apple HR files are required."}), 400

        try:
            data = build_preview(
                fit_raw=fit_file.read(),
                hr_raw=hr_file.read(),
                source_type=request.form.get("source_type", "auto"),
            )
            return jsonify(data)
        except (FitIoError, ValueError) as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            return jsonify({"error": f"Failed to parse input files: {exc}"}), 400

    @app.post("/api/tools/fit-hr-merge/run")
    def run_merge():
        payload = request.get_json(silent=True) or {}
        if not payload.get("fit_b64") or not payload.get("hr_b64"):
            return jsonify({"error": "Missing preview payload. Please run preview first."}), 400
        try:
            return jsonify(execute_merge(payload))
        except (FitIoError, ValueError, NotImplementedError) as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            return jsonify({"error": f"Merge failed: {exc}"}), 400

    return app


def main() -> None:  # pragma: no cover
    create_app().run(debug=True, port=8080)


if __name__ == "__main__":  # pragma: no cover
    main()
