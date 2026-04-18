"""Tests for merge preview response envelope."""

from pathlib import Path
import sys
from unittest.mock import MagicMock, patch

sys.modules.setdefault("psycopg2", MagicMock())
sys.modules.setdefault("psycopg2.extras", MagicMock())
sys.modules.setdefault("fitparse", MagicMock())

_ingestor_dir = Path(__file__).resolve().parent.parent / "ingestor"
if str(_ingestor_dir) not in sys.path:
    sys.path.insert(0, str(_ingestor_dir))

import hr_merge_service


def test_preview_merge_includes_top_level_apple_debug_with_sample_preview():
    fit_summary = {"start_time": "2026-04-11T07:00:00Z", "end_time": "2026-04-11T08:00:00Z", "sample_count": 10, "has_existing_hr": False}
    with (
        patch("hr_merge_service.parse_fit_records_for_merge", return_value={"records": [], "summary": fit_summary}),
        patch(
            "hr_merge_service.parse_apple_hr_payload_details",
            return_value={
                "source_type": "json",
                "samples": [{"timestamp": f"2026-04-11T07:00:0{i}Z", "hr": 120 + i} for i in range(7)],
                "debug": {"parser_mode": "json", "workouts_found": 1, "selected_workout_id": "wk1"},
            },
        ),
    ):
        payload, response = hr_merge_service.preview_merge("ride.fit", b"fit", b"apple", "auto")

    assert payload["apple_debug"]["detected_source_type"] == "json"
    assert response["apple_debug"]["detected_source_type"] == "json"
    assert response["apple_debug"]["parser_mode"] == "json"
    assert len(response["apple_debug"]["sample_preview"]) == 5
    assert response["apple_summary"]["point_count"] == 7
    assert response["warnings"] == ["Partial HR coverage"]
