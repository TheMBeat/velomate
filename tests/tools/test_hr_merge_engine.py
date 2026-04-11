from datetime import datetime, timezone

from velomate.tools.hr_merge_engine import MergeOptions, merge_fit_with_hr
from velomate.tools.hr_models import FitRecord, HrPoint


def _dt(s: str):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def test_merge_into_fit_with_no_existing_hr():
    fit = [FitRecord(timestamp=_dt("2026-04-10T12:00:00Z")), FitRecord(timestamp=_dt("2026-04-10T12:00:02Z"))]
    hr = [HrPoint(timestamp=_dt("2026-04-10T12:00:00Z"), hr=140), HrPoint(timestamp=_dt("2026-04-10T12:00:02Z"), hr=141)]
    merged, report = merge_fit_with_hr(fit, hr, MergeOptions())
    assert merged[0].heart_rate == 140
    assert report["hr_points_written"] == 2


def test_existing_hr_not_overwritten_when_disabled():
    fit = [FitRecord(timestamp=_dt("2026-04-10T12:00:00Z"), heart_rate=100)]
    hr = [HrPoint(timestamp=_dt("2026-04-10T12:00:00Z"), hr=150)]
    merged, report = merge_fit_with_hr(fit, hr, MergeOptions(overwrite_existing=False))
    assert merged[0].heart_rate == 100
    assert report["preserved_existing_hr"] == 1


def test_existing_hr_overwritten_when_enabled():
    fit = [FitRecord(timestamp=_dt("2026-04-10T12:00:00Z"), heart_rate=100)]
    hr = [HrPoint(timestamp=_dt("2026-04-10T12:00:00Z"), hr=150)]
    merged, report = merge_fit_with_hr(fit, hr, MergeOptions(overwrite_existing=True))
    assert merged[0].heart_rate == 150
    assert report["hr_points_written"] == 1


def test_out_of_range_timestamps_not_merged():
    fit = [FitRecord(timestamp=_dt("2026-04-10T12:00:00Z"))]
    hr = [HrPoint(timestamp=_dt("2026-04-10T11:00:00Z"), hr=150)]
    merged, report = merge_fit_with_hr(fit, hr, MergeOptions())
    assert merged[0].heart_rate is None
    assert report["hr_points_matched"] == 0


def test_report_generation_fields_present():
    fit = [FitRecord(timestamp=_dt("2026-04-10T12:00:00Z"))]
    hr = [HrPoint(timestamp=_dt("2026-04-10T12:00:00Z"), hr=150)]
    _, report = merge_fit_with_hr(fit, hr, MergeOptions())
    assert set(["coverage_pct", "first_hr_timestamp_used", "last_hr_timestamp_used"]).issubset(report.keys())


def test_interpolation_strategy_reserved():
    fit = [FitRecord(timestamp=_dt("2026-04-10T12:00:00Z"))]
    hr = [HrPoint(timestamp=_dt("2026-04-10T12:00:00Z"), hr=150)]
    import pytest
    with pytest.raises(NotImplementedError):
        merge_fit_with_hr(fit, hr, MergeOptions(strategy="interpolate"))
