from pathlib import Path

from velomate.tools import hr_merge_cli
from velomate.tools.hr_models import FitRecord, HrPoint


def test_cli_uses_shared_merge_engine(monkeypatch, tmp_path: Path):
    fit_path = tmp_path / "ride.fit"
    hr_path = tmp_path / "apple.json"
    out_path = tmp_path / "out.fit"
    fit_path.write_bytes(b"fit")
    hr_path.write_bytes(b"[]")

    monkeypatch.setattr(
        hr_merge_cli,
        "parse_fit_records",
        lambda _: ([FitRecord(timestamp=__import__("datetime").datetime(2026, 1, 1, tzinfo=__import__("datetime").timezone.utc))], object()),
    )
    monkeypatch.setattr(
        hr_merge_cli,
        "parse_apple_hr",
        lambda *_args, **_kwargs: [HrPoint(timestamp=__import__("datetime").datetime(2026, 1, 1, tzinfo=__import__("datetime").timezone.utc), hr=150)],
    )
    monkeypatch.setattr(hr_merge_cli, "write_fit_with_hr", lambda _obj, _merged: b"merged")

    called = {"merge": False}

    def fake_merge(records, hr_series, options):
        called["merge"] = True
        return records, {"ok": True}

    monkeypatch.setattr(hr_merge_cli, "merge_fit_with_hr", fake_merge)
    monkeypatch.setattr("sys.argv", [
        "velomate-hr-merge",
        "--fit", str(fit_path),
        "--apple-hr", str(hr_path),
        "--output", str(out_path),
    ])

    hr_merge_cli.main()
    assert called["merge"] is True
    assert out_path.read_bytes() == b"merged"
