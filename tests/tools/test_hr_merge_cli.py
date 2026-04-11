import base64
from pathlib import Path

from velomate.tools import hr_merge_cli


def test_cli_delegates_to_service(monkeypatch, tmp_path: Path):
    fit_path = tmp_path / "ride.fit"
    hr_path = tmp_path / "apple.json"
    out_path = tmp_path / "out.fit"
    fit_path.write_bytes(b"fit")
    hr_path.write_bytes(b"[]")

    seen = {"called": False}

    def fake_execute(payload):
        seen["called"] = True
        assert payload["strategy"] == "nearest"
        return {
            "filename": str(out_path),
            "merged_fit_b64": base64.b64encode(b"merged").decode("ascii"),
            "report": {"ok": True},
        }

    monkeypatch.setattr(hr_merge_cli, "execute_merge", fake_execute)
    monkeypatch.setattr(
        "sys.argv",
        [
            "velomate-hr-merge",
            "--fit",
            str(fit_path),
            "--apple-hr",
            str(hr_path),
            "--output",
            str(out_path),
        ],
    )

    hr_merge_cli.main()
    assert seen["called"] is True
    assert out_path.read_bytes() == b"merged"
