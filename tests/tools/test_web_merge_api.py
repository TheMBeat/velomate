import base64
import importlib

import pytest


flask_spec = importlib.util.find_spec("flask")
pytestmark = pytest.mark.skipif(flask_spec is None, reason="flask not installed")

from velomate.tools.web import create_app  # noqa: E402


def test_preview_missing_files_returns_400():
    app = create_app()
    client = app.test_client()
    res = client.post("/api/tools/fit-hr-merge/preview", data={})
    assert res.status_code == 400


def test_run_missing_payload_returns_400():
    app = create_app()
    client = app.test_client()
    res = client.post("/api/tools/fit-hr-merge/run", json={})
    assert res.status_code == 400


def test_run_handles_fit_io_error(monkeypatch):
    app = create_app()
    client = app.test_client()

    from velomate.tools import web as web_mod

    def boom(_):
        raise web_mod.FitIoError("FIT support requires optional dependency 'fit-tool'.")

    monkeypatch.setattr(web_mod, "parse_fit_records", boom)
    payload = {
        "fit_b64": base64.b64encode(b"fit").decode("ascii"),
        "hr_b64": base64.b64encode(b"[]").decode("ascii"),
    }
    res = client.post("/api/tools/fit-hr-merge/run", json=payload)
    assert res.status_code == 400


def test_main_debug_disabled_by_default(monkeypatch):
    from velomate.tools import web as web_mod

    called = {}

    class _FakeApp:
        def run(self, **kwargs):
            called.update(kwargs)

    monkeypatch.setattr(web_mod, "create_app", lambda: _FakeApp())
    monkeypatch.delenv("VELOMATE_HR_MERGE_DEBUG", raising=False)
    web_mod.main()
    assert called.get("debug") is False


def test_main_debug_opt_in(monkeypatch):
    from velomate.tools import web as web_mod

    called = {}

    class _FakeApp:
        def run(self, **kwargs):
            called.update(kwargs)

    monkeypatch.setattr(web_mod, "create_app", lambda: _FakeApp())
    monkeypatch.setenv("VELOMATE_HR_MERGE_DEBUG", "1")
    web_mod.main()
    assert called.get("debug") is True
