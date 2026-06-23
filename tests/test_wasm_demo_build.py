from __future__ import annotations

import builtins
import importlib
import os
import sqlite3
import sys

import httpx

from scripts.build_wasm_demo import build_wasm_demo
from scripts.seed_managers import SEED_MANAGERS
from scripts.seed_readiness_data import READINESS_DOC_FILENAME


def test_build_wasm_demo_creates_synthetic_sqlite_bundle(tmp_path):
    db_path = build_wasm_demo(tmp_path / "web")

    assert db_path.exists()
    assert db_path.stat().st_size > 0

    with sqlite3.connect(db_path) as conn:
        managers = conn.execute("SELECT name FROM managers ORDER BY manager_id").fetchall()
        documents = conn.execute("SELECT filename FROM documents").fetchall()

    assert [row[0] for row in managers] == [manager["name"] for manager in SEED_MANAGERS]
    assert (READINESS_DOC_FILENAME,) in documents


def test_deterministic_pages_render_offline(monkeypatch, tmp_path):
    db_path = build_wasm_demo(tmp_path / "web")
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("MINIO_ENDPOINT", raising=False)
    monkeypatch.setenv("UI_OFFLINE", "1")
    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")

    from ui import daily_report, dashboard, search, upload

    managers = dashboard.load_managers()
    assert not managers.empty
    manager_id = int(managers.iloc[0]["manager_id"])
    assert not dashboard.load_latest_holdings_snapshot(manager_id).empty
    assert not dashboard.load_top_deltas(manager_id).empty
    assert not daily_report.load_diffs("2026-03-15").empty
    assert not search.search_news("readiness").empty
    assert upload._load_managers()


def test_dashboard_offline_alert_count_skips_http(monkeypatch):
    from ui import dashboard

    dashboard.load_unacknowledged_alert_count.clear()
    monkeypatch.setenv("UI_OFFLINE", "1")

    def fail_client(*args, **kwargs):
        raise AssertionError("httpx.Client should not be constructed in offline mode")

    monkeypatch.setattr(httpx, "Client", fail_client)
    assert dashboard.load_unacknowledged_alert_count() == 0


def test_wasm_demo_excludes_research_page(monkeypatch, tmp_path):
    db_path = build_wasm_demo(tmp_path / "web")
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("UI_OFFLINE", "1")
    monkeypatch.delenv("DB_URL", raising=False)

    from web.wasm_app import OFFLINE_PAGE_TITLES, _build_offline_pages

    assert len(_build_offline_pages()) == len(OFFLINE_PAGE_TITLES)
    assert OFFLINE_PAGE_TITLES == ["Dashboard", "Daily Report", "Search", "Upload"]
    assert "Research" not in " ".join(OFFLINE_PAGE_TITLES)


def test_wasm_app_main_sets_offline_environment(monkeypatch, tmp_path):
    db_path = build_wasm_demo(tmp_path / "web")
    monkeypatch.chdir(db_path.parent)
    monkeypatch.delenv("DB_PATH", raising=False)
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.delenv("UI_OFFLINE", raising=False)
    monkeypatch.delenv("USE_SIMPLE_EMBED", raising=False)

    from web import wasm_app

    class FakeNavigation:
        def __init__(self):
            self.ran = False

        def run(self):
            self.ran = True

    captured = {}

    def fake_navigation(pages, *, position):
        captured["pages"] = pages
        captured["position"] = position
        captured["navigation"] = FakeNavigation()
        return captured["navigation"]

    monkeypatch.setattr(wasm_app.st, "navigation", fake_navigation)
    wasm_app.main()

    assert captured["position"] == "sidebar"
    assert len(captured["pages"]) == len(wasm_app.OFFLINE_PAGE_TITLES)
    assert captured["navigation"].ran is True
    assert os.environ["DB_PATH"].endswith("manager_demo.sqlite")
    assert os.environ["UI_OFFLINE"] == "1"
    assert os.environ["USE_SIMPLE_EMBED"] == "1"
    assert "DB_URL" not in os.environ
    for name in ("DB_PATH", "UI_OFFLINE", "USE_SIMPLE_EMBED"):
        os.environ.pop(name, None)


def test_signals_offline_fallback_sets_ui_offline(monkeypatch):
    monkeypatch.delenv("UI_OFFLINE", raising=False)
    original_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name in {"fastapi", "pydantic"}:
            raise ModuleNotFoundError(name)
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    sys.modules.pop("api.signals", None)
    try:
        signals = importlib.import_module("api.signals")

        assert os.environ["UI_OFFLINE"] == "1"
        assert signals.APIRouter.__name__ == "OfflineAPIRouter"
    finally:
        sys.modules.pop("api.signals", None)
