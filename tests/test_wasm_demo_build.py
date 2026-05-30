from __future__ import annotations

import sqlite3

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
