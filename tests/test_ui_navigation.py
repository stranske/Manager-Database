import importlib
import sqlite3
import sys
from pathlib import Path

from streamlit.testing.v1 import AppTest

sys.path.append(str(Path(__file__).resolve().parents[1]))


def _create_empty_dashboard_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE filings (
            filing_id INTEGER PRIMARY KEY,
            manager_id INTEGER,
            type TEXT,
            filed_date TEXT
        );
        CREATE TABLE holdings (
            filing_id INTEGER,
            manager_id INTEGER,
            filed TEXT
        );
        CREATE TABLE news_items (
            manager_id INTEGER,
            headline TEXT,
            url TEXT,
            published_at TEXT,
            source TEXT,
            topics TEXT,
            confidence REAL
        );
        CREATE TABLE api_usage (ts TEXT);
        """)
    conn.close()


class FakeNavigation:
    def __init__(self) -> None:
        self.run_called = False

    def run(self) -> None:
        self.run_called = True


class FakeStreamlit:
    def __init__(self) -> None:
        self.page_calls: list[dict[str, object]] = []
        self.navigation_calls: list[dict[str, object]] = []
        self.navigation_instance = FakeNavigation()

    def Page(  # noqa: N802 - mirrors streamlit API
        self,
        target,
        title: str,
        icon: str,
        url_path: str,
        default: bool = False,
    ):
        page = {
            "target": target,
            "title": title,
            "icon": icon,
            "url_path": url_path,
            "default": default,
        }
        self.page_calls.append(page)
        return page

    def navigation(self, pages, position: str):
        self.navigation_calls.append({"pages": pages, "position": position})
        return self.navigation_instance


def test_navigation_includes_research_page(monkeypatch):
    app = importlib.reload(importlib.import_module("ui.app"))
    fake_st = FakeStreamlit()
    monkeypatch.setattr(app, "st", fake_st)

    app.main()

    assert fake_st.navigation_calls[0]["position"] == "sidebar"
    url_paths = [page["url_path"] for page in fake_st.page_calls]
    titles = [page["title"] for page in fake_st.page_calls]

    assert "research" in url_paths
    assert "🔬 Research" in titles
    assert fake_st.page_calls[0]["default"] is True
    assert fake_st.navigation_instance.run_called is True


def test_real_streamlit_navigation_constructs_without_exception(tmp_path, monkeypatch):
    db_path = tmp_path / "dashboard.db"
    _create_empty_dashboard_db(db_path)
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(db_path))

    at = AppTest.from_file("ui/app.py").run(timeout=5)

    assert not at.exception
