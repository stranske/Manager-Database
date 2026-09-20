import importlib
import sqlite3
import subprocess
import sys
from pathlib import Path

from streamlit.testing.v1 import AppTest

from ui import ALERTS_URL_PATH
from ui import alerts as alerts_ui

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


def test_alerts_route_is_registered_in_full_shell(monkeypatch):
    app = importlib.reload(importlib.import_module("ui.app"))
    fake_st = FakeStreamlit()
    monkeypatch.setattr(app, "st", fake_st)

    app.main()

    alerts_pages = [page for page in fake_st.page_calls if page["url_path"] == ALERTS_URL_PATH]
    assert len(alerts_pages) == 1
    assert alerts_pages[0]["target"] is alerts_ui.main
    assert alerts_pages[0]["title"] == "Alerts"


def test_alerts_destination_renders_rule_and_inbox_controls_with_synthetic_api(monkeypatch):
    monkeypatch.delenv("UI_USERNAME", raising=False)
    monkeypatch.delenv("UI_PASSWORD", raising=False)
    alerts_ui._clear_alert_caches()

    def fake_api(method, path, *, params=None, json_body=None):
        assert method == "GET"
        if path == "/managers":
            return True, {"items": [], "total": 0}
        if path in {"/api/alerts/rules", "/api/alerts/history"}:
            return True, []
        raise AssertionError(f"Unexpected API path: {path}")

    monkeypatch.setattr(alerts_ui, "_api_request", fake_api)
    at = AppTest.from_string("from ui.alerts import main\nmain()", default_timeout=10).run()

    assert not at.exception
    assert [heading.value for heading in at.subheader] == [
        "Rule Builder",
        "Alert Inbox",
        "Alert Stats",
    ]
    assert any(button.label == "Create Rule" for button in at.button)
    assert any(button.label == "Acknowledge All" for button in at.button)


def test_full_shell_import_resolves_domain_alerts_package_from_script_directory():
    root = Path(__file__).resolve().parents[1]
    script = (
        "import runpy, sys; from pathlib import Path; "
        "sys.path.insert(0, str(Path('ui').resolve())); "
        "runpy.run_path('ui/app.py', run_name='ui_app_import_smoke')"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_real_streamlit_navigation_constructs_without_exception(tmp_path, monkeypatch):
    db_path = tmp_path / "dashboard.db"
    _create_empty_dashboard_db(db_path)
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(db_path))

    at = AppTest.from_file("ui/app.py").run(timeout=5)

    assert not at.exception
