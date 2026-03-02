import sqlite3
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ui import dashboard
from ui.dashboard import load_delta, load_unacknowledged_alert_count, main


def setup_db(tmp_path: Path) -> str:
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE holdings (cik TEXT, accession TEXT, filed DATE, nameOfIssuer TEXT, cusip TEXT, value INTEGER, sshPrnamt INTEGER)"
    )
    rows = [
        ("0", "a", "2024-01-01", "CorpA", "AAA", 1, 1),
        ("0", "b", "2024-01-02", "CorpB", "BBB", 1, 1),
        ("0", "c", "2024-01-02", "CorpC", "CCC", 1, 1),
    ]
    conn.executemany("INSERT INTO holdings VALUES (?,?,?,?,?,?,?)", rows)
    conn.commit()
    conn.close()
    return str(db_path)


def test_load_delta_counts(tmp_path: Path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    df = load_delta()
    assert list(df["date"]) == ["2024-01-01", "2024-01-02"]
    assert list(df["filings"]) == [1, 2]


class _FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _FakeClient:
    def __init__(self, response_payload: dict, requested_urls: list[str]):
        self._response_payload = response_payload
        self._requested_urls = requested_urls

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url: str):
        self._requested_urls.append(url)
        return _FakeResponse(self._response_payload)


def test_load_unacknowledged_alert_count_uses_alerts_endpoint(monkeypatch):
    requested_urls: list[str] = []
    monkeypatch.setenv("ALERTS_API_BASE_URL", "http://alerts.local")
    monkeypatch.setattr(
        dashboard,
        "httpx",
        type(
            "FakeHttpx",
            (),
            {"Client": lambda **kwargs: _FakeClient({"count": 7}, requested_urls)},
        ),
    )
    load_unacknowledged_alert_count.clear()

    count = load_unacknowledged_alert_count()

    assert count == 7
    assert requested_urls == ["http://alerts.local/api/alerts/unacknowledged/count"]


class _FakeSidebar:
    def __init__(self):
        self.markdowns: list[str] = []
        self.metrics: list[tuple[str, int]] = []

    def markdown(self, text: str, unsafe_allow_html: bool = False) -> None:
        self.markdowns.append(text)

    def metric(self, label: str, value: int) -> None:
        self.metrics.append((label, value))


class _FakeStreamlit:
    def __init__(self):
        self.sidebar = _FakeSidebar()
        self.headers: list[str] = []
        self.infos: list[str] = []

    def header(self, text: str) -> None:
        self.headers.append(text)

    def info(self, text: str) -> None:
        self.infos.append(text)


def test_main_renders_alert_badge_and_metric(monkeypatch):
    fake_st = _FakeStreamlit()
    monkeypatch.setattr(dashboard, "st", fake_st)
    monkeypatch.setattr(dashboard, "require_login", lambda: True)
    monkeypatch.setattr(dashboard, "load_unacknowledged_alert_count", lambda: 3)
    monkeypatch.setattr(dashboard, "load_delta", lambda: pd.DataFrame())

    main()

    assert fake_st.sidebar.markdowns[0] == "### Navigation"
    assert "Alerts" in fake_st.sidebar.markdowns[1]
    assert ">3<" in fake_st.sidebar.markdowns[1]
    assert fake_st.sidebar.metrics == [("Unacknowledged Alerts", 3)]
    assert fake_st.headers == ["Holdings Delta"]
    assert fake_st.infos == ["No data available"]
