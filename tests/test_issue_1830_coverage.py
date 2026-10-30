"""Focused regression coverage for the ranked #1830 adapter and API gaps."""

from __future__ import annotations

import math
import sqlite3
from datetime import date as real_date
from datetime import datetime
from typing import cast

import httpx
import pytest

from adapters import edgar, mas
from api import _compat, activism
from etl import activism_flow, summariser_flow
from ui import launch


def test_edgar_normalization_helpers_cover_unusual_sec_shapes() -> None:
    """SEC payload variants retain an accession and reject malformed optional values."""
    assert edgar._parse_int_field(3) == 3
    assert edgar._parse_int_field(2.9) == 2
    assert edgar._parse_int_field("") == 0
    assert edgar._extract_accession("not-an-accession") == "not-an-accession"
    assert edgar._extract_efts_hits({"hits": [{"_source": {}}]}) == [{"_source": {}}]
    assert edgar._extract_efts_hits({"filings": [{"form": "SC 13D"}]}) == [{"form": "SC 13D"}]
    assert edgar._json_mapping(["not", "a", "mapping"]) == {}
    assert list(edgar._json_iterable("not records")) == []
    assert edgar._extract_label_value("Issuer: Example Corp", "Issuer") == "Example Corp"
    assert edgar._extract_label_value("unrelated", "Issuer") is None
    assert edgar._parse_int(None) is None
    assert edgar._parse_float(None) is None
    assert edgar._parse_float("not a percentage") is None
    assert edgar._extract_group_members("no named people") == []


@pytest.mark.asyncio
async def test_edgar_download_normalizes_relative_url(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, str] = {}

    class Response:
        text = "payload"

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args: object) -> bool:
            return False

    async def request(client: object, url: str, headers: dict[str, str], *, source: str):
        seen["url"] = url
        return Response()

    monkeypatch.setattr(edgar.httpx, "AsyncClient", Client)
    monkeypatch.setattr(edgar, "_request_with_retry", request)
    assert await edgar.download({"url": "/Archives/example.txt"}) == "payload"
    assert seen["url"] == "https://www.sec.gov/Archives/example.txt"


def test_api_conversion_and_missing_tables_are_safe() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        assert activism.query_activism_filings(conn) == []
        assert activism.query_activism_events(conn) == []
        assert activism.query_activism_timeline(conn, 1) == []
        assert activism.query_activism_campaigns(conn) == []
        assert activism.query_activism_campaign_timeline(conn, 1) == []
        assert activism.query_activism_documents(conn, 1) == []
        assert activism.query_manager_activism_profile(conn, 1) is None
    finally:
        conn.close()
    assert activism._to_float("2.5") == 2.5
    assert activism._to_float("invalid") is None
    assert activism._to_float(math.inf) is None
    assert activism._to_int("invalid") is None
    assert activism._json_strings("not json") == []
    assert activism._json_strings('{"not": "a list"}') == []


@pytest.mark.asyncio
async def test_small_ranked_target_contracts(monkeypatch: pytest.MonkeyPatch) -> None:
    assert await mas.download({"raw": {"id": 1}}) == b"{'id': 1}"
    assert (await mas.parse(b"abc"))[0]["raw_bytes"] == 3
    assert launch.ui_launch_command(9123)[-2:] == ["--server.port=9123", "--server.headless=true"]

    class Date:
        @classmethod
        def today(cls):
            return real_date(2026, 10, 18)

    captured: list[str] = []

    async def fake_summarise(date: str) -> str:
        captured.append(date)
        return "summary"

    monkeypatch.setattr(summariser_flow.dt, "date", Date)
    monkeypatch.setattr(summariser_flow, "summarise", fake_summarise)
    assert await summariser_flow.summariser_flow.fn() == "summary"
    assert captured == ["2026-10-17"]

    class UnsupportedConn:
        pass

    with pytest.raises(TypeError, match="Unsupported database connection type"):
        activism_flow._ensure_activism_filings_table(UnsupportedConn())


def test_offline_api_imports_require_ui_offline_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("UI_OFFLINE", raising=False)
    with pytest.raises(ModuleNotFoundError):
        _compat.offline_api_imports()
    monkeypatch.setenv("UI_OFFLINE", "1")
    router_cls, model_cls, field_fn, query_fn = _compat.offline_api_imports()
    assert router_cls is _compat.OfflineAPIRouter

    def endpoint() -> str:
        return "offline"

    assert router_cls().get("/test")(endpoint) is endpoint
    assert model_cls(**{"x": 1}).model_dump() == {"x": 1}
    assert field_fn(default_factory=list) == []
    assert query_fn(default=7) == 7


def test_activism_date_helpers_and_filtered_queries() -> None:
    assert activism._to_date(real_date(2026, 1, 2)) == real_date(2026, 1, 2)
    assert activism._to_date(datetime(2026, 1, 2, 3, 4, 5)) == real_date(2026, 1, 2)
    assert activism._to_datetime("2026-01-02T03:04:05Z") == datetime.fromisoformat(
        "2026-01-02T03:04:05+00:00"
    )
    assert activism._json_strings('["a", "b"]') == ["a", "b"]

    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT)")
        activism_flow._ensure_activism_filings_table(conn)
        conn.execute(
            """INSERT INTO activism_filings(
                filing_id, manager_id, filing_type, subject_company, subject_cusip,
                ownership_pct, shares, group_members, purpose_snippet, filed_date, url
            ) VALUES (1, 10, 'SC 13D', 'Target Co', '123AB6789', 6.5, 100, '[]', '', '2026-03-01', 'https://x')"""
        )
        conn.execute("INSERT INTO managers(manager_id, name) VALUES (10, 'Alpha Fund')")
        conn.execute("INSERT INTO managers(manager_id, name) VALUES (20, 'Beta Fund')")
        # Each near match differs on just one predicate, so dropping any filter
        # admits an extra filing even while the other three remain correct.
        conn.executemany(
            """INSERT INTO activism_filings(
                filing_id, manager_id, filing_type, subject_company, subject_cusip,
                ownership_pct, shares, group_members, purpose_snippet, filed_date, url
            ) VALUES (?, ?, ?, 'Target Co', ?, 6.5, 100, '[]', '', ?, 'https://x')""",
            [
                (2, 20, "SC 13D", "123AB6789", "2026-03-01"),
                (3, 10, "SC 13G", "123AB6789", "2026-03-01"),
                (4, 10, "SC 13D", "987654321", "2026-03-01"),
                (5, 10, "SC 13D", "123AB6789", "2025-12-31"),
            ],
        )
        filings = activism.query_activism_filings(
            conn,
            manager_id=10,
            filing_type="SC 13D",
            cusip="123AB6789",
            since=real_date(2026, 1, 1),
        )
        assert [filing.filing_id for filing in filings] == [1]
        match = filings[0]
        assert match.manager_name == "Alpha Fund"
        assert match.filing_type == "SC 13D"
        assert match.subject_company == "Target Co"
        assert match.subject_cusip == "123AB6789"
        assert match.ownership_pct == 6.5
        assert match.shares == 100
        assert match.filed_date == real_date(2026, 3, 1)
        assert match.url == "https://x"
        # upper() CUSIP comparison must accept mixed-case input.
        mixed = activism.query_activism_filings(
            conn,
            manager_id=10,
            filing_type="SC 13D",
            cusip="123AB6789".lower(),
            since=real_date(2026, 1, 1),
        )
        assert [filing.filing_id for filing in mixed] == [1]
    finally:
        conn.close()


def test_activism_flow_ensure_sqlite_table_is_idempotent() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        activism_flow._ensure_activism_filings_table(conn)
        activism_flow._ensure_activism_filings_table(conn)
        names = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='activism_filings'"
            ).fetchall()
        }
        assert "idx_activism_manager" in names
        assert "idx_activism_cusip" in names
        assert "idx_activism_date" in names
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_edgar_request_with_retry_succeeds_after_transient_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = {"count": 0}

    class Response:
        def raise_for_status(self) -> None:
            return None

    class DummyClient:
        async def get(self, *a, **k):
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise edgar.httpx.RequestError("transient network failure")
            return Response()

    class Tracked:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args: object) -> bool:
            return False

        def __call__(self, _response: object) -> None:
            return None

    async def _noop_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(edgar, "tracked_call", lambda *_a, **_k: Tracked())
    monkeypatch.setattr(edgar.asyncio, "sleep", _noop_sleep)
    monkeypatch.setattr(edgar, "_last_edgar_request_at", None)
    response = await edgar._request_with_retry(
        cast(httpx.AsyncClient, DummyClient()),
        "https://data.sec.gov/submissions",
        {"User-Agent": "test"},
        source="edgar",
    )
    assert isinstance(response, Response)
    assert attempts["count"] == 2
