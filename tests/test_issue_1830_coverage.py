"""Focused regression coverage for the ranked #1830 adapter and API gaps."""

from __future__ import annotations

import math
import sqlite3
from datetime import date as real_date

import pytest

from adapters import edgar, mas
from api import activism
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
