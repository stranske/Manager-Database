from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from adapters import openfigi
from etl import edgar_flow


class DummyResponse:
    def __init__(self, payload):
        self.payload = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return self.payload


def _conn(tmp_path: Path):
    conn = sqlite3.connect(tmp_path / "ids.db")
    conn.execute("""CREATE TABLE holdings (
            holding_id INTEGER PRIMARY KEY,
            filing_id INTEGER,
            cusip TEXT,
            isin TEXT,
            name_of_issuer TEXT,
            shares INTEGER,
            value_usd NUMERIC
        )""")
    return conn


def test_openfigi_resolves_known_cusip_and_uses_cache(tmp_path: Path):
    calls = []

    def opener(request, timeout):
        calls.append((json.loads(request.data.decode("utf-8")), timeout))
        return DummyResponse(
            [
                {
                    "data": [
                        {
                            "ticker": "AAPL",
                            "figi": "BBG000B9XRY4",
                            "compositeFIGI": "BBG000B9XRY4",
                            "name": "APPLE INC",
                        }
                    ]
                }
            ]
        )

    conn = _conn(tmp_path)
    rows = [{"cusip": "037833100", "nameOfIssuer": "Apple Inc", "value": 10, "sshPrnamt": 1}]
    client = openfigi.OpenFigiClient(api_key="test-key", opener=opener, sleep=lambda _delay: None)

    rate = openfigi.resolve_holding_identifiers(conn, rows, filing_id=10, client=client)
    second_rows = [{"cusip": "037833100", "nameOfIssuer": "Apple Inc"}]
    second_rate = openfigi.resolve_holding_identifiers(
        conn, second_rows, filing_id=11, client=client
    )

    assert rate == 0.0
    assert second_rate == 0.0
    assert len(calls) == 1
    assert rows[0]["resolved_ticker"] == "AAPL"
    assert rows[0]["resolved_figi"] == "BBG000B9XRY4"
    assert second_rows[0]["resolved_ticker"] == "AAPL"
    assert conn.execute("SELECT ticker FROM identifier_resolution_cache").fetchone() == ("AAPL",)


def test_openfigi_records_unmapped_rate_without_dropping_holding(tmp_path: Path):
    conn = _conn(tmp_path)
    rows = [
        {"cusip": "111111111", "nameOfIssuer": "Unmapped", "value": 10, "sshPrnamt": 1},
        {"cusip": "", "nameOfIssuer": "No CUSIP", "value": 20, "sshPrnamt": 2},
    ]
    client = openfigi.OpenFigiClient(
        api_key="test-key", opener=lambda *_args, **_kwargs: DummyResponse([{}])
    )

    rate = openfigi.resolve_holding_identifiers(conn, rows, filing_id=42, client=client)

    assert rate == 1.0
    assert rows[0].get("resolved_ticker") is None
    assert rows[0]["nameOfIssuer"] == "Unmapped"
    assert conn.execute(
        "SELECT source, filing_id, total_cusips, unmapped_cusips, unmapped_cusip_rate "
        "FROM identifier_resolution_metrics"
    ).fetchone() == ("edgar", 42, 1, 1, 1.0)


def test_openfigi_rejects_malformed_cusips_before_lookup(tmp_path: Path):
    calls = []
    conn = _conn(tmp_path)
    rows = [{"cusip": "037833100999", "nameOfIssuer": "Malformed", "value": 10}]
    client = openfigi.OpenFigiClient(
        api_key="test-key",
        opener=lambda *_args, **_kwargs: calls.append(True) or DummyResponse([]),
    )

    rate = openfigi.resolve_holding_identifiers(conn, rows, filing_id=43, client=client)

    assert rate == 0.0
    assert calls == []
    assert rows[0].get("resolved_ticker") is None


def test_openfigi_counts_unmapped_unique_cusips(tmp_path: Path):
    conn = _conn(tmp_path)
    rows = [
        {"cusip": "111111111", "nameOfIssuer": "Unmapped A", "value": 10},
        {"cusip": "111111111", "nameOfIssuer": "Unmapped B", "value": 20},
    ]
    client = openfigi.OpenFigiClient(
        api_key="test-key", opener=lambda *_args, **_kwargs: DummyResponse([{}])
    )

    rate = openfigi.resolve_holding_identifiers(conn, rows, filing_id=44, client=client)

    assert rate == 1.0
    assert conn.execute(
        "SELECT total_cusips, unmapped_cusips, unmapped_cusip_rate "
        "FROM identifier_resolution_metrics"
    ).fetchone() == (1, 1, 1.0)


def test_openfigi_cache_lookup_chunks_large_batches(monkeypatch, tmp_path: Path):
    conn = _conn(tmp_path)
    openfigi.ensure_identifier_resolution_schema(conn)
    conn.executemany(
        "INSERT INTO identifier_resolution_cache(cusip, ticker, source) VALUES (?, ?, ?)",
        [
            ("111111111", "AAA", "openfigi"),
            ("222222222", "BBB", "openfigi"),
            ("333333333", "CCC", "openfigi"),
        ],
    )
    monkeypatch.setattr(openfigi, "CACHE_LOOKUP_CHUNK_SIZE", 2)

    cached = openfigi._load_cached(conn, ["111111111", "222222222", "333333333"])

    assert sorted(cached) == ["111111111", "222222222", "333333333"]
    assert cached["333333333"].ticker == "CCC"


@pytest.mark.asyncio
async def test_edgar_flow_persists_resolved_identifiers(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "flow.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT, cik TEXT UNIQUE)"
    )
    conn.execute(
        "INSERT INTO managers(manager_id, name, cik) VALUES (100, 'Manager', '0000320193')"
    )
    conn.commit()
    conn.close()

    class Adapter:
        async def list_new_filings(self, _cik, _since):
            return [{"accession": "a1", "cik": "0000320193", "filed": "2024-05-01"}]

        async def download(self, _filing):
            return "<xml />"

        async def parse(self, _raw):
            return [
                {"nameOfIssuer": "Apple Inc", "cusip": "037833100", "value": 10, "sshPrnamt": 1}
            ]

    def fake_resolve(conn, rows, *, filing_id=None, source="edgar"):
        assert filing_id is not None
        assert source == "edgar"
        for row in rows:
            row["resolved_ticker"] = "AAPL"
            row["resolved_figi"] = "BBG000B9XRY4"
            row["resolution_source"] = "openfigi"
        openfigi.ensure_identifier_resolution_schema(conn)
        return 0.0

    async def fake_fire_alerts(*_args, **_kwargs):
        return None

    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(edgar_flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(edgar_flow, "ADAPTER", Adapter())
    monkeypatch.setattr(edgar_flow.S3, "put_object", lambda **_kwargs: None)
    monkeypatch.setattr(edgar_flow, "store_document", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(edgar_flow, "fire_alerts_for_event", fake_fire_alerts)
    monkeypatch.setattr(edgar_flow, "resolve_holding_identifiers", fake_resolve)

    await edgar_flow.fetch_and_store.fn("0000320193", "2024-01-01")

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT cusip, resolved_ticker, resolved_figi, resolution_source FROM holdings"
    ).fetchone()
    conn.close()
    assert row == ("037833100", "AAPL", "BBG000B9XRY4", "openfigi")


@pytest.mark.asyncio
async def test_edgar_flow_inserts_holdings_when_identifier_resolution_fails(
    monkeypatch, tmp_path: Path
):
    db_path = tmp_path / "flow.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT, cik TEXT UNIQUE)"
    )
    conn.execute(
        "INSERT INTO managers(manager_id, name, cik) VALUES (100, 'Manager', '0000320193')"
    )
    conn.commit()
    conn.close()

    class Adapter:
        async def list_new_filings(self, _cik, _since):
            return [{"accession": "a1", "cik": "0000320193", "filed": "2024-05-01"}]

        async def download(self, _filing):
            return "<xml />"

        async def parse(self, _raw):
            return [
                {"nameOfIssuer": "Apple Inc", "cusip": "037833100", "value": 10, "sshPrnamt": 1}
            ]

    def failing_resolve(*_args, **_kwargs):
        raise RuntimeError("resolver unavailable")

    async def fake_fire_alerts(*_args, **_kwargs):
        return None

    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(edgar_flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(edgar_flow, "ADAPTER", Adapter())
    monkeypatch.setattr(edgar_flow.S3, "put_object", lambda **_kwargs: None)
    monkeypatch.setattr(edgar_flow, "store_document", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(edgar_flow, "fire_alerts_for_event", fake_fire_alerts)
    monkeypatch.setattr(edgar_flow, "resolve_holding_identifiers", failing_resolve)

    await edgar_flow.fetch_and_store.fn("0000320193", "2024-01-01")

    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT cusip, name_of_issuer FROM holdings").fetchone()
    conn.close()
    assert row == ("037833100", "Apple Inc")


def test_schema_contains_identifier_resolution_tables():
    schema = Path("schema.sql").read_text(encoding="utf-8")
    migration = Path("alembic/versions/011_openfigi_identifier_resolution.py").read_text(
        encoding="utf-8"
    )

    assert "identifier_resolution_cache" in schema
    assert "identifier_resolution_metrics" in schema
    assert "resolved_ticker text" in schema
    assert "identifier_resolution_cache" in migration
