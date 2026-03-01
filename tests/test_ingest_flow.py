import json
import sqlite3

import pytest

import etl.edgar_flow as edgar_flow
import etl.ingest_flow as ingest_flow
import etl.uk_flow as uk_flow


class _USAdapter:
    async def list_new_filings(self, cik, since):
        return [{"accession": "0001-24-000001", "filed": "2024-01-05"}]

    async def download(self, filing):
        return "<xml>payload</xml>"

    async def parse(self, raw):
        return [
            {"nameOfIssuer": "Example Corp", "cusip": "123456789", "value": 1000, "sshPrnamt": 50}
        ]


class _UKAdapter:
    async def list_new_filings(self, company_number, since):
        return [{"transaction_id": "txn-1", "date": "2024-02-03"}]

    async def download(self, filing):
        return b"%PDF-1.4\nfake\n%%EOF"

    async def parse(self, raw):
        return [
            {
                "company_name": "Example Widgets Ltd",
                "company_number": "12345678",
                "filing_date": "2024-02-03",
                "filing_type": "confirmation_statement",
                "errors": [],
                "status": "ok",
            }
        ]


@pytest.mark.asyncio
async def test_fetch_and_store_us_uses_manager_cik_and_inserts_holdings(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE managers (id INTEGER PRIMARY KEY AUTOINCREMENT, cik TEXT, registry_ids TEXT)"
    )
    conn.execute("INSERT INTO managers(cik, registry_ids) VALUES (?, ?)", ("0000000001", "{}"))
    conn.commit()
    conn.close()

    monkeypatch.setattr(ingest_flow, "get_adapter", lambda _name: _USAdapter())
    monkeypatch.setattr(ingest_flow.S3, "put_object", lambda **_kwargs: None)
    monkeypatch.setattr(ingest_flow, "store_document", lambda _raw: None)

    rows = await ingest_flow.fetch_and_store.fn(
        "0000000001",
        "2024-01-01",
        jurisdiction="us",
        db_path=str(db_path),
    )

    assert rows and rows[0]["cusip"] == "123456789"

    conn = sqlite3.connect(db_path)
    filing = conn.execute("SELECT manager_id, source, external_id, type FROM filings").fetchone()
    holding = conn.execute(
        "SELECT manager_id, cik, accession, cusip, value, sshPrnamt FROM holdings"
    ).fetchone()
    conn.close()

    assert filing == (1, "us", "0001-24-000001", "13F-HR")
    assert holding == (1, "0000000001", "0001-24-000001", "123456789", 1000, 50)


@pytest.mark.asyncio
async def test_fetch_and_store_uk_uses_registry_id_and_stores_payload(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE managers (id INTEGER PRIMARY KEY AUTOINCREMENT, cik TEXT, registry_ids TEXT)"
    )
    conn.execute(
        "INSERT INTO managers(cik, registry_ids) VALUES (?, ?)",
        ("", json.dumps({"uk_company_number": "12345678"})),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(ingest_flow, "get_adapter", lambda _name: _UKAdapter())
    monkeypatch.setattr(ingest_flow.S3, "put_object", lambda **_kwargs: None)
    monkeypatch.setattr(ingest_flow, "store_document", lambda _raw: None)

    rows = await ingest_flow.fetch_and_store.fn(
        "12345678",
        "2024-01-01",
        jurisdiction="uk",
        db_path=str(db_path),
    )

    assert rows and rows[0]["filing_type"] == "confirmation_statement"

    conn = sqlite3.connect(db_path)
    filing = conn.execute(
        "SELECT manager_id, source, external_id, type, parsed_payload FROM filings"
    ).fetchone()
    holdings_count = conn.execute("SELECT COUNT(*) FROM holdings").fetchone()[0]
    conn.close()

    assert filing[:4] == (1, "uk", "txn-1", "confirmation_statement")
    payload = json.loads(filing[4])
    assert payload[0]["company_number"] == "12345678"
    assert payload[0]["company_name"] == "Example Widgets Ltd"
    assert holdings_count == 0


@pytest.mark.asyncio
async def test_edgar_flow_is_us_wrapper(monkeypatch):
    captured = {}

    async def fake_ingest_flow(*, jurisdiction, identifiers, since, fetcher):
        captured["jurisdiction"] = jurisdiction
        captured["identifiers"] = identifiers
        captured["since"] = since
        captured["fetcher"] = fetcher
        return []

    monkeypatch.setattr(edgar_flow.ingest_module, "ingest_flow", fake_ingest_flow)

    rows = await edgar_flow.edgar_flow.fn(cik_list=["0001"], since="2024-01-01")

    assert rows == []
    assert captured["jurisdiction"] == "us"
    assert captured["identifiers"] == ["0001"]
    assert captured["since"] == "2024-01-01"
    assert captured["fetcher"] is edgar_flow.fetch_and_store


@pytest.mark.asyncio
async def test_uk_flow_is_uk_wrapper(monkeypatch):
    captured = {}

    async def fake_ingest_flow(*, jurisdiction, identifiers, since, fetcher=None):
        captured["jurisdiction"] = jurisdiction
        captured["identifiers"] = identifiers
        captured["since"] = since
        captured["fetcher"] = fetcher
        return []

    monkeypatch.setattr(uk_flow, "ingest_flow", fake_ingest_flow)

    rows = await uk_flow.uk_flow.fn(company_numbers=["12345678"], since="2024-01-01")

    assert rows == []
    assert captured["jurisdiction"] == "uk"
    assert captured["identifiers"] == ["12345678"]
    assert captured["since"] == "2024-01-01"
    assert captured["fetcher"] is None
