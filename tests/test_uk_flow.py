import importlib
import json
import sqlite3
from contextlib import asynccontextmanager

import httpx
import pytest

from adapters import uk as uk_adapter
import etl.ingest_flow as ingest_flow
import etl.uk_flow as uk_flow


class MockCompaniesHouseAdapter:
    async def list_new_filings(self, company_number: str, since: str):
        assert company_number == "12345678"
        assert since == "2024-01-01"
        return [
            {
                "transaction_id": "txn-uk-001",
                "company_number": company_number,
                "date": "2024-01-05",
            }
        ]

    async def download(self, filing: dict[str, str]):
        assert filing["transaction_id"] == "txn-uk-001"
        return b"%PDF-1.4\nmock\n%%EOF"

    async def parse(self, raw: bytes):
        assert raw.startswith(b"%PDF")
        return [
            {
                "company_name": "Example Widgets Ltd",
                "company_number": "12345678",
                "filing_date": "2024-01-05",
                "filing_type": "CS01",
                "errors": [],
                "status": "ok",
            }
        ]


class _MockCompaniesHouseClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url: str, params: dict[str, str] | None = None):
        request = httpx.Request("GET", url, params=params)
        if url.endswith("/filing-history"):
            assert params == {"category": "annual-return", "since": "2024-01-01"}
            return httpx.Response(
                200,
                request=request,
                json={
                    "items": [
                        {
                            "transaction_id": "txn-uk-api-001",
                            "date": "2024-01-05T10:00:00Z",
                        }
                    ]
                },
            )
        if "document?format=pdf" in url:
            pdf = (
                b"%PDF-1.4\n"
                b"(Confirmation Statement CS01)\n"
                b"(Company name in full: Example Widgets Ltd)\n"
                b"(Company number: 12345678)\n"
                b"(Date of filing: 2024-01-05)\n"
                b"%%EOF"
            )
            return httpx.Response(200, request=request, content=pdf)
        return httpx.Response(404, request=request)


@pytest.mark.asyncio
async def test_uk_flow_inserts_uk_filing_with_payload_keys(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

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

    monkeypatch.setattr(ingest_flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(ingest_flow, "RAW_DIR", raw_dir)
    monkeypatch.setattr(ingest_flow, "get_adapter", lambda _name: MockCompaniesHouseAdapter())
    monkeypatch.setattr(ingest_flow.S3, "put_object", lambda **_kwargs: None)
    monkeypatch.setattr(ingest_flow, "store_document", lambda _raw: None)

    async def direct_fetcher(identifier: str, since: str):
        return await ingest_flow.fetch_and_store.fn(
            identifier,
            since,
            jurisdiction="uk",
            db_path=str(db_path),
        )

    rows = await ingest_flow.ingest_flow.fn(
        jurisdiction="uk",
        identifiers=["12345678"],
        since="2024-01-01",
        fetcher=direct_fetcher,
    )

    assert len(rows) == 1
    assert rows[0]["filing_type"] == "CS01"
    assert rows[0]["company_number"] == "12345678"

    conn = sqlite3.connect(db_path)
    filing = conn.execute(
        "SELECT manager_id, source, external_id, type, parsed_payload FROM filings"
    ).fetchone()
    conn.close()

    assert filing is not None
    assert filing[0] == 1
    assert filing[1] == "uk"
    assert filing[2] == "txn-uk-001"
    assert filing[3] == "CS01"
    payload = json.loads(filing[4])
    assert payload[0]["company_name"] == "Example Widgets Ltd"
    assert payload[0]["company_number"] == "12345678"
    assert payload[0]["filing_date"] == "2024-01-05"
    assert payload[0]["filing_type"] == "CS01"
    assert payload[0]["status"] == "ok"
    assert payload[0]["errors"] == []


@pytest.mark.asyncio
async def test_uk_flow_mocks_companies_house_api_and_inserts_filing(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

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

    @asynccontextmanager
    async def dummy_tracked_call(*args, **kwargs):
        def _log(_resp):
            return None

        yield _log

    monkeypatch.setattr(ingest_flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(ingest_flow, "RAW_DIR", raw_dir)
    monkeypatch.setattr(ingest_flow, "get_adapter", lambda _name: uk_adapter)
    monkeypatch.setattr(uk_adapter.httpx, "AsyncClient", _MockCompaniesHouseClient)
    monkeypatch.setattr(uk_adapter, "tracked_call", dummy_tracked_call)
    monkeypatch.setattr(ingest_flow.S3, "put_object", lambda **_kwargs: None)
    monkeypatch.setattr(ingest_flow, "store_document", lambda _raw: None)

    rows = await ingest_flow.fetch_and_store.fn(
        "12345678",
        "2024-01-01",
        jurisdiction="uk",
        db_path=str(db_path),
    )

    assert len(rows) == 1

    conn = sqlite3.connect(db_path)
    filing = conn.execute(
        "SELECT manager_id, source, external_id, type, parsed_payload FROM filings"
    ).fetchone()
    conn.close()

    assert filing is not None
    assert filing[:4] == (1, "uk", "txn-uk-api-001", "confirmation_statement")
    payload = json.loads(filing[4])
    assert payload[0]["company_number"] == "12345678"
    assert payload[0]["company_name"] == "Example Widgets Ltd"
    assert payload[0]["filing_date"] == "2024-01-05"
    assert payload[0]["filing_type"] == "confirmation_statement"


@pytest.mark.asyncio
async def test_uk_flow_inserts_into_canonical_filings_schema(tmp_path, monkeypatch):
    db_path = tmp_path / "canonical.db"
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT NOT NULL, cik TEXT, registry_ids TEXT)"
    )
    conn.execute(
        "CREATE TABLE filings ("
        "filing_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "manager_id INTEGER NOT NULL, "
        "type TEXT NOT NULL, "
        "filed_date TEXT, "
        "source TEXT NOT NULL, "
        "raw_key TEXT UNIQUE, "
        "parsed_payload TEXT, "
        "FOREIGN KEY(manager_id) REFERENCES managers(manager_id)"
        ")"
    )
    conn.execute(
        "CREATE TABLE holdings ("
        "holding_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "filing_id INTEGER NOT NULL, "
        "cusip TEXT, "
        "name_of_issuer TEXT, "
        "shares INTEGER, "
        "value_usd INTEGER, "
        "FOREIGN KEY(filing_id) REFERENCES filings(filing_id)"
        ")"
    )
    conn.execute(
        "INSERT INTO managers(manager_id, name, cik, registry_ids) VALUES (?, ?, ?, ?)",
        (42, "Example Widgets Ltd", "", json.dumps({"uk_company_number": "12345678"})),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(ingest_flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(ingest_flow, "RAW_DIR", raw_dir)
    monkeypatch.setattr(ingest_flow, "get_adapter", lambda _name: MockCompaniesHouseAdapter())
    monkeypatch.setattr(ingest_flow.S3, "put_object", lambda **_kwargs: None)
    monkeypatch.setattr(ingest_flow, "store_document", lambda _raw: None)

    rows = await ingest_flow.fetch_and_store.fn(
        "12345678",
        "2024-01-01",
        jurisdiction="uk",
        db_path=str(db_path),
    )

    assert len(rows) == 1

    conn = sqlite3.connect(db_path)
    filing = conn.execute(
        "SELECT manager_id, source, type, raw_key, parsed_payload FROM filings"
    ).fetchone()
    holdings_count = conn.execute("SELECT COUNT(*) FROM holdings").fetchone()[0]
    conn.close()

    assert filing is not None
    assert filing[:4] == (42, "uk", "CS01", "uk:txn-uk-001")
    payload = json.loads(filing[4])
    assert payload[0]["company_number"] == "12345678"
    assert payload[0]["filing_type"] == "CS01"
    assert holdings_count == 0


def test_uk_flow_deployment_uses_nightly_defaults(monkeypatch):
    monkeypatch.delenv("UK_FLOW_CRON", raising=False)
    monkeypatch.delenv("UK_FLOW_TIMEZONE", raising=False)
    monkeypatch.setenv("TZ", "UTC")
    module = importlib.reload(uk_flow)

    assert module.UK_FLOW_NIGHTLY_CRON == "0 1 * * *"
    assert module.UK_FLOW_TIMEZONE == "UTC"
    schedule = module.uk_flow_deployment.schedules[0].schedule
    assert schedule.cron == "0 1 * * *"
    assert schedule.timezone == "UTC"


def test_uk_flow_deployment_allows_env_overrides(monkeypatch):
    monkeypatch.setenv("UK_FLOW_CRON", "30 2 * * *")
    monkeypatch.setenv("UK_FLOW_TIMEZONE", "Europe/London")
    module = importlib.reload(uk_flow)

    assert module.UK_FLOW_NIGHTLY_CRON == "30 2 * * *"
    assert module.UK_FLOW_TIMEZONE == "Europe/London"
    schedule = module.uk_flow_deployment.schedules[0].schedule
    assert schedule.cron == "30 2 * * *"
    assert schedule.timezone == "Europe/London"
