import importlib
import json
import sqlite3

import pytest

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
