import json
import sqlite3
from contextlib import contextmanager

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


class _UKAdapterWithFormType:
    async def list_new_filings(self, company_number, since):
        return [{"transaction_id": "txn-2", "date": "2024-02-04"}]

    async def download(self, filing):
        return b"%PDF-1.4\nfake\n%%EOF"

    async def parse(self, raw):
        return [
            {
                "company_name": "Alt Type Ltd",
                "company_number": "12345678",
                "filing_date": "2024-02-04",
                "form_type": "CS01",
                "nameOfIssuer": "Not A Real Holding",
                "cusip": "123456789",
                "value": 2000,
                "sshPrnamt": 75,
            }
        ]


class _MetadataOnlyAdapter:
    def __init__(self, *, source, filing_id, date):
        self.source = source
        self.filing_id = filing_id
        self.date = date

    async def list_new_filings(self, identifier, since):
        return [{"id": self.filing_id, "date": self.date}]

    async def download(self, filing):
        return b"metadata"

    async def parse(self, raw):
        return [
            {
                "status": "unsupported",
                "source": self.source,
                "filing_type": f"{self.source}_metadata",
                "errors": [f"{self.source}_documents_not_supported"],
                "raw_bytes": len(raw),
            }
        ]


class _TransactionalConn:
    def __init__(self):
        self.transactions = 0
        self.sql = []

    @contextmanager
    def transaction(self):
        self.transactions += 1
        yield

    def execute(self, sql, params=()):
        self.sql.append((sql, tuple(params)))
        return []


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
async def test_fetch_and_store_us_replaces_existing_holdings_for_same_filing(tmp_path, monkeypatch):
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

    await ingest_flow.fetch_and_store.fn(
        "0000000001",
        "2024-01-01",
        jurisdiction="us",
        db_path=str(db_path),
    )
    await ingest_flow.fetch_and_store.fn(
        "0000000001",
        "2024-01-01",
        jurisdiction="us",
        db_path=str(db_path),
    )

    conn = sqlite3.connect(db_path)
    holdings_count = conn.execute("SELECT COUNT(*) FROM holdings").fetchone()[0]
    holding = conn.execute("SELECT accession, cusip, value, sshPrnamt FROM holdings").fetchone()
    conn.close()

    assert holdings_count == 1
    assert holding == ("0001-24-000001", "123456789", 1000, 50)


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
async def test_fetch_and_store_uk_uses_form_type_and_never_inserts_holdings(tmp_path, monkeypatch):
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

    monkeypatch.setattr(ingest_flow, "get_adapter", lambda _name: _UKAdapterWithFormType())
    monkeypatch.setattr(ingest_flow.S3, "put_object", lambda **_kwargs: None)
    monkeypatch.setattr(ingest_flow, "store_document", lambda _raw: None)

    rows = await ingest_flow.fetch_and_store.fn(
        "12345678",
        "2024-01-01",
        jurisdiction="uk",
        db_path=str(db_path),
    )

    assert rows and rows[0]["form_type"] == "CS01"

    conn = sqlite3.connect(db_path)
    filing = conn.execute(
        "SELECT manager_id, source, external_id, type, parsed_payload FROM filings"
    ).fetchone()
    holdings_count = conn.execute("SELECT COUNT(*) FROM holdings").fetchone()[0]
    conn.close()

    assert filing[:4] == (1, "uk", "txn-2", "CS01")
    payload = json.loads(filing[4])
    assert payload[0]["company_number"] == "12345678"
    assert payload[0]["form_type"] == "CS01"
    assert holdings_count == 0


def test_adapter_registry_covers_documented_jurisdictions():
    assert ingest_flow._ADAPTER_MAP == {
        "us": "edgar",
        "uk": "uk",
        "ca": "canada",
        "sg": "mas",
        "au": "asic",
    }
    assert set(ingest_flow._IDENTIFIER_ENV) == {"us", "uk", "ca", "sg", "au"}


def test_replace_holdings_rows_uses_postgres_transaction(monkeypatch):
    conn = _TransactionalConn()

    monkeypatch.setattr(ingest_flow, "_table_columns", lambda _conn, _table: {"filing_id"})
    monkeypatch.setattr(
        ingest_flow,
        "_insert_holdings_rows",
        lambda *args, **kwargs: 2,
    )

    inserted = ingest_flow._replace_holdings_rows(
        conn,
        filing_id=42,
        manager_id=7,
        identifier="0000000001",
        external_id="0001-24-000001",
        filed_date="2024-01-05",
        parsed_rows=[{"cusip": "123456789"}],
        jurisdiction="us",
    )

    assert inserted == 2
    assert conn.transactions == 1
    assert conn.sql == [("DELETE FROM holdings WHERE filing_id = %s", (42,))]


@pytest.mark.parametrize(
    ("jurisdiction", "env_key", "default_value"),
    [
        ("us", "CIK_LIST", "0001791786,0001434997"),
        ("uk", "UK_COMPANY_NUMBERS", ""),
        ("ca", "CA_CIK_LIST", ""),
        ("sg", "SG_ENTITY_IDS", ""),
        ("au", "AU_ASIC_IDS", ""),
    ],
)
def test_default_identifiers_uses_jurisdiction_environment_mapping(
    monkeypatch, jurisdiction, env_key, default_value
):
    monkeypatch.delenv(env_key, raising=False)
    assert ingest_flow._default_identifiers(jurisdiction) == (
        default_value.split(",") if default_value else []
    )

    monkeypatch.setenv(env_key, " ID-1 , ID-2 ")
    assert ingest_flow._default_identifiers(jurisdiction) == ["ID-1", "ID-2"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("jurisdiction", "registry_ids", "identifier", "filing_id", "date"),
    [
        ("ca", {}, "0000000002", "sedar-1", None),
        ("sg", {"sg_entity_id": "MAS-123"}, "MAS-123", "mas-1", "2024-03-01"),
        ("au", {"au_asic_id": "ASIC-123"}, "ASIC-123", "asic-1", "2024-04-01"),
    ],
)
async def test_fetch_and_store_metadata_jurisdictions_store_payload_without_holdings(
    tmp_path,
    monkeypatch,
    jurisdiction,
    registry_ids,
    identifier,
    filing_id,
    date,
):
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE managers (id INTEGER PRIMARY KEY AUTOINCREMENT, cik TEXT, registry_ids TEXT)"
    )
    conn.execute(
        "INSERT INTO managers(cik, registry_ids) VALUES (?, ?)",
        (identifier if jurisdiction == "ca" else "", json.dumps(registry_ids)),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(
        ingest_flow,
        "get_adapter",
        lambda adapter_name: _MetadataOnlyAdapter(
            source=jurisdiction,
            filing_id=filing_id,
            date=date,
        ),
    )
    monkeypatch.setattr(ingest_flow.S3, "put_object", lambda **_kwargs: None)
    monkeypatch.setattr(ingest_flow, "store_document", lambda _raw: None)

    rows = await ingest_flow.fetch_and_store.fn(
        identifier,
        "2024-01-01",
        jurisdiction=jurisdiction,
        db_path=str(db_path),
    )

    assert rows == [
        {
            "status": "unsupported",
            "source": jurisdiction,
            "filing_type": f"{jurisdiction}_metadata",
            "errors": [f"{jurisdiction}_documents_not_supported"],
            "raw_bytes": len(b"metadata"),
        }
    ]

    conn = sqlite3.connect(db_path)
    filing = conn.execute(
        "SELECT manager_id, source, external_id, filed_date, type, parsed_payload FROM filings"
    ).fetchone()
    holdings_count = conn.execute("SELECT COUNT(*) FROM holdings").fetchone()[0]
    conn.close()

    assert filing[:5] == (1, jurisdiction, filing_id, date, f"{jurisdiction}_metadata")
    assert json.loads(filing[5])[0]["status"] == "unsupported"
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
    assert captured["fetcher"] is not edgar_flow.fetch_and_store


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
