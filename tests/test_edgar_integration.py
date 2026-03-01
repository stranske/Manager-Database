import hashlib
import json
import logging
import sqlite3
import sys
from pathlib import Path

import httpx
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

import adapters.edgar as edgar
import etl.edgar_flow as flow


def seed_manager(db_path, cik, manager_id=1):
    conn = sqlite3.connect(db_path)
    conn.execute("""CREATE TABLE IF NOT EXISTS managers (
            manager_id INTEGER PRIMARY KEY,
            name TEXT,
            cik TEXT UNIQUE
        )""")
    conn.execute(
        "INSERT INTO managers(manager_id, name, cik) VALUES (?, ?, ?)",
        (manager_id, "Test Manager", cik),
    )
    conn.commit()
    conn.close()



def make_client(responder):
    class DummyClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url, headers=None):
            return responder(url)

    return DummyClient


def sample_filing_payload():
    return {
        "filings": {
            "recent": {
                "form": ["13F-HR"],
                "filingDate": ["2024-05-01"],
                "accessionNumber": ["0000000000-24-000001"],
            }
        }
    }


def sample_xml():
    return (
        "<edgarSubmission>"
        "<infoTable>"
        "<nameOfIssuer>Example Corp</nameOfIssuer>"
        "<cusip>123456789</cusip>"
        "<value>1000</value>"
        "<shrsOrPrnAmt><sshPrnamt>100</sshPrnamt></shrsOrPrnAmt>"
        "</infoTable>"
        "</edgarSubmission>"
    )


def sample_xml_multiple():
    return (
        "<edgarSubmission>"
        "<infoTable>"
        "<nameOfIssuer>Example Corp</nameOfIssuer>"
        "<cusip>123456789</cusip>"
        "<value>1000</value>"
        "<shrsOrPrnAmt><sshPrnamt>100</sshPrnamt></shrsOrPrnAmt>"
        "</infoTable>"
        "<infoTable>"
        "<nameOfIssuer>Missing Fields</nameOfIssuer>"
        "<cusip></cusip>"
        "<value></value>"
        "<shrsOrPrnAmt><sshPrnamt></sshPrnamt></shrsOrPrnAmt>"
        "</infoTable>"
        "</edgarSubmission>"
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_edgar_flow_full_cycle(monkeypatch, tmp_path):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")
    monkeypatch.setattr(flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "RAW_DIR", tmp_path)
    monkeypatch.setattr(flow, "ADAPTER", edgar)
    monkeypatch.setattr(flow, "fetch_and_store", flow.fetch_and_store.fn)

    put_calls = []

    def put_object(**kwargs):
        put_calls.append(kwargs)

    monkeypatch.setattr(flow.S3, "put_object", put_object)
    seed_manager(db_path, "0000000000", manager_id=10)

    def responder(url):
        if "submissions/CIK" in url:
            return httpx.Response(
                200, request=httpx.Request("GET", url), json=sample_filing_payload()
            )
        if "primary_doc.xml" in url:
            return httpx.Response(200, request=httpx.Request("GET", url), text=sample_xml())
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(edgar.httpx, "AsyncClient", make_client(responder))

    rows = await flow.edgar_flow.fn(cik_list=["0000000000"], since="2024-01-01")

    assert rows == [
        {
            "nameOfIssuer": "Example Corp",
            "cusip": "123456789",
            "value": 1000,
            "sshPrnamt": 100,
        }
    ]
    expected_prefix = hashlib.sha256(sample_xml().encode("utf-8")).hexdigest()[:16]
    assert put_calls[0]["Key"] == f"raw/edgar/{expected_prefix}_0000000000-24-000001.xml"
    parsed_path = tmp_path / "parsed.json"
    assert json.loads(parsed_path.read_text()) == rows
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT cik, accession, cusip, value, sshPrnamt FROM holdings").fetchone()
    conn.close()
    assert row == ("0000000000", "0000000000-24-000001", "123456789", 1000, 100)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fetch_step_with_mocked_http(monkeypatch, tmp_path):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    def responder(url):
        return httpx.Response(200, request=httpx.Request("GET", url), json=sample_filing_payload())

    monkeypatch.setattr(edgar.httpx, "AsyncClient", make_client(responder))

    filings = await edgar.list_new_filings("0000000000", "2024-01-01")

    assert filings == [
        {"accession": "0000000000-24-000001", "cik": "0000000000", "filed": "2024-05-01"}
    ]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_store_step_with_mocked_storage(monkeypatch, tmp_path):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")
    monkeypatch.setattr(flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "ADAPTER", edgar)

    stored = []

    def record_document(raw, **kwargs):
        stored.append((raw, kwargs))

    put_calls = []

    def put_object(**kwargs):
        put_calls.append(kwargs)

    # Mock external storage writes to keep the test local.
    def responder(url):
        if "submissions/CIK" in url:
            return httpx.Response(
                200, request=httpx.Request("GET", url), json=sample_filing_payload()
            )
        if "primary_doc.xml" in url:
            return httpx.Response(200, request=httpx.Request("GET", url), text=sample_xml())
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(edgar.httpx, "AsyncClient", make_client(responder))
    monkeypatch.setattr(flow, "store_document", record_document)
    monkeypatch.setattr(flow.S3, "put_object", put_object)
    seed_manager(db_path, "0000000000")

    rows = await flow.fetch_and_store.fn("0000000000", "2024-01-01")

    assert rows
    assert stored == [
        (
            sample_xml(),
            {
                "db_path": str(db_path),
                "manager_id": 1,
                "kind": "filing_text",
                "filename": "0000000000-24-000001.xml",
            },
        )
    ]
    expected_prefix = hashlib.sha256(sample_xml().encode("utf-8")).hexdigest()[:16]
    assert put_calls[0]["Key"] == f"raw/edgar/{expected_prefix}_0000000000-24-000001.xml"
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT cik, accession, cusip, value, sshPrnamt FROM holdings").fetchone()
    conn.close()
    assert row == ("0000000000", "0000000000-24-000001", "123456789", 1000, 100)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parse_step_with_mocked_input():
    rows = await edgar.parse(sample_xml())
    assert rows == [
        {
            "nameOfIssuer": "Example Corp",
            "cusip": "123456789",
            "value": 1000,
            "sshPrnamt": 100,
        }
    ]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parse_step_with_mocked_input_multiple_rows():
    # Include missing numeric fields to verify parser defaults to zero.
    rows = await edgar.parse(sample_xml_multiple())
    assert rows == [
        {
            "nameOfIssuer": "Example Corp",
            "cusip": "123456789",
            "value": 1000,
            "sshPrnamt": 100,
        },
        {
            "nameOfIssuer": "Missing Fields",
            "cusip": "",
            "value": 0,
            "sshPrnamt": 0,
        },
    ]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parse_step_with_mocked_input_fixture():
    sample_path = Path(__file__).parent / "data" / "sample_13f.xml"
    rows = await edgar.parse(sample_path.read_text())

    assert rows == [
        {
            "nameOfIssuer": "Example Corp",
            "cusip": "123456789",
            "value": 1000,
            "sshPrnamt": 100,
        }
    ]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parse_step_with_mocked_input_empty_document():
    rows = await edgar.parse("<edgarSubmission></edgarSubmission>")
    assert rows == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rate_limit_handling(monkeypatch, tmp_path):
    attempts = {"count": 0}
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    async def fast_sleep(_):
        return None

    class DummyClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url, headers=None):
            attempts["count"] += 1
            return httpx.Response(429, request=httpx.Request("GET", url))

    monkeypatch.setattr(edgar.httpx, "AsyncClient", DummyClient)
    monkeypatch.setattr(edgar.asyncio, "sleep", fast_sleep)

    with pytest.raises(httpx.HTTPStatusError):
        await edgar.list_new_filings("0000000000", "2024-01-01")

    assert attempts["count"] == 3


@pytest.mark.integration
@pytest.mark.asyncio
async def test_malformed_data_handling(monkeypatch, tmp_path, caplog):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")
    monkeypatch.setattr(flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "RAW_DIR", tmp_path)
    monkeypatch.setattr(flow, "ADAPTER", edgar)
    monkeypatch.setattr(flow, "fetch_and_store", flow.fetch_and_store.fn)
    monkeypatch.setattr(flow.S3, "put_object", lambda **kwargs: None)

    def responder(url):
        if "submissions/CIK" in url:
            return httpx.Response(
                200, request=httpx.Request("GET", url), json=sample_filing_payload()
            )
        if "primary_doc.xml" in url:
            return httpx.Response(200, request=httpx.Request("GET", url), text="<xml>")
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(edgar.httpx, "AsyncClient", make_client(responder))

    caplog.set_level(logging.ERROR, logger="etl.edgar_flow")
    rows = await flow.edgar_flow.fn(cik_list=["0000000000"], since="2024-01-01")

    assert rows == []
    assert any("EDGAR flow failed" in msg for msg in caplog.messages)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_network_timeout_handling(monkeypatch, tmp_path):
    attempts = {"count": 0}
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    async def fast_sleep(_):
        return None

    class DummyClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url, headers=None):
            attempts["count"] += 1
            raise httpx.ConnectTimeout("timeout", request=httpx.Request("GET", url))

    monkeypatch.setattr(edgar.httpx, "AsyncClient", DummyClient)
    monkeypatch.setattr(edgar.asyncio, "sleep", fast_sleep)

    with pytest.raises(httpx.RequestError):
        await edgar.list_new_filings("0000000000", "2024-01-01")

    assert attempts["count"] == 3
