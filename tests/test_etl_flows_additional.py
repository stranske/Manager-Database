import json
import sqlite3
from contextlib import asynccontextmanager
from datetime import date

import pytest

import etl.daily_diff_flow as daily_flow
import etl.edgar_flow as edgar_flow
import etl.summariser_flow as summariser_flow


# Fixed date for deterministic flow defaults.
class FixedDate(date):
    @classmethod
    def today(cls):
        return cls(2024, 5, 2)


@pytest.mark.asyncio
async def test_edgar_flow_writes_parsed_json_and_skips_warnings(tmp_path, monkeypatch):
    monkeypatch.setenv("CIK_LIST", "skip,ok")
    monkeypatch.setattr(edgar_flow, "RAW_DIR", tmp_path)
    calls = []

    async def fake_fetch_and_store(cik, since):
        calls.append((cik, since))
        if cik == "skip":
            raise UserWarning("skip")
        return [
            {
                "nameOfIssuer": "Corp",
                "cusip": f"{cik}CUS",
                "value": 1,
                "sshPrnamt": 1,
            }
        ]

    # Mock task to avoid external storage and adapters.
    monkeypatch.setattr(edgar_flow, "fetch_and_store", fake_fetch_and_store)

    rows = await edgar_flow.edgar_flow.fn()

    assert calls == [("skip", "1970-01-01"), ("ok", "1970-01-01")]
    assert rows == [{"nameOfIssuer": "Corp", "cusip": "okCUS", "value": 1, "sshPrnamt": 1}]
    parsed = json.loads((tmp_path / "parsed.json").read_text())
    assert parsed == rows


@pytest.mark.asyncio
async def test_fetch_and_store_returns_empty_for_no_filings(tmp_path, monkeypatch):
    class EmptyAdapter:
        async def list_new_filings(self, cik, since):
            return []

        async def download(self, filing):
            raise AssertionError("download should not be called")

        async def parse(self, raw):
            raise AssertionError("parse should not be called")

    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(edgar_flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(edgar_flow, "ADAPTER", EmptyAdapter())

    rows = await edgar_flow.fetch_and_store.fn("0", "2024-01-01")

    assert rows == []
    conn = sqlite3.connect(db_path)
    # Ensure the holdings table exists even with no filings.
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='holdings'"
    ).fetchall()
    conn.close()
    assert tables == [("holdings",)]


@pytest.mark.asyncio
async def test_fetch_and_store_uploads_raw_and_persists_rows(tmp_path, monkeypatch):
    filing = {"accession": "0001", "filed": "2024-01-01"}

    class Adapter:
        async def list_new_filings(self, cik, since):
            return [filing]

        async def download(self, filing_info):
            assert filing_info == filing
            return "<xml>raw</xml>"

        async def parse(self, raw):
            assert raw == "<xml>raw</xml>"
            return [
                {"nameOfIssuer": "CorpA", "cusip": "AAA", "value": 10, "sshPrnamt": 1},
                {"nameOfIssuer": "CorpB", "cusip": "BBB", "value": 20, "sshPrnamt": 2},
            ]

    # Capture side effects for assertions without external services.
    recorded = {}

    def fake_store_document(raw):
        recorded["stored"] = raw

    class DummyS3:
        def put_object(self, **kwargs):
            recorded["s3"] = kwargs

    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(edgar_flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(edgar_flow, "ADAPTER", Adapter())
    monkeypatch.setattr(edgar_flow, "S3", DummyS3())
    monkeypatch.setattr(edgar_flow, "BUCKET", "filings-test")
    monkeypatch.setattr(edgar_flow, "store_document", fake_store_document)

    rows = await edgar_flow.fetch_and_store.fn("0001", "2024-01-01")

    assert rows == [
        {"nameOfIssuer": "CorpA", "cusip": "AAA", "value": 10, "sshPrnamt": 1},
        {"nameOfIssuer": "CorpB", "cusip": "BBB", "value": 20, "sshPrnamt": 2},
    ]
    assert recorded["stored"] == "<xml>raw</xml>"
    assert recorded["s3"] == {
        "Bucket": "filings-test",
        "Key": "raw/0001.xml",
        "Body": "<xml>raw</xml>",
        "ServerSideEncryption": "AES256",
    }
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT accession, nameOfIssuer, cusip, value, sshPrnamt FROM holdings"
    ).fetchall()
    conn.close()
    assert rows == [
        ("0001", "CorpA", "AAA", 10, 1),
        ("0001", "CorpB", "BBB", 20, 2),
    ]


def test_daily_diff_flow_defaults_use_env_and_yesterday(tmp_path, monkeypatch):
    monkeypatch.setattr(daily_flow.dt, "date", FixedDate)
    monkeypatch.setenv("CIK_LIST", "1,2")
    monkeypatch.setenv("DB_PATH", str(tmp_path / "dev.db"))
    calls = []

    def fake_compute(cik, date_value, db_path):
        calls.append((cik, date_value, db_path))

    # Capture inputs without touching the DB.
    monkeypatch.setattr(daily_flow, "compute", fake_compute)

    daily_flow.daily_diff_flow.fn(cik_list=None, date=None)

    assert calls == [
        ("1", "2024-05-01", str(tmp_path / "dev.db")),
        ("2", "2024-05-01", str(tmp_path / "dev.db")),
    ]


def test_compute_inserts_additions_and_exits(tmp_path, monkeypatch):
    def fake_diff_holdings(cik, db_path):
        return {"AAA"}, {"BBB"}

    # Mock diffing to exercise both add/exit inserts.
    monkeypatch.setattr(daily_flow, "diff_holdings", fake_diff_holdings)
    db_path = tmp_path / "dev.db"

    daily_flow.compute.fn("0000", "2024-05-01", str(db_path))

    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT cik, cusip, change FROM daily_diff ORDER BY change").fetchall()
    conn.close()
    assert rows == [
        ("0000", "AAA", "ADD"),
        ("0000", "BBB", "EXIT"),
    ]


@pytest.mark.asyncio
async def test_summarise_posts_to_slack_when_webhook_set(tmp_path, monkeypatch):
    db_file = tmp_path / "dev.db"
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE daily_diff (date TEXT, cik TEXT, cusip TEXT, change TEXT)")
    conn.execute(
        "INSERT INTO daily_diff VALUES (?,?,?,?)",
        ("2024-01-02", "1", "AAA", "ADD"),
    )
    conn.commit()
    conn.close()
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://example.test/webhook")
    calls = {}

    @asynccontextmanager
    async def fake_tracked_call(source, endpoint):
        calls["source"] = source
        calls["endpoint"] = endpoint

        def log(resp):
            calls["status"] = resp.status_code

        yield log

    class DummyResp:
        status_code = 200
        content = b"ok"

    # Stub outbound HTTP while still logging usage.
    monkeypatch.setattr(summariser_flow, "tracked_call", fake_tracked_call)
    monkeypatch.setattr(summariser_flow.requests, "post", lambda *_args, **_kw: DummyResp())

    result = await summariser_flow.summarise.fn("2024-01-02")

    assert result == "1 changes on 2024-01-02"
    assert calls == {
        "source": "slack",
        "endpoint": "https://example.test/webhook",
        "status": 200,
    }


@pytest.mark.asyncio
async def test_summarise_skips_webhook_when_unset(tmp_path, monkeypatch):
    db_file = tmp_path / "dev.db"
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE daily_diff (date TEXT, cik TEXT, cusip TEXT, change TEXT)")
    conn.executemany(
        "INSERT INTO daily_diff VALUES (?,?,?,?)",
        [
            ("2024-01-02", "1", "AAA", "ADD"),
            ("2024-01-02", "2", "BBB", "EXIT"),
        ],
    )
    conn.commit()
    conn.close()
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)

    # Guard against accidental outbound HTTP when webhook is unset.
    def fail_post(*_args, **_kwargs):
        raise AssertionError("requests.post should not be called")

    monkeypatch.setattr(summariser_flow.requests, "post", fail_post)

    result = await summariser_flow.summarise.fn("2024-01-02")

    assert result == "2 changes on 2024-01-02"


@pytest.mark.asyncio
async def test_summariser_flow_defaults_to_yesterday(monkeypatch):
    monkeypatch.setattr(summariser_flow.dt, "date", FixedDate)
    seen = {}

    async def fake_summarise(date_value):
        seen["date"] = date_value
        return "ok"

    # Avoid database access while validating the default date logic.
    monkeypatch.setattr(summariser_flow, "summarise", fake_summarise)

    result = await summariser_flow.summariser_flow.fn()

    assert result == "ok"
    assert seen["date"] == "2024-05-01"
