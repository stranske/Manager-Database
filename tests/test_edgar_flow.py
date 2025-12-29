import sqlite3

import pytest

import etl.edgar_flow as flow


class DummyAdapter:
    async def list_new_filings(self, cik, since):
        return [{"accession": "1", "cik": cik, "filed": "2024-05-01"}]

    async def download(self, filing):
        return "<xml></xml>"

    async def parse(self, raw):
        return [{"nameOfIssuer": "Corp", "cusip": "AAA", "value": 1, "sshPrnamt": 1}]


class MultiFilingAdapter:
    async def list_new_filings(self, cik, since):
        return [
            {"accession": "1", "cik": cik, "filed": "2024-05-01"},
            {"accession": "2", "cik": cik, "filed": "2024-05-02"},
        ]

    async def download(self, filing):
        return f"<xml accession='{filing['accession']}'></xml>"

    async def parse(self, raw):
        return [{"nameOfIssuer": "Corp", "cusip": "AAA", "value": 1, "sshPrnamt": 1}]


class EmptyFilingAdapter:
    async def list_new_filings(self, cik, since):
        return []

    async def download(self, filing):
        raise AssertionError("download should not be called for empty filings")

    async def parse(self, raw):
        raise AssertionError("parse should not be called for empty filings")


@pytest.mark.nightly
@pytest.mark.asyncio
async def test_fetch_and_store_encryption(monkeypatch, tmp_path):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "ADAPTER", DummyAdapter())

    calls = {}

    def put_object(**kwargs):
        calls.update(kwargs)

    monkeypatch.setattr(flow.S3, "put_object", put_object)

    await flow.fetch_and_store("0", "2024-01-01")

    assert calls.get("ServerSideEncryption") == "AES256"
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT cik, cusip FROM holdings").fetchone()
    conn.close()
    assert row == ("0", "AAA")


@pytest.mark.asyncio
async def test_fetch_and_store_inserts_multiple_filings(monkeypatch, tmp_path):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "ADAPTER", MultiFilingAdapter())

    put_calls = []

    def put_object(**kwargs):
        put_calls.append(kwargs)

    stored = []

    def record_document(raw):
        stored.append(raw)

    monkeypatch.setattr(flow.S3, "put_object", put_object)
    monkeypatch.setattr(flow, "store_document", record_document)

    # Use the underlying function to avoid spinning up the Prefect engine.
    results = await flow.fetch_and_store.fn("0", "2024-01-01")

    assert len(results) == 2
    assert len(put_calls) == 2
    assert stored == ["<xml accession='1'></xml>", "<xml accession='2'></xml>"]


@pytest.mark.asyncio
async def test_fetch_and_store_handles_empty_filings(monkeypatch, tmp_path):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "ADAPTER", EmptyFilingAdapter())

    put_calls = []
    stored = []

    def put_object(**kwargs):
        put_calls.append(kwargs)

    def record_document(raw):
        stored.append(raw)

    monkeypatch.setattr(flow.S3, "put_object", put_object)
    monkeypatch.setattr(flow, "store_document", record_document)

    # Ensure empty filings do not trigger storage or embedding side effects.
    results = await flow.fetch_and_store.fn("0", "2024-01-01")

    assert results == []
    assert put_calls == []
    assert stored == []
    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM holdings").fetchone()[0]
    conn.close()
    assert count == 0


@pytest.mark.asyncio
async def test_edgar_flow_skips_userwarning_and_writes_json(monkeypatch, tmp_path):
    captured = {"since": None}

    async def fake_fetch_and_store(cik, since):
        if cik == "bad":
            raise UserWarning("not a filer")
        captured["since"] = since  # Ensure we assert the default "since" value.
        return [{"nameOfIssuer": "Corp", "cusip": "AAA", "value": 1, "sshPrnamt": 1}]

    monkeypatch.setattr(flow, "fetch_and_store", fake_fetch_and_store)
    monkeypatch.setattr(flow, "RAW_DIR", tmp_path)

    # Use the underlying function to avoid spinning up the Prefect engine.
    rows = await flow.edgar_flow.fn(cik_list=["ok", "bad"])

    assert rows == [
        {"nameOfIssuer": "Corp", "cusip": "AAA", "value": 1, "sshPrnamt": 1}
    ]
    assert captured["since"] == "1970-01-01"
    assert (tmp_path / "parsed.json").exists()


@pytest.mark.asyncio
async def test_edgar_flow_default_ciks(monkeypatch, tmp_path):
    seen = []

    async def fake_fetch_and_store(cik, since):
        seen.append(cik)
        return []

    monkeypatch.setenv("CIK_LIST", "0001,0002")
    monkeypatch.setattr(flow, "fetch_and_store", fake_fetch_and_store)
    monkeypatch.setattr(flow, "RAW_DIR", tmp_path)

    # Use the underlying function to avoid spinning up the Prefect engine.
    await flow.edgar_flow.fn(cik_list=None, since="2024-01-01")

    assert seen == ["0001", "0002"]
