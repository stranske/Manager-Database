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
