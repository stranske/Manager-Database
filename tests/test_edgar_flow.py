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
