import sqlite3
import sys
import types
from contextlib import closing
from pathlib import Path

import httpx
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from adapters import base
from adapters.base import get_adapter


def test_get_adapter_returns_module():
    adapter = get_adapter("edgar")
    assert isinstance(adapter, types.ModuleType)
    assert hasattr(adapter, "list_new_filings")


def test_get_adapter_returns_non_us_uk_modules():
    for name in ("canada", "mas", "asic"):
        adapter = get_adapter(name)
        assert isinstance(adapter, types.ModuleType)
        assert hasattr(adapter, "list_new_filings")
        assert hasattr(adapter, "download")
        assert hasattr(adapter, "parse")


@pytest.mark.asyncio
@pytest.mark.parametrize("request_fails", [False, True])
async def test_tracked_call_unread_stream_preserves_request_outcome(
    monkeypatch, caplog, request_fails
):
    def unexpected_connect(*_args, **_kwargs):
        pytest.fail("Unread response metrics must not open a database connection")

    monkeypatch.setattr(base, "connect_db", unexpected_connect)
    request_error = RuntimeError("adapter request failed")
    with closing(httpx.Response(200, stream=httpx.ByteStream(b"payload"))) as response:

        async def request():
            async with base.tracked_call("edgar", "/filings") as log:
                log(response)
                if request_fails:
                    raise request_error
            return response

        if request_fails:
            with pytest.raises(RuntimeError) as caught:
                await request()
            assert caught.value is request_error
        else:
            assert await request() is response
        assert not response.is_stream_consumed

    warnings = [
        record
        for record in caplog.records
        if record.getMessage() == "Failed to record API usage metrics"
    ]
    assert len(warnings) == 1
    assert warnings[0].source == "edgar"
    assert warnings[0].endpoint == "/filings"
    assert isinstance(warnings[0].exc_info[1], httpx.ResponseNotRead)


@pytest.mark.asyncio
@pytest.mark.parametrize("request_fails", [False, True])
async def test_tracked_call_close_failure_preserves_request_outcome(
    monkeypatch, caplog, tmp_path, request_fails
):
    close_error = sqlite3.OperationalError("metrics connection close failed")
    close_attempts = []

    class CloseFailureConnection(sqlite3.Connection):
        def close(self):
            close_attempts.append(self)
            super().close()
            raise close_error

    db_path = tmp_path / "usage.db"
    conn = sqlite3.connect(db_path, factory=CloseFailureConnection)
    monkeypatch.setattr(base, "connect_db", lambda _path: conn)
    response = httpx.Response(201, content=b"abc")
    request_error = RuntimeError("adapter request failed")

    async def request():
        async with base.tracked_call("edgar", "/filings", cost_usd=1.25) as log:
            log(response)
            if request_fails:
                raise request_error
        return response

    if request_fails:
        with pytest.raises(RuntimeError) as caught:
            await request()
        assert caught.value is request_error
    else:
        assert await request() is response

    assert close_attempts == [conn]
    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        conn.execute("SELECT 1")
    with closing(sqlite3.connect(db_path)) as reader:
        assert reader.execute(
            "SELECT source, endpoint, status, bytes, cost_usd FROM api_usage"
        ).fetchall() == [("edgar", "/filings", 201, 3, 1.25)]
    warnings = [
        record
        for record in caplog.records
        if record.getMessage() == "Failed to close API usage metrics connection"
    ]
    assert len(warnings) == 1
    assert warnings[0].source == "edgar"
    assert warnings[0].endpoint == "/filings"
    assert warnings[0].exc_info[1] is close_error
