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
