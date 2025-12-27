import sqlite3
from pathlib import Path

import pytest

from adapters.base import tracked_call


class DummyResp:
    def __init__(self, status_code=200, content=b"ok"):
        self.status_code = status_code
        self.content = content


@pytest.mark.asyncio
async def test_tracked_call_writes(tmp_path: Path):
    db_path = tmp_path / "dev.db"
    async with tracked_call("test", "http://x", db_path=str(db_path)) as log:
        log(DummyResp())
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT source, endpoint, status FROM api_usage").fetchone()
    view_row = conn.execute("SELECT month, source, calls FROM monthly_usage").fetchone()
    conn.close()
    assert row == ("test", "http://x", 200)
    assert view_row[1:] == ("test", 1)
