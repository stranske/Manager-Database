"""Pytest configuration for Manager-Database tests.

This conftest.py automatically skips tests marked as `nightly` unless
explicitly requested via `-m nightly` or `--run-nightly`.
"""

from contextlib import AsyncExitStack
from typing import Any, cast

import pytest

from api.chat import app as chat_app


def _install_router_lifespan_shim() -> None:
    """Restore the legacy router startup/shutdown hooks expected by older tests."""

    router = cast(Any, chat_app.router)

    if hasattr(router, "startup") and hasattr(router, "shutdown"):
        return

    async def _startup() -> None:
        stack = getattr(router, "_codex_lifespan_stack", None)
        if stack is not None:
            return
        stack = AsyncExitStack()
        await stack.enter_async_context(chat_app.router.lifespan_context(chat_app))
        router._codex_lifespan_stack = stack

    async def _shutdown() -> None:
        stack = getattr(router, "_codex_lifespan_stack", None)
        if stack is None:
            return
        router._codex_lifespan_stack = None
        await stack.aclose()

    router.startup = _startup
    router.shutdown = _shutdown


_install_router_lifespan_shim()


def pytest_configure(config):
    """Register the nightly marker and configure auto-skip."""
    config.addinivalue_line(
        "markers", "nightly: mark test as nightly regression test (skipped by default)"
    )


def pytest_collection_modifyitems(config, items):
    """Skip nightly tests unless explicitly requested."""
    # Check if nightly tests are explicitly requested
    if config.getoption("-m") and "nightly" in config.getoption("-m"):
        return  # User explicitly asked for nightly tests

    # Check for custom flag
    if hasattr(config.option, "run_nightly") and config.option.run_nightly:
        return

    skip_nightly = pytest.mark.skip(
        reason="Nightly test skipped (use -m nightly or --run-nightly to run)"
    )
    for item in items:
        if "nightly" in item.keywords:
            item.add_marker(skip_nightly)


def pytest_addoption(parser):
    """Add custom command line option for running nightly tests."""
    parser.addoption(
        "--run-nightly",
        action="store_true",
        default=False,
        help="Run nightly tests",
    )


@pytest.fixture(params=[False, True], ids=["tuple-rows", "mapping-rows"])
def same_day_amendment_db(request):
    """Seed an amendment with a lower ID that becomes known after its original."""
    import sqlite3

    conn = sqlite3.connect(":memory:")
    if request.param:
        conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE filings (
            filing_id INTEGER PRIMARY KEY, manager_id INTEGER, type TEXT,
            period_end TEXT, filed_date TEXT
        );
        CREATE TABLE holdings (
            holding_id INTEGER PRIMARY KEY, filing_id INTEGER, cusip TEXT,
            name_of_issuer TEXT, shares INTEGER, value_usd REAL,
            knowledge_time TEXT, superseded_at TEXT
        );
        INSERT INTO filings VALUES
            (1, 1, '13F-HR/A', '2024-03-31', '2024-05-15'),
            (2, 1, '13F-HR', '2024-03-31', '2024-05-15'),
            (3, 1, '13F-HR', '2023-12-31', '2024-02-15');
        INSERT INTO holdings VALUES
            (1, 1, 'AMENDED', 'Amended', 20, 200, '2024-06-01T12:00:00Z', NULL),
            (2, 2, 'ORIGINAL', 'Original', 10, 100, '2024-05-15T12:00:00Z', NULL),
            (3, 3, 'PRIOR', 'Prior', 5, 50, '2024-02-15T12:00:00Z', NULL);
    """)
    try:
        yield conn
    finally:
        conn.close()
