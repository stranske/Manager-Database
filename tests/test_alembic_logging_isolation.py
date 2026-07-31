"""Running a migration must not disable loggers that already exist.

`alembic/env.py` calls `logging.config.fileConfig`, whose `disable_existing_loggers`
argument defaults to True. With the default, any test that runs `command.upgrade`
switches off every logger created before it for the rest of the pytest session, so a
later test asserting on log output fails or passes purely by collection order. That is
how `tests/test_backtest_flow.py::test_missing_price_skips_position_without_crashing`
fails when it runs after `tests/test_schema.py` but passes on its own.
"""

from __future__ import annotations

import logging
from pathlib import Path

from alembic.config import Config

from alembic import command

ROOT = Path(__file__).resolve().parents[1]


def _alembic_config(url: str) -> Config:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    config.set_main_option("sqlalchemy.url", url)
    return config


class _RecordingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def test_migration_does_not_disable_preexisting_loggers(monkeypatch, tmp_path):
    """A logger created before a migration is still enabled and still emits after it.

    The probe carries its own handler rather than using `caplog`, because `fileConfig`
    always rebuilds the root logger's handlers. The regression being guarded is the
    `disabled` flag that `disable_existing_loggers=True` sets on every existing logger.
    """
    monkeypatch.delenv("DB_URL", raising=False)
    logger = logging.getLogger("tests.alembic_logging_isolation.probe")
    handler = _RecordingHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)
    try:
        logger.warning("before migration")
        assert handler.messages == ["before migration"]

        command.upgrade(_alembic_config(f"sqlite:///{tmp_path / 'migrated.db'}"), "head")

        assert logger.disabled is False, (
            "alembic env.py disabled an existing logger; pass "
            "disable_existing_loggers=False to fileConfig"
        )
        assert logger.isEnabledFor(logging.WARNING)

        logger.warning("after migration")
        assert handler.messages == ["before migration", "after migration"]
    finally:
        logger.removeHandler(handler)
