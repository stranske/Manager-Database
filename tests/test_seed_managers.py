from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_seed_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "seed_managers.py"
    spec = importlib.util.spec_from_file_location("seed_managers_module", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeCursor:
    def __init__(self, rows_by_cik: dict[str, dict[str, object]]) -> None:
        self._rows_by_cik = rows_by_cik
        self._last_row: tuple[bool] | None = None

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, query: str, params: tuple[object, ...]) -> None:
        assert "ON CONFLICT (cik) WHERE cik IS NOT NULL DO UPDATE" in query
        cik = str(params[1])
        inserted = cik not in self._rows_by_cik
        self._rows_by_cik[cik] = {
            "name": params[0],
            "aliases": params[2],
            "jurisdictions": params[3],
            "tags": params[4],
        }
        self._last_row = (inserted,)

    def fetchone(self) -> tuple[bool] | None:
        return self._last_row


class _FakeConnection:
    def __init__(self, rows_by_cik: dict[str, dict[str, object]], commits: list[int]) -> None:
        self._rows_by_cik = rows_by_cik
        self._commits = commits

    def __enter__(self) -> _FakeConnection:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._rows_by_cik)

    def commit(self) -> None:
        self._commits.append(1)


def test_seed_managers_is_idempotent(monkeypatch) -> None:
    sm = _load_seed_module()
    rows_by_cik: dict[str, dict[str, object]] = {}
    commits: list[int] = []

    def _fake_connect(_db_url: str) -> _FakeConnection:
        return _FakeConnection(rows_by_cik, commits)

    monkeypatch.setattr(sm.psycopg, "connect", _fake_connect)
    monkeypatch.setenv("DB_URL", "postgresql://example:example@localhost:5432/postgres")

    first_inserted = sm.seed_managers()
    second_inserted = sm.seed_managers()

    assert first_inserted == len(sm.SEED_MANAGERS)
    assert second_inserted == 0
    assert len(rows_by_cik) == len(sm.SEED_MANAGERS)
    assert len(commits) == 2
