import asyncio
import logging
import sqlite3
import sys
from pathlib import Path
from typing import Any, cast

import httpx
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.chat import app


async def _post_bulk_json(payload: object | None):
    # Use ASGI transport to avoid spinning up a server for bulk import tests.
    await cast(Any, app.router).startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, app))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            if payload is None:
                return await client.post("/api/managers/bulk")
            return await client.post("/api/managers/bulk", json=payload)
    finally:
        await cast(Any, app.router).shutdown()


async def _post_bulk_raw(
    contents: bytes,
    *,
    content_type: str,
    headers: dict[str, str] | None = None,
):
    """Post exact request bytes so malformed-input boundaries stay observable."""
    await cast(Any, app.router).startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, app))
        request_headers = {"content-type": content_type, **(headers or {})}
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.post(
                "/api/managers/bulk", content=contents, headers=request_headers
            )
    finally:
        await cast(Any, app.router).shutdown()


async def _post_bulk_csv(contents: str):
    # Post raw CSV payloads with the text/csv content type.
    await cast(Any, app.router).startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, app))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.post(
                "/api/managers/bulk",
                content=contents,
                headers={"content-type": "text/csv"},
            )
    finally:
        await cast(Any, app.router).shutdown()


def test_bulk_json_imports_valid_records(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {"name": "Manager A", "jurisdictions": ["us"]},
        {"name": "", "jurisdictions": ["us"]},
        {"name": "Manager B", "jurisdictions": ["uk"], "tags": ["quant"]},
    ]

    resp = asyncio.run(_post_bulk_json(payloads))
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 3
    assert body["succeeded"] == 2
    assert body["failed"] == 1
    assert {item["index"] for item in body["successes"]} == {0, 2}
    assert body["failures"][0]["index"] == 1

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT name, jurisdictions, tags FROM managers ORDER BY id").fetchall()
    finally:
        conn.close()
    assert rows == [
        ("Manager A", '["us"]', "[]"),
        ("Manager B", '["uk"]', '["quant"]'),
    ]


def test_bulk_json_rejects_duplicate_cik_within_request(tmp_path, monkeypatch):
    import api.managers as managers_api

    monkeypatch.setenv("DB_PATH", str(tmp_path / "dev.db"))
    inserted_names = []
    original_insert = managers_api._insert_manager

    def track_insert(conn, payload, *, commit=True):
        inserted_names.append(payload.name)
        return original_insert(conn, payload, commit=commit)

    monkeypatch.setattr(managers_api, "_insert_manager", track_insert)

    resp = asyncio.run(
        _post_bulk_json(
            [
                {"name": "Original", "cik": "0001791786"},
                {"name": "Duplicate", "cik": "0001791786"},
            ]
        )
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["succeeded"] == 1
    assert body["failed"] == 1
    assert body["successes"][0]["index"] == 0
    assert body["failures"] == [
        {
            "index": 1,
            "errors": [{"field": "cik", "message": "A manager with this CIK already exists."}],
        }
    ]
    assert inserted_names == ["Original"]


def test_bulk_json_rejects_cik_already_in_database(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "dev.db"))
    payload = {"name": "Original", "cik": "0001791786"}
    assert asyncio.run(_post_bulk_json([payload])).status_code == 200

    resp = asyncio.run(_post_bulk_json([{**payload, "name": "Duplicate"}]))

    assert resp.status_code == 200
    body = resp.json()
    assert body["succeeded"] == 0
    assert body["failed"] == 1
    assert body["failures"][0]["errors"][0]["field"] == "cik"


@pytest.mark.parametrize("source", ["json", "csv"])
def test_bulk_import_invalid_record_does_not_reserve_cik(tmp_path, monkeypatch, source):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    assert (
        asyncio.run(_post_bulk_json([{"name": "Existing", "cik": "0001791786"}])).status_code == 200
    )
    records = [
        {"name": "", "cik": "0001791787"},
        {"name": "Valid", "cik": " 0001791787 "},
        {"name": "Request duplicate", "cik": "0001791787"},
        {"name": "Database duplicate", "cik": "0001791786"},
        {"name": "Other", "cik": "0001791788"},
    ]
    if source == "json":
        resp = asyncio.run(_post_bulk_json(records))
    else:
        csv_payload = "name,cik\n" + "".join(
            f"{record['name']},{record['cik']}\n" for record in records
        )
        resp = asyncio.run(_post_bulk_csv(csv_payload))

    assert resp.status_code == 200
    body = resp.json()
    assert (body["total"], body["succeeded"], body["failed"]) == (5, 2, 3)
    assert [item["index"] for item in body["successes"]] == [1, 4]
    failures = {item["index"]: item["errors"] for item in body["failures"]}
    assert set(failures) == {0, 2, 3}
    assert failures[0][0]["field"] == "name"
    for index in (2, 3):
        assert failures[index] == [
            {"field": "cik", "message": "A manager with this CIK already exists."}
        ]
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT name, cik FROM managers ORDER BY id").fetchall() == [
            ("Existing", "0001791786"),
            ("Valid", "0001791787"),
            ("Other", "0001791788"),
        ]
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint failed: managers.cik"):
            conn.execute(
                "INSERT INTO managers(name, cik) VALUES (?, ?)", ("Direct duplicate", "0001791787")
            )


@pytest.mark.parametrize("source", ["json", "csv"])
def test_bulk_import_enforces_cik_uniqueness_when_all_records_conflict(
    tmp_path, monkeypatch, source
):
    import api.managers as managers_api

    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    # A legacy database may contain managers without a CIK uniqueness constraint.
    with sqlite3.connect(db_path) as conn:
        managers_api._ensure_manager_table(conn)
        conn.execute("INSERT INTO managers(name, cik) VALUES (?, ?)", ("Original", "0001791786"))

    if source == "json":
        resp = asyncio.run(
            _post_bulk_json(
                [
                    {"name": "Duplicate A", "cik": "0001791786"},
                    {"name": "Duplicate B", "cik": " 0001791786 "},
                ]
            )
        )
    else:
        resp = asyncio.run(
            _post_bulk_csv("name,cik\nDuplicate A,0001791786\nDuplicate B, 0001791786 \n")
        )

    assert resp.status_code == 200
    assert resp.json() == {
        "total": 2,
        "succeeded": 0,
        "failed": 2,
        "successes": [],
        "failures": [
            {
                "index": index,
                "errors": [{"field": "cik", "message": "A manager with this CIK already exists."}],
            }
            for index in (0, 1)
        ],
    }
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT name, cik FROM managers").fetchall() == [
            ("Original", "0001791786")
        ]
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint failed: managers.cik"):
            conn.execute(
                "INSERT INTO managers(name, cik) VALUES (?, ?)", ("Direct duplicate", "0001791786")
            )


@pytest.mark.parametrize("source", ["json", "csv"])
def test_bulk_import_reports_legacy_duplicate_ciks_without_changing_rows(
    tmp_path, monkeypatch, source
):
    import api.managers as managers_api

    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    original_rows = [("Legacy A", "0001791786"), ("Legacy B", "0001791786")]
    with sqlite3.connect(db_path) as conn:
        managers_api._ensure_manager_table(conn)
        conn.executemany("INSERT INTO managers(name, cik) VALUES (?, ?)", original_rows)

    queries = []

    def connect_traced_db():
        conn = sqlite3.connect(db_path)
        conn.set_trace_callback(queries.append)
        return conn

    monkeypatch.setattr(managers_api, "connect_db", connect_traced_db)
    if source == "json":
        resp = asyncio.run(
            _post_bulk_json(
                [
                    {"name": "Duplicate", "cik": "0001791786"},
                    {"name": "New", "cik": "0001791787"},
                ]
            )
        )
    else:
        resp = asyncio.run(_post_bulk_csv("name,cik\nDuplicate,0001791786\nNew,0001791787\n"))

    assert resp.status_code == 409
    errors = [
        {
            "field": "cik",
            "message": "Existing manager records share a CIK; clean up duplicate CIKs before importing managers.",
        }
    ]
    assert resp.json() == {"errors": errors, "error": errors}
    assert any("GROUP BY cik HAVING COUNT(*) > 1" in query for query in queries)
    assert not any("CREATE UNIQUE INDEX" in query for query in queries)
    assert not any(query.startswith("INSERT INTO managers") for query in queries)
    with sqlite3.connect(db_path) as conn:
        assert (
            conn.execute("SELECT name, cik FROM managers ORDER BY id").fetchall() == original_rows
        )
        # Once the duplicate owner is resolved, imports can create the index and resume.
        conn.execute("UPDATE managers SET cik = NULL WHERE name = 'Legacy B'")

    resp = asyncio.run(_post_bulk_json([{"name": "New", "cik": "0001791787"}]))
    assert resp.status_code == 200
    assert resp.json()["succeeded"] == 1
    queries.clear()
    resp = asyncio.run(_post_bulk_json([{"name": "Other", "cik": "0001791788"}]))
    assert resp.status_code == 200
    assert resp.json()["succeeded"] == 1
    assert not any("GROUP BY cik" in query for query in queries)
    with sqlite3.connect(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint failed: managers.cik"):
            conn.execute(
                "INSERT INTO managers(name, cik) VALUES (?, ?)", ("Duplicate", "0001791787")
            )


def test_bulk_import_detects_postgres_legacy_duplicates_before_index_creation(monkeypatch):
    import api.managers as managers_api

    class PostgresConn:
        def __init__(self):
            self.events = []
            self.last_sql = ""

        def execute(self, sql):
            self.events.append(sql)
            self.last_sql = sql
            assert "CREATE UNIQUE INDEX" not in sql
            return self

        def fetchone(self):
            return (1,) if "GROUP BY cik HAVING COUNT(*) > 1" in self.last_sql else None

        def rollback(self):
            self.events.append("rollback")

        def close(self):
            self.events.append("close")

    conn = PostgresConn()
    monkeypatch.setattr(managers_api, "connect_db", lambda: conn)
    resp = asyncio.run(_post_bulk_json([{"name": "New", "cik": "0001791787"}]))

    assert resp.status_code == 409
    assert resp.json()["errors"][0]["field"] == "cik"
    assert "clean up duplicate CIKs" in resp.json()["errors"][0]["message"]
    assert any("FROM pg_indexes" in event for event in conn.events)
    assert conn.events[-2:] == ["rollback", "close"]


@pytest.mark.parametrize("constraint", ["idx_managers_cik_unique", "idx_managers_lei_unique"])
def test_bulk_import_postgres_schema_conflict_requires_cik_cleanup(monkeypatch, constraint):
    from types import SimpleNamespace

    psycopg = pytest.importorskip("psycopg")
    import api.managers as managers_api

    class UniqueViolation(psycopg.errors.UniqueViolation):
        @property
        def diag(self):
            return SimpleNamespace(constraint_name=constraint)

    events = []
    conn = SimpleNamespace(
        rollback=lambda: events.append("rollback"),
        close=lambda: events.append("close"),
    )
    monkeypatch.setattr(managers_api, "connect_db", lambda: conn)

    def fail_schema(_conn, **_kwargs):
        raise UniqueViolation("simulated legacy duplicates")

    monkeypatch.setattr(managers_api, "_ensure_universe_schema", fail_schema)
    resp = asyncio.run(_post_bulk_json([{"name": "New", "cik": "0001791787"}]))

    assert events == ["rollback", "close"]
    if constraint == "idx_managers_cik_unique":
        assert resp.status_code == 409
        assert resp.json()["errors"][0]["field"] == "cik"
        assert "clean up duplicate CIKs" in resp.json()["errors"][0]["message"]
    else:
        assert resp.status_code == 503
        assert resp.json()["detail"] == "Database unavailable"


@pytest.mark.parametrize("conflict_index", [0, 1, 2])
def test_bulk_json_handles_cik_insert_race_per_record(tmp_path, monkeypatch, conflict_index):
    import api.managers as managers_api

    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [{"name": f"Manager {index}", "cik": f"{index + 1:010d}"} for index in range(3)]
    original_filter = managers_api._exclude_conflicting_bulk_ciks

    def insert_competing_manager(conn, records):
        result = original_filter(conn, records)
        # Another connection wins the race after validation but before insertion.
        with sqlite3.connect(db_path) as competing_conn:
            competing_conn.execute(
                "INSERT INTO managers(name, cik) VALUES (?, ?)",
                ("Competing manager", payloads[conflict_index]["cik"]),
            )
        return result

    monkeypatch.setattr(managers_api, "_exclude_conflicting_bulk_ciks", insert_competing_manager)

    resp = asyncio.run(_post_bulk_json(payloads))

    assert resp.status_code == 200
    body = resp.json()
    assert body["succeeded"] == 2
    assert body["failed"] == 1
    assert [item["index"] for item in body["successes"]] == [
        index for index in range(3) if index != conflict_index
    ]
    assert body["failures"] == [
        {
            "index": conflict_index,
            "errors": [{"field": "cik", "message": "A manager with this CIK already exists."}],
        }
    ]
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT name, cik FROM managers ORDER BY cik").fetchall() == [
            (
                "Competing manager" if index == conflict_index else payload["name"],
                payload["cik"],
            )
            for index, payload in enumerate(payloads)
        ]


@pytest.mark.parametrize("source", ["json", "csv"])
def test_bulk_import_rolls_back_successes_after_cik_race(tmp_path, monkeypatch, source):
    import api.managers as managers_api

    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    records = [
        {"name": "Before", "cik": "0000000001"},
        {"name": "Conflict", "cik": "0000000002"},
        {"name": "After", "cik": "0000000003"},
        {"name": "Database failure", "cik": "0000000004"},
    ]
    original_filter = managers_api._exclude_conflicting_bulk_ciks
    original_insert = managers_api._insert_manager
    attempted_names = []

    def insert_competing_manager(conn, payloads):
        result = original_filter(conn, payloads)
        with sqlite3.connect(db_path) as competing_conn:
            competing_conn.execute(
                "INSERT INTO managers(name, cik) VALUES (?, ?)", ("Competing", "0000000002")
            )
        return result

    def fail_last_insert(conn, payload, *, commit=True):
        attempted_names.append(payload.name)
        if payload.name == "Database failure":
            raise sqlite3.OperationalError("simulated failure after CIK conflict recovery")
        return original_insert(conn, payload, commit=commit)

    monkeypatch.setattr(managers_api, "_exclude_conflicting_bulk_ciks", insert_competing_manager)
    monkeypatch.setattr(managers_api, "_insert_manager", fail_last_insert)
    if source == "json":
        resp = asyncio.run(_post_bulk_json(records))
    else:
        contents = "name,cik\n" + "".join(f"{row['name']},{row['cik']}\n" for row in records)
        resp = asyncio.run(_post_bulk_csv(contents))

    assert resp.status_code == 503
    assert resp.json()["detail"] == "Database unavailable"
    # The CIK conflict must recover before the unrelated failure aborts the batch.
    assert attempted_names == [row["name"] for row in records]
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT name, cik FROM managers ORDER BY id").fetchall() == [
            ("Competing", "0000000002")
        ]
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint failed: managers.cik"):
            conn.execute(
                "INSERT INTO managers(name, cik) VALUES (?, ?)", ("Duplicate", "0000000002")
            )


@pytest.mark.parametrize("constraint", ["idx_managers_cik_unique", "idx_managers_lei_unique"])
def test_bulk_import_postgres_recovers_only_cik_conflicts(monkeypatch, constraint):
    from types import SimpleNamespace

    psycopg = pytest.importorskip("psycopg")
    import api.managers as managers_api

    class UniqueViolation(psycopg.errors.UniqueViolation):
        @property
        def diag(self):
            return SimpleNamespace(constraint_name=constraint)

    class PostgresConn:
        autocommit = True
        aborted = False

        def __init__(self):
            self.events = []

        def execute(self, sql):
            assert self.autocommit is False
            if sql == "ROLLBACK TO SAVEPOINT bulk_manager_insert":
                assert self.aborted
                self.aborted = False
            else:
                assert not self.aborted
            self.events.append(sql)

        def commit(self):
            assert not self.aborted
            self.events.append("commit")

        def rollback(self):
            self.aborted = False
            self.events.append("rollback")

        def close(self):
            self.events.append("close")

    conn = PostgresConn()
    monkeypatch.setattr(managers_api, "connect_db", lambda: conn)
    monkeypatch.setattr(managers_api, "_ensure_universe_schema", lambda _, **_kwargs: None)
    monkeypatch.setattr(managers_api, "_exclude_conflicting_bulk_ciks", lambda _, rows: (rows, []))
    monkeypatch.setattr(managers_api, "invalidate_cache_prefix", lambda _: None)

    def insert(_conn, payload, *, commit=True):
        assert _conn is conn
        assert not conn.aborted
        assert not commit
        conn.events.append(payload.name)
        if payload.name == "Duplicate":
            conn.aborted = True
            raise UniqueViolation("simulated unique conflict")
        return int(payload.cik)

    monkeypatch.setattr(managers_api, "_insert_manager", insert)
    resp = asyncio.run(
        _post_bulk_json(
            [
                {"name": "Before", "cik": "0000000001"},
                {"name": "Duplicate", "cik": "0000000002"},
                {"name": "After", "cik": "0000000003"},
            ]
        )
    )

    assert conn.autocommit is True
    assert conn.events[-1] == "close"
    if constraint == "idx_managers_cik_unique":
        assert resp.status_code == 200
        assert [item["index"] for item in resp.json()["successes"]] == [0, 2]
        assert resp.json()["failures"] == [
            {
                "index": 1,
                "errors": [{"field": "cik", "message": "A manager with this CIK already exists."}],
            }
        ]
        assert conn.events == [
            "SAVEPOINT bulk_manager_insert",
            "Before",
            "RELEASE SAVEPOINT bulk_manager_insert",
            "SAVEPOINT bulk_manager_insert",
            "Duplicate",
            "ROLLBACK TO SAVEPOINT bulk_manager_insert",
            "RELEASE SAVEPOINT bulk_manager_insert",
            "SAVEPOINT bulk_manager_insert",
            "After",
            "RELEASE SAVEPOINT bulk_manager_insert",
            "commit",
            "close",
        ]
    else:
        assert resp.status_code == 503
        assert "After" not in conn.events
        assert "commit" not in conn.events
        assert conn.events[-2] == "rollback"


def test_bulk_json_normalizes_ciks_before_inserting(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    resp = asyncio.run(
        _post_bulk_json(
            [
                {"name": "Original", "cik": " 0001791786 "},
                {"name": "Duplicate", "cik": "0001791786"},
            ]
        )
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["succeeded"] == 1
    assert body["failed"] == 1
    assert body["successes"][0]["manager"]["cik"] == "0001791786"
    assert body["failures"] == [
        {
            "index": 1,
            "errors": [{"field": "cik", "message": "A manager with this CIK already exists."}],
        }
    ]
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT name, cik FROM managers").fetchall() == [
            ("Original", "0001791786")
        ]


@pytest.mark.parametrize("source", ["json", "csv"])
def test_bulk_import_allows_multiple_records_without_ciks(tmp_path, monkeypatch, source):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    if source == "json":
        resp = asyncio.run(
            _post_bulk_json(
                [
                    {"name": "Manager A", "cik": ""},
                    {"name": "Manager B", "cik": " "},
                    {"name": "Manager C", "cik": None},
                ]
            )
        )
    else:
        resp = asyncio.run(_post_bulk_csv("name,cik\nManager A,\nManager B, \nManager C,\n"))

    assert resp.status_code == 200
    body = resp.json()
    assert body["succeeded"] == 3
    assert body["failed"] == 0
    assert all(item["manager"]["cik"] is None for item in body["successes"])
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT name, cik FROM managers ORDER BY id").fetchall() == [
            ("Manager A", None),
            ("Manager B", None),
            ("Manager C", None),
        ]


@pytest.mark.parametrize("padding", [" ", "\t", "\n\r\v\f", "\u00a0\u3000", "\x1c\x85"])
def test_bulk_csv_reports_duplicate_ciks_and_imports_unique_records(tmp_path, monkeypatch, padding):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    assert (
        asyncio.run(_post_bulk_json([{"name": "Existing", "cik": "0001791786"}])).status_code == 200
    )
    # Legacy CIK whitespace must still participate in the duplicate lookup.
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE managers SET cik = ?", (f"{padding}0001791786{padding}",))

    resp = asyncio.run(
        _post_bulk_csv(
            "name,cik\nExisting duplicate,0001791786\n"
            "New,0001791787\nRequest duplicate, 0001791787 \nOther,0001791788\n"
        )
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 4
    assert body["succeeded"] == 2
    assert body["failed"] == 2
    assert [item["index"] for item in body["successes"]] == [1, 3]
    assert body["failures"] == [
        {
            "index": index,
            "errors": [{"field": "cik", "message": "A manager with this CIK already exists."}],
        }
        for index in (0, 2)
    ]
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT name, cik FROM managers ORDER BY id").fetchall() == [
            ("Existing", f"{padding}0001791786{padding}"),
            ("New", "0001791787"),
            ("Other", "0001791788"),
        ]
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint failed: managers.cik"):
            conn.execute(
                "INSERT INTO managers(name, cik) VALUES (?, ?)", ("Duplicate", "0001791787")
            )


def test_bulk_cik_checks_are_batched_and_use_expression_index():
    import api.managers as managers_api

    with sqlite3.connect(":memory:") as conn:
        managers_api._ensure_universe_schema(conn)
        cik = "0000000001"
        # Exercise every character in the payload's strip rule, not just spaces.
        whitespace = "".join(chr(code) for code in range(sys.maxunicode + 1) if chr(code).isspace())
        conn.execute(
            "INSERT INTO managers(name, cik) VALUES (?, ?)", ("Legacy", f"{whitespace}{cik}")
        )
        queries = []
        conn.set_trace_callback(queries.append)
        records = [
            (index, managers_api.ManagerCreate(name=f"Manager {index}", cik=f"{index + 1:010d}"))
            for index in range(901)
        ]
        records.append(
            (901, managers_api.ManagerCreate(name="Request duplicate", cik="0000000901"))
        )

        accepted, failures = managers_api._exclude_conflicting_bulk_ciks(conn, records)

        assert [index for index, _ in accepted] == list(range(1, 901))
        assert [failure.index for failure in failures] == [0, 901]
        lookups = [query for query in queries if query.startswith("SELECT DISTINCT TRIM(cik,")]
        assert len(lookups) == 2
        assert not any(query.startswith("SELECT 1 FROM managers") for query in queries)
        expression = managers_api._cik_lookup_expression(conn)
        plan = conn.execute(
            f"EXPLAIN QUERY PLAN SELECT {expression} FROM managers WHERE {expression} IN (?)",
            (cik,),
        ).fetchall()
        assert any("idx_managers_trimmed_cik" in row[-1] for row in plan)
        assert managers_api._manager_exists_for_cik(conn, f"\t{cik}\t")


def test_bulk_cik_checks_batch_postgres_queries():
    import api.managers as managers_api

    class PostgresConn:
        def __init__(self):
            self.queries = []

        def execute(self, sql, parameters):
            self.queries.append((sql, parameters))
            assert sql.count("%s") == len(parameters)
            assert "BTRIM(cik, chr(9)" in sql
            return self

        def fetchall(self):
            return [("0000000001",)] if len(self.queries) == 1 else []

    conn = PostgresConn()
    records = [
        (index, managers_api.ManagerCreate(name=f"Manager {index}", cik=f"{index + 1:010d}"))
        for index in range(901)
    ]
    accepted, failures = managers_api._exclude_conflicting_bulk_ciks(conn, records)

    assert [len(parameters) for _, parameters in conn.queries] == [900, 1]
    assert [index for index, _ in accepted] == list(range(1, 901))
    assert [failure.index for failure in failures] == [0]
    expression = managers_api._cik_lookup_expression(conn)
    assert (
        "".join((Path(__file__).parents[1] / "schema.sql").read_text().split()).find(
            "".join(expression.split())
        )
        != -1
    )


def test_bulk_json_import_persists_investment_manager_fields(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {
            "name": "Elliott Investment Management L.P.",
            "cik": "0001791786",
            "lei": "549300U3N12T57QLOU60",
            "aliases": ["Elliott Management"],
            "jurisdictions": ["us"],
            "tags": ["activist"],
            "registry_ids": {"fca_frn": "122927"},
        }
    ]

    resp = asyncio.run(_post_bulk_json(payloads))
    assert resp.status_code == 200
    body = resp.json()
    assert body["succeeded"] == 1
    assert body["failed"] == 0
    manager = body["successes"][0]["manager"]
    assert manager["name"] == payloads[0]["name"]
    assert manager["cik"] == payloads[0]["cik"]
    assert manager["lei"] == payloads[0]["lei"]
    assert manager["aliases"] == payloads[0]["aliases"]
    assert manager["jurisdictions"] == payloads[0]["jurisdictions"]
    assert manager["tags"] == payloads[0]["tags"]
    assert manager["registry_ids"] == payloads[0]["registry_ids"]

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT name, cik, lei, aliases, jurisdictions, tags, registry_ids FROM managers"
        ).fetchone()
    finally:
        conn.close()
    assert row == (
        "Elliott Investment Management L.P.",
        "0001791786",
        "549300U3N12T57QLOU60",
        '["Elliott Management"]',
        '["us"]',
        '["activist"]',
        '{"fca_frn": "122927"}',
    )


def test_bulk_csv_import_logs_invalid_rows(tmp_path, monkeypatch, caplog):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    csv_payload = "\n".join(
        [
            "name,cik,jurisdictions",
            'Manager A,0001791786,["us"]',
            "Missing Name,0001791787,[]",
            ',0001791786,["us"]',
        ]
    )

    with caplog.at_level(logging.WARNING, logger="api.managers"):
        resp = asyncio.run(_post_bulk_csv(csv_payload))

    assert resp.status_code == 200
    body = resp.json()
    assert body["succeeded"] == 2
    assert body["failed"] == 1
    assert "Bulk import CSV record missing required values" in caplog.text

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT name, cik, jurisdictions FROM managers ORDER BY id").fetchall()
    finally:
        conn.close()
    assert rows == [("Manager A", "0001791786", '["us"]'), ("Missing Name", "0001791787", "[]")]


def test_bulk_csv_import_parses_investment_manager_field_names(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    csv_payload = "\n".join(
        [
            "name,cik,lei,aliases,jurisdictions,tags,registry_ids",
            (
                "Elliott Investment Management L.P.,0001791786,549300U3N12T57QLOU60,"
                '"[""Elliott Management""]","[""us""]","[""activist""]","{""fca_frn"": ""122927""}"'
            ),
        ]
    )

    resp = asyncio.run(_post_bulk_csv(csv_payload))
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["succeeded"] == 1
    assert body["failed"] == 0
    manager = body["successes"][0]["manager"]
    assert manager["name"] == "Elliott Investment Management L.P."
    assert manager["cik"] == "0001791786"
    assert manager["lei"] == "549300U3N12T57QLOU60"
    assert manager["aliases"] == ["Elliott Management"]
    assert manager["jurisdictions"] == ["us"]
    assert manager["tags"] == ["activist"]
    assert manager["registry_ids"] == {"fca_frn": "122927"}


def test_bulk_csv_import_rejects_missing_headers(tmp_path, monkeypatch, caplog):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    csv_payload = "\n".join(
        [
            "cik,jurisdictions",
            '0001791786,["us"]',
        ]
    )

    with caplog.at_level(logging.WARNING, logger="api.managers"):
        resp = asyncio.run(_post_bulk_csv(csv_payload))

    assert resp.status_code == 400
    payload = resp.json()
    assert payload["errors"][0]["field"] == "body"
    assert "missing required headers" in payload["errors"][0]["message"].lower()
    assert "Bulk import CSV missing required headers" in caplog.text


def test_bulk_json_import_handles_large_batch(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [{"name": f"Manager {idx}", "jurisdictions": ["us"]} for idx in range(105)]

    resp = asyncio.run(_post_bulk_json(payloads))
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 105
    assert body["succeeded"] == 105
    assert body["failed"] == 0


def test_bulk_import_rejects_large_payload(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("BULK_IMPORT_MAX_BYTES", "50")
    payloads = [{"name": "X" * 100, "jurisdictions": ["us"]}]

    resp = asyncio.run(_post_bulk_json(payloads))

    assert resp.status_code == 413
    payload = resp.json()
    assert payload["errors"][0]["field"] == "body"
    assert "payload exceeds" in payload["errors"][0]["message"].lower()


def test_bulk_import_requires_payload(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    resp = asyncio.run(_post_bulk_json(None))
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["errors"][0]["field"] == "body"


def test_bulk_json_rejects_invalid_utf8_as_bad_request(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "dev.db"))

    resp = asyncio.run(_post_bulk_raw(b"\xff", content_type="application/json"))

    assert resp.status_code == 400
    assert resp.json()["errors"] == [
        {"field": "body", "message": "Request body must be valid JSON."}
    ]


def test_bulk_json_reports_non_object_and_invalid_records(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    resp = asyncio.run(
        _post_bulk_json(
            [
                "not-an-object",
                {"name": "Bad Types", "aliases": "not-a-list"},
                {"name": "Valid Manager", "jurisdictions": ["us"]},
            ]
        )
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 3
    assert body["succeeded"] == 1
    assert body["failed"] == 2
    assert [item["index"] for item in body["failures"]] == [0, 1]
    assert body["failures"][0]["errors"] == [
        {"field": "record", "message": "Record must be an object."}
    ]
    assert body["failures"][1]["errors"][0]["field"] == "aliases"

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT name FROM managers").fetchall()
    finally:
        conn.close()
    assert rows == [("Valid Manager",)]


def test_bulk_import_checks_actual_body_size_after_declared_length(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "dev.db"))
    monkeypatch.setenv("BULK_IMPORT_MAX_BYTES", "50")
    contents = b'[{"name":"' + (b"X" * 100) + b'"}]'

    resp = asyncio.run(
        _post_bulk_raw(
            contents,
            content_type="application/json",
            headers={"content-length": "1"},
        )
    )

    assert resp.status_code == 413
    assert "payload exceeds 50 bytes" in resp.json()["errors"][0]["message"].lower()


def test_bulk_csv_rejects_invalid_utf8(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "dev.db"))

    resp = asyncio.run(_post_bulk_raw(b"\xff", content_type="text/csv"))

    assert resp.status_code == 400
    assert resp.json()["errors"] == [
        {"field": "body", "message": "CSV payload must be UTF-8 encoded."}
    ]


def test_bulk_json_requires_an_array_body(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "dev.db"))

    resp = asyncio.run(_post_bulk_json({"name": "not-an-array"}))

    assert resp.status_code == 400
    assert resp.json()["errors"] == [
        {"field": "body", "message": "Request body must be a JSON array."}
    ]


def test_bulk_csv_with_only_empty_records_is_rejected(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "dev.db"))

    resp = asyncio.run(_post_bulk_csv("name,cik,tags\n,,\n"))

    assert resp.status_code == 400
    assert resp.json()["errors"] == [
        {"field": "body", "message": "No manager records were provided."}
    ]


@pytest.mark.parametrize("with_ciks", [False, True])
def test_bulk_import_rolls_back_on_mid_batch_failure(tmp_path, monkeypatch, with_ciks):
    import api.managers as managers_api

    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {"name": "Manager A", "jurisdictions": ["us"]},
        {"name": "Manager B", "jurisdictions": ["uk"]},
    ]
    if with_ciks:
        for index, payload in enumerate(payloads):
            payload["cik"] = f"{index + 1:010d}"
    original_insert = managers_api._insert_manager
    calls = {"count": 0}

    def fail_on_second(conn, payload, *, commit=True):
        calls["count"] += 1
        if calls["count"] >= 2:
            raise sqlite3.OperationalError("simulated mid-batch failure")
        return original_insert(conn, payload, commit=commit)

    monkeypatch.setattr(managers_api, "_insert_manager", fail_on_second)

    resp = asyncio.run(_post_bulk_json(payloads))

    assert resp.status_code == 503
    assert resp.json()["detail"] == "Database unavailable"
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute("SELECT name FROM managers").fetchall()
    finally:
        conn.close()
    assert row == []


@pytest.mark.parametrize("fail_insert", [False, True])
def test_bulk_import_postgres_transaction_state(monkeypatch, fail_insert):
    psycopg = pytest.importorskip("psycopg")
    import api.managers as managers_api

    events = []

    class PostgresConn:
        def __init__(self):
            self._autocommit = True

        @property
        def autocommit(self):
            return self._autocommit

        @autocommit.setter
        def autocommit(self, value):
            events.append(("autocommit", value))
            self._autocommit = value

        def commit(self):
            events.append(("commit",))

        def rollback(self):
            events.append(("rollback",))

        def close(self):
            events.append(("close",))

    conn = PostgresConn()
    monkeypatch.setattr(managers_api, "connect_db", lambda: conn)
    monkeypatch.setattr(managers_api, "_ensure_universe_schema", lambda _, **_kwargs: None)
    monkeypatch.setattr(managers_api, "invalidate_cache_prefix", lambda _: None)

    def insert(_conn, _payload, *, commit=True):
        assert _conn is conn
        assert commit is False
        assert conn.autocommit is False
        events.append(("insert",))
        if fail_insert and events.count(("insert",)) == 2:
            raise psycopg.OperationalError("simulated insert failure")
        return events.count(("insert",))

    monkeypatch.setattr(managers_api, "_insert_manager", insert)
    payloads = [{"name": "Manager A"}, {"name": "Manager B"}]
    resp = asyncio.run(_post_bulk_json(payloads))

    assert resp.status_code == (503 if fail_insert else 200)
    if fail_insert:
        assert resp.json()["detail"] == "Database unavailable"
        assert events == [
            ("autocommit", False),
            ("insert",),
            ("insert",),
            ("rollback",),
            ("autocommit", True),
            ("close",),
        ]
    else:
        assert events == [
            ("autocommit", False),
            ("insert",),
            ("insert",),
            ("commit",),
            ("autocommit", True),
            ("close",),
        ]


def test_bulk_import_preserves_original_error_when_postgres_cleanup_fails(monkeypatch, caplog):
    psycopg = pytest.importorskip("psycopg")
    import api.managers as managers_api

    events = []

    class FailingCleanupConn:
        autocommit = True

        def __setattr__(self, name, value):
            if name == "autocommit" and value is True and "rollback" in events:
                raise psycopg.ProgrammingError("transaction still active")
            super().__setattr__(name, value)
            if name == "autocommit":
                events.append((name, value))

        def rollback(self):
            events.append("rollback")
            raise psycopg.OperationalError("rollback failed")

        def close(self):
            events.append("close")

    conn = FailingCleanupConn()
    monkeypatch.setattr(managers_api, "connect_db", lambda: conn)
    monkeypatch.setattr(managers_api, "_ensure_universe_schema", lambda _, **_kwargs: None)

    def fail_insert(_conn, _payload, *, commit=True):
        raise psycopg.OperationalError("original insert failure")

    monkeypatch.setattr(managers_api, "_insert_manager", fail_insert)
    resp = asyncio.run(_post_bulk_json([{"name": "Manager A"}]))

    assert resp.status_code == 503
    assert resp.json()["detail"] == "Database unavailable"
    assert "original insert failure" in caplog.text
    assert "Bulk import rollback failed: rollback failed" in caplog.text
    assert "Bulk import autocommit restoration failed: transaction still active" in caplog.text
    assert events == [("autocommit", False), "rollback", "close"]


def test_bulk_import_reports_postgres_close_failure(monkeypatch, caplog):
    psycopg = pytest.importorskip("psycopg")
    import api.managers as managers_api

    events = []

    class FailingCloseConn:
        autocommit = True

        def __setattr__(self, name, value):
            super().__setattr__(name, value)
            if name == "autocommit":
                events.append((name, value))

        def commit(self):
            events.append("commit")

        def close(self):
            events.append("close")
            raise psycopg.OperationalError("close failed")

    conn = FailingCloseConn()
    monkeypatch.setattr(managers_api, "connect_db", lambda: conn)
    monkeypatch.setattr(managers_api, "_ensure_universe_schema", lambda _, **_kwargs: None)
    monkeypatch.setattr(managers_api, "invalidate_cache_prefix", lambda _: None)
    monkeypatch.setattr(managers_api, "_insert_manager", lambda *_args, **_kwargs: 1)

    resp = asyncio.run(_post_bulk_json([{"name": "Manager A"}]))

    assert resp.status_code == 503
    assert resp.json()["detail"] == "Database unavailable"
    assert "Bulk import connection close failed: close failed" in caplog.text
    assert events == [("autocommit", False), "commit", ("autocommit", True), "close"]
