"""Check PostgreSQL adapter arguments without claiming a live database execution."""

import json
from types import SimpleNamespace

from api import managers


class RecordingConnection:
    def __init__(self, row=None):
        self.row = row
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((" ".join(sql.split()), params))
        return SimpleNamespace(fetchone=lambda: self.row, rowcount=1)


def test_postgres_array_filters_keep_values_in_parameters():
    conn = RecordingConnection()
    jurisdiction = "us'); SELECT secret; --"
    tag = "event-driven%_"
    clauses, params = managers._manager_filter_clauses(conn, jurisdiction, tag, None, None)
    assert clauses == ["%s = ANY(jurisdictions)", "%s = ANY(tags)"]
    assert params == [jurisdiction, tag]
    assert conn.executed == []


def test_postgres_universe_upsert_preserves_native_array_and_conflict_update():
    conn = RecordingConnection()
    managers._upsert_universe_record(conn, "O'Brien Capital", "000123", "us")
    assert len(conn.executed) == 1
    sql, params = conn.executed[0]
    assert "VALUES (%s, %s, %s, %s, now())" in sql
    assert "ON CONFLICT(cik) DO UPDATE SET" in sql
    assert "name = EXCLUDED.name" in sql
    assert "jurisdictions = EXCLUDED.jurisdictions" in sql
    assert params == ("O'Brien Capital", "000123", "us", ["us"])
    assert "O'Brien" not in sql


def test_postgres_insert_missing_returned_identity_is_zero():
    for row in (None, (None,)):
        conn = RecordingConnection(row)
        assert managers._insert_manager(conn, managers.ManagerCreate(name="Example")) == 0
        assert len(conn.executed) == 1
        sql, params = conn.executed[0]
        assert sql.endswith("RETURNING manager_id")
        assert params == ("Example", None, None, [], [], [], "{}")


def test_postgres_registry_update_uses_jsonb_cast_and_serialized_parameter():
    conn = RecordingConnection((None, None, {}, []))
    registry = {"fca_frn": "123'456"}
    assert managers._update_manager(conn, 17, managers.ManagerUpdate(registry_ids=registry))
    assert len(conn.executed) == 2
    sql, params = conn.executed[-1]
    assert sql == (
        "UPDATE managers SET registry_ids = %s::jsonb, updated_at = CURRENT_TIMESTAMP "
        "WHERE manager_id = %s"
    )
    assert params == [json.dumps(registry), 17]
    assert "123'456" not in sql


def test_postgres_conflict_update_preserves_old_flags_with_jsonb_cast():
    old_flag = {"type": "existing", "detail": "retained"}
    conn = RecordingConnection(("000123", None, {}, [old_flag]))
    assert managers._update_manager(conn, 17, managers.ManagerUpdate(cik="000456"))
    sql, params = conn.executed[-1]
    assert sql == (
        "UPDATE managers SET cik = %s, quality_flags = %s::jsonb, "
        "updated_at = CURRENT_TIMESTAMP WHERE manager_id = %s"
    )
    assert params[0] == "000456" and params[-1] == 17
    flags = json.loads(params[1])
    assert flags[0] == old_flag
    assert len(flags) == 2
    assert flags[1]["type"] == "identifier_conflict"
    assert flags[1]["field"] == "cik"
    assert flags[1]["previous_value"] == "000123"
    assert flags[1]["current_value"] == "000456"
    assert flags[1]["observed_at"]


def test_count_missing_aggregate_row_is_zero_without_cache():
    for row in (None, (None,)):
        conn = RecordingConnection(row)
        count = managers._count_managers.__wrapped__(
            conn, "postgres://adapter-boundary", None, None, None, None
        )
        assert count == 0
        assert conn.executed == [("SELECT COUNT(*) FROM managers", [])]
