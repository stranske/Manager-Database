"""Additional PostgreSQL parameter contracts using the existing recording double."""

import json

import pytest

from api import managers
from tests.test_manager_dialect_boundaries import RecordingConnection


@pytest.mark.parametrize("row, expected", [(None, 0), ((None,), 0), (("7",), 7)])
def test_postgres_count_keeps_array_filters_in_query_parameters(row, expected):
    conn = RecordingConnection(row)
    jurisdiction = "us'); SELECT 1; --"
    tag = "event-driven%_"

    assert (
        managers._count_managers.__wrapped__(
            conn, "postgres://recording-only", jurisdiction, tag, None, None
        )
        == expected
    )
    assert conn.executed == [
        (
            "SELECT COUNT(*) FROM managers WHERE %s = ANY(jurisdictions) AND %s = ANY(tags)",
            [jurisdiction, tag],
        )
    ]


@pytest.mark.parametrize("commit", [False, True])
def test_postgres_insert_preserves_populated_arrays_and_serializes_registry(commit):
    conn = RecordingConnection(("17",))
    payload = managers.ManagerCreate(
        name="O'Brien Capital",
        cik="000123",
        lei="example-lei",
        aliases=["O'Brien", "Capital %_"],
        jurisdictions=["us", "uk"],
        tags=["event-driven"],
        registry_ids={"fca_frn": "123'456"},
    )
    before = payload.model_dump()

    assert managers._insert_manager(conn, payload, commit=commit) == 17
    assert conn.executed == [
        (
            "INSERT INTO managers(name, cik, lei, aliases, jurisdictions, tags, registry_ids) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb) RETURNING manager_id",
            (
                payload.name,
                payload.cik,
                payload.lei,
                ["O'Brien", "Capital %_"],
                ["us", "uk"],
                ["event-driven"],
                json.dumps({"fca_frn": "123'456"}),
            ),
        )
    ]
    assert payload.model_dump() == before


def test_postgres_update_missing_current_row_stops_before_write():
    conn = RecordingConnection()
    payload = managers.ManagerUpdate(registry_ids={"fca_frn": "123'456"})
    before = payload.model_dump()

    assert managers._update_manager(conn, 17, payload) is False
    assert conn.executed == [
        (
            "SELECT cik, lei, registry_ids, quality_flags FROM managers WHERE manager_id = %s",
            (17,),
        )
    ]
    assert payload.model_dump() == before
