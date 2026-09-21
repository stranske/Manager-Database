"""Fail-closed manager erasure across relational and object storage state."""

from __future__ import annotations

import os
from collections.abc import Iterable
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from adapters.base import get_placeholder, get_table_columns, is_sqlite, table_exists


class AmbiguousManagerObjectsError(RuntimeError):
    """Raised when legacy rows do not identify their physical object key."""


class ManagerObjectDeletionError(RuntimeError):
    """Raised when an exact object key could not be removed."""


class ManagerErasureFenceError(RuntimeError):
    """Raised when ingestion cannot write because erasure is active or complete."""


ACTIVE_DELETION_STATES = (
    "blocked",
    "collecting",
    "deleting_objects",
    "object_failed",
    "purging_relational",
)
_MANAGER_LOCK_NAMESPACE = 4_320_000_000_000_000


@dataclass(frozen=True)
class ManagerDeletionResult:
    deleted: bool
    operation_id: str | None = None


def _id_column(conn: Any, table: str, *candidates: str) -> str | None:
    columns = get_table_columns(conn, table)
    return next((candidate for candidate in candidates if candidate in columns), None)


def _ensure_ledger(conn: Any) -> None:
    json_default = "'[]'" if is_sqlite(conn) else "'[]'::jsonb"
    conn.execute(f"""CREATE TABLE IF NOT EXISTS manager_deletion_operations (
            operation_id text PRIMARY KEY,
            manager_id bigint NOT NULL,
            state text NOT NULL,
            ambiguous_keys jsonb NOT NULL DEFAULT {json_default},
            error text,
            created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at timestamp
        )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS manager_deletion_objects (
            operation_id text NOT NULL REFERENCES manager_deletion_operations(operation_id)
                ON DELETE CASCADE,
            bucket text NOT NULL,
            object_key text NOT NULL,
            state text NOT NULL DEFAULT 'pending',
            error text,
            PRIMARY KEY (operation_id, bucket, object_key)
        )""")


def _commit(conn: Any) -> None:
    commit = getattr(conn, "commit", None)
    if callable(commit):
        commit()


def _rollback(conn: Any) -> None:
    rollback = getattr(conn, "rollback", None)
    if callable(rollback):
        rollback()


def manager_deletion_active(conn: Any, manager_id: int) -> bool:
    if not table_exists(conn, "manager_deletion_operations"):
        return False
    marker = get_placeholder(conn)
    active = conn.execute(
        "SELECT 1 FROM manager_deletion_operations "
        f"WHERE manager_id = {marker} AND state IN "
        "('blocked', 'collecting', 'deleting_objects', 'object_failed', "
        "'purging_relational') LIMIT 1",
        (manager_id,),
    ).fetchone()
    return active is not None


def _manager_lock_key(manager_id: int) -> int:
    return _MANAGER_LOCK_NAMESPACE + int(manager_id)


def _acquire_manager_lock(conn: Any, manager_id: int) -> None:
    if is_sqlite(conn):
        if getattr(conn, "in_transaction", False):
            conn.commit()
        conn.execute("BEGIN IMMEDIATE")
        return
    conn.execute("SELECT pg_advisory_lock(%s)", (_manager_lock_key(manager_id),))


def _release_manager_lock(conn: Any, manager_id: int) -> None:
    if not is_sqlite(conn):
        conn.execute("SELECT pg_advisory_unlock(%s)", (_manager_lock_key(manager_id),))
        if getattr(conn, "autocommit", True) is False:
            conn.commit()


def acquire_manager_ingestion_lock(conn: Any, manager_id: int) -> bool:
    """Lock one manager and recheck that ingestion is still allowed."""
    _acquire_manager_lock(conn, manager_id)
    allowed = _manager_exists(conn, manager_id) and not manager_deletion_active(conn, manager_id)
    if not allowed:
        if is_sqlite(conn) and getattr(conn, "in_transaction", False):
            conn.rollback()
        _release_manager_lock(conn, manager_id)
    return allowed


def release_manager_ingestion_lock(conn: Any, manager_id: int) -> None:
    """Release a lock acquired by :func:`acquire_manager_ingestion_lock`."""
    if is_sqlite(conn) and getattr(conn, "in_transaction", False):
        conn.rollback()
    _release_manager_lock(conn, manager_id)


def _manager_exists(conn: Any, manager_id: int) -> bool:
    manager_pk = _id_column(conn, "managers", "manager_id", "id")
    if manager_pk is None:
        return False
    marker = get_placeholder(conn)
    return (
        conn.execute(
            f"SELECT 1 FROM managers WHERE {manager_pk} = {marker} LIMIT 1",
            (manager_id,),
        ).fetchone()
        is not None
    )


def _rows(conn: Any, sql: str, params: tuple[Any, ...]) -> list[tuple[Any, ...]]:
    return list(conn.execute(sql, params).fetchall())


def _target_storage_references(conn: Any, manager_id: int) -> tuple[set[str], set[str]]:
    """Return exact physical keys and ambiguous legacy identifiers."""
    marker = get_placeholder(conn)
    exact: set[str] = set()
    ambiguous: set[str] = set()

    if table_exists(conn, "filings"):
        columns = get_table_columns(conn, "filings")
        selected = [
            column for column in ("storage_key", "raw_key", "external_id") if column in columns
        ]
        if "manager_id" in columns and selected:
            for row in _rows(
                conn,
                f"SELECT {', '.join(selected)} FROM filings WHERE manager_id = {marker}",
                (manager_id,),
            ):
                values = dict(zip(selected, row, strict=True))
                storage_key = str(values.get("storage_key") or "").strip()
                raw_key = str(values.get("raw_key") or "").strip()
                external_id = str(values.get("external_id") or "").strip()
                if storage_key:
                    exact.add(storage_key)
                elif raw_key.startswith("raw/"):
                    exact.add(raw_key)
                elif raw_key or external_id:
                    ambiguous.add(raw_key or external_id)

    if table_exists(conn, "activism_filings"):
        columns = get_table_columns(conn, "activism_filings")
        if {"manager_id", "raw_key"}.issubset(columns):
            for (value,) in _rows(
                conn,
                f"SELECT raw_key FROM activism_filings WHERE manager_id = {marker}",
                (manager_id,),
            ):
                key = str(value or "").strip()
                if key.startswith("raw/"):
                    exact.add(key)
                elif key:
                    ambiguous.add(key)

    if table_exists(conn, "activism_documents") and table_exists(conn, "activism_campaigns"):
        document_columns = get_table_columns(conn, "activism_documents")
        campaign_columns = get_table_columns(conn, "activism_campaigns")
        if {"campaign_id", "raw_key"}.issubset(document_columns) and {
            "campaign_id",
            "manager_id",
        }.issubset(campaign_columns):
            for (value,) in _rows(
                conn,
                "SELECT ad.raw_key FROM activism_documents ad "
                "JOIN activism_campaigns ac ON ac.campaign_id = ad.campaign_id "
                f"WHERE ac.manager_id = {marker}",
                (manager_id,),
            ):
                key = str(value or "").strip()
                if key.startswith("raw/"):
                    exact.add(key)
                elif key:
                    ambiguous.add(key)

    return exact, ambiguous


def _shared_exact_keys(conn: Any, manager_id: int, keys: Iterable[str]) -> set[str]:
    """Return exact keys still referenced by another manager-owned row."""
    key_set = set(keys)
    if not key_set:
        return set()
    shared: set[str] = set()
    for table in ("filings", "activism_filings"):
        if not table_exists(conn, table):
            continue
        columns = get_table_columns(conn, table)
        key_columns = [column for column in ("storage_key", "raw_key") if column in columns]
        if "manager_id" not in columns:
            continue
        for key_column in key_columns:
            rows = _rows(conn, f"SELECT {key_column}, manager_id FROM {table}", ())
            shared.update(
                str(key)
                for key, owner in rows
                if key in key_set and owner is not None and int(owner) != manager_id
            )
    if table_exists(conn, "activism_documents") and table_exists(conn, "activism_campaigns"):
        document_columns = get_table_columns(conn, "activism_documents")
        campaign_columns = get_table_columns(conn, "activism_campaigns")
        if {"campaign_id", "raw_key"}.issubset(document_columns) and {
            "campaign_id",
            "manager_id",
        }.issubset(campaign_columns):
            rows = _rows(
                conn,
                "SELECT ad.raw_key, ac.manager_id FROM activism_documents ad "
                "JOIN activism_campaigns ac ON ac.campaign_id = ad.campaign_id",
                (),
            )
            shared.update(
                str(key)
                for key, owner in rows
                if key in key_set and owner is not None and int(owner) != manager_id
            )
    return shared


def _record_manifest(
    conn: Any,
    manager_id: int,
    bucket: str,
    keys: set[str],
    ambiguous: set[str],
) -> str:
    import json

    _ensure_ledger(conn)
    operation_id = str(uuid4())
    marker = get_placeholder(conn)
    ambiguous_value: Any = json.dumps(sorted(ambiguous))
    if not is_sqlite(conn):
        ambiguous_placeholder = f"{marker}::jsonb"
    else:
        ambiguous_placeholder = marker
    conn.execute(
        "INSERT INTO manager_deletion_operations("
        "operation_id, manager_id, state, ambiguous_keys) "
        f"VALUES ({marker}, {marker}, {marker}, {ambiguous_placeholder})",
        (
            operation_id,
            manager_id,
            "blocked" if ambiguous else "deleting_objects",
            ambiguous_value,
        ),
    )
    for key in sorted(keys):
        conn.execute(
            "INSERT INTO manager_deletion_objects(operation_id, bucket, object_key) "
            f"VALUES ({marker}, {marker}, {marker})",
            (operation_id, bucket, key),
        )
    return operation_id


def _delete_objects(bucket: str, keys: set[str]) -> None:
    if not keys:
        return
    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minio"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
    )
    ordered_keys = sorted(keys)
    for start in range(0, len(ordered_keys), 1000):
        batch = ordered_keys[start : start + 1000]
        response = client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
        )
        errors = response.get("Errors") or []
        if errors:
            raise ManagerObjectDeletionError("Object storage rejected one or more exact keys")


def _mark_objects_deleted(conn: Any, operation_id: str) -> None:
    marker = get_placeholder(conn)
    conn.execute(
        f"UPDATE manager_deletion_objects SET state = 'deleted', error = NULL "
        f"WHERE operation_id = {marker}",
        (operation_id,),
    )
    conn.execute(
        f"UPDATE manager_deletion_operations SET state = 'purging_relational' "
        f"WHERE operation_id = {marker}",
        (operation_id,),
    )


def _delete_where(conn: Any, table: str, predicate: str, params: tuple[Any, ...]) -> None:
    if table_exists(conn, table):
        conn.execute(f"DELETE FROM {table} WHERE {predicate}", params)


def _delete_documents(conn: Any, manager_id: int) -> None:
    if not table_exists(conn, "documents"):
        return
    columns = get_table_columns(conn, "documents")
    doc_pk = _id_column(conn, "documents", "doc_id", "id")
    if doc_pk is None:
        return
    marker = get_placeholder(conn)
    candidates: set[int] = set()
    if "manager_id" in columns:
        candidates.update(
            int(row[0])
            for row in _rows(
                conn,
                f"SELECT {doc_pk} FROM documents WHERE manager_id = {marker}",
                (manager_id,),
            )
        )
    junction = table_exists(conn, "document_managers")
    if junction:
        candidates.update(
            int(row[0])
            for row in _rows(
                conn,
                f"SELECT doc_id FROM document_managers WHERE manager_id = {marker}",
                (manager_id,),
            )
        )
        conn.execute(
            f"DELETE FROM document_managers WHERE manager_id = {marker}",
            (manager_id,),
        )
    for doc_id in candidates:
        remaining_owner: int | None = None
        if junction:
            row = conn.execute(
                f"SELECT manager_id FROM document_managers WHERE doc_id = {marker} "
                "ORDER BY manager_id LIMIT 1",
                (doc_id,),
            ).fetchone()
            if row and row[0] is not None:
                remaining_owner = int(row[0])
        if "manager_id" in columns:
            conn.execute(
                f"UPDATE documents SET manager_id = {marker} "
                f"WHERE {doc_pk} = {marker} AND manager_id = {marker}",
                (remaining_owner, doc_id, manager_id),
            )
            owner_row = conn.execute(
                f"SELECT manager_id FROM documents WHERE {doc_pk} = {marker}",
                (doc_id,),
            ).fetchone()
            if owner_row and owner_row[0] is not None:
                continue
        if remaining_owner is None:
            conn.execute(f"DELETE FROM documents WHERE {doc_pk} = {marker}", (doc_id,))


def _purge_relational(conn: Any, manager_id: int, operation_id: str) -> bool:
    marker = get_placeholder(conn)
    filing_id = _id_column(conn, "filings", "filing_id", "id")
    activism_filing_id = _id_column(conn, "activism_filings", "filing_id", "id")

    if table_exists(conn, "activism_campaign_timeline"):
        predicates: list[str] = []
        params: list[Any] = []
        if table_exists(conn, "activism_campaigns"):
            predicates.append(
                f"campaign_id IN (SELECT campaign_id FROM activism_campaigns WHERE manager_id = {marker})"
            )
            params.append(manager_id)
        if activism_filing_id:
            predicates.append(
                f"filing_id IN (SELECT {activism_filing_id} FROM activism_filings "
                f"WHERE manager_id = {marker})"
            )
            params.append(manager_id)
        if predicates:
            _delete_where(
                conn, "activism_campaign_timeline", " OR ".join(predicates), tuple(params)
            )
    if table_exists(conn, "activism_documents"):
        predicates = []
        params = []
        if table_exists(conn, "activism_campaigns"):
            predicates.append(
                f"campaign_id IN (SELECT campaign_id FROM activism_campaigns WHERE manager_id = {marker})"
            )
            params.append(manager_id)
        if activism_filing_id:
            predicates.append(
                f"filing_id IN (SELECT {activism_filing_id} FROM activism_filings "
                f"WHERE manager_id = {marker})"
            )
            params.append(manager_id)
        if predicates:
            _delete_where(conn, "activism_documents", " OR ".join(predicates), tuple(params))
    for table in ("activism_events", "activism_campaigns", "activism_filings"):
        if "manager_id" in get_table_columns(conn, table):
            _delete_where(conn, table, f"manager_id = {marker}", (manager_id,))

    filing_subquery = (
        f"SELECT {filing_id} FROM filings WHERE manager_id = {marker}" if filing_id else None
    )
    for table in (
        "holdings",
        "conviction_scores",
        "manager_attribution",
        "identifier_resolution_metrics",
    ):
        columns = get_table_columns(conn, table)
        predicates = []
        params = []
        if filing_subquery and "filing_id" in columns:
            predicates.append(f"filing_id IN ({filing_subquery})")
            params.append(manager_id)
        if "manager_id" in columns:
            predicates.append(f"manager_id = {marker}")
            params.append(manager_id)
        if predicates:
            _delete_where(conn, table, " OR ".join(predicates), tuple(params))

    if table_exists(conn, "backtest_runs") and "manager_id" in get_table_columns(
        conn, "backtest_runs"
    ):
        if table_exists(conn, "backtest_results"):
            _delete_where(
                conn,
                "backtest_results",
                f"run_id IN (SELECT run_id FROM backtest_runs WHERE manager_id = {marker})",
                (manager_id,),
            )
        _delete_where(conn, "backtest_runs", f"manager_id = {marker}", (manager_id,))

    if table_exists(conn, "alert_rules") and "manager_id" in get_table_columns(conn, "alert_rules"):
        if table_exists(conn, "alert_history"):
            if table_exists(conn, "alert_delivery_attempts"):
                _delete_where(
                    conn,
                    "alert_delivery_attempts",
                    "alert_id IN (SELECT ah.alert_id FROM alert_history ah "
                    "JOIN alert_rules ar ON ar.rule_id = ah.rule_id "
                    f"WHERE ar.manager_id = {marker})",
                    (manager_id,),
                )
            _delete_where(
                conn,
                "alert_history",
                f"rule_id IN (SELECT rule_id FROM alert_rules WHERE manager_id = {marker})",
                (manager_id,),
            )
        _delete_where(conn, "alert_rules", f"manager_id = {marker}", (manager_id,))

    if filing_id:
        _delete_where(conn, "filings", f"manager_id = {marker}", (manager_id,))
    if table_exists(conn, "manager_similarity"):
        _delete_where(
            conn,
            "manager_similarity",
            f"manager_id_a = {marker} OR manager_id_b = {marker}",
            (manager_id, manager_id),
        )
    for table in ("news_items", "daily_diffs", "contrarian_signals"):
        if "manager_id" in get_table_columns(conn, table):
            _delete_where(conn, table, f"manager_id = {marker}", (manager_id,))
    if table_exists(conn, "crowded_trades") and "manager_ids" in get_table_columns(
        conn, "crowded_trades"
    ):
        if is_sqlite(conn):
            _delete_where(
                conn,
                "crowded_trades",
                f"EXISTS (SELECT 1 FROM json_each(manager_ids) WHERE value = {marker})",
                (manager_id,),
            )
        else:
            _delete_where(conn, "crowded_trades", f"{marker} = ANY(manager_ids)", (manager_id,))

    _delete_documents(conn, manager_id)
    manager_pk = _id_column(conn, "managers", "manager_id", "id")
    if manager_pk is None:
        return False
    cursor = conn.execute(
        f"DELETE FROM managers WHERE {manager_pk} = {marker}",
        (manager_id,),
    )
    if not is_sqlite(conn):
        materialized_view = conn.execute("SELECT to_regclass(%s)", ("mv_daily_report",)).fetchone()
        if materialized_view and materialized_view[0]:
            conn.execute("REFRESH MATERIALIZED VIEW mv_daily_report")
    conn.execute(
        "UPDATE manager_deletion_operations SET state = 'superseded' "
        f"WHERE manager_id = {marker} AND operation_id != {marker} "
        "AND state != 'completed'",
        (manager_id, operation_id),
    )
    conn.execute(
        "UPDATE manager_deletion_operations SET state = 'completed', "
        "completed_at = CURRENT_TIMESTAMP, error = NULL "
        f"WHERE operation_id = {marker}",
        (operation_id,),
    )
    return cursor.rowcount > 0


def delete_manager_data(conn: Any, manager_id: int) -> ManagerDeletionResult:
    """Delete one manager only after exact object cleanup can be proven."""
    _acquire_manager_lock(conn, manager_id)
    try:
        if not _manager_exists(conn, manager_id):
            return ManagerDeletionResult(deleted=False)
        bucket = os.getenv("MINIO_BUCKET", "filings")
        exact, ambiguous = _target_storage_references(conn, manager_id)
        exact -= _shared_exact_keys(conn, manager_id, exact)
        operation_id = _record_manifest(conn, manager_id, bucket, exact, ambiguous)
        if ambiguous:
            _commit(conn)
            raise AmbiguousManagerObjectsError(
                "Manager deletion is blocked because legacy filings lack an exact physical object key"
            )
        try:
            _delete_objects(bucket, exact)
        except Exception as exc:
            marker = get_placeholder(conn)
            conn.execute(
                "UPDATE manager_deletion_operations SET state = 'object_failed', error = "
                f"{marker} WHERE operation_id = {marker}",
                (str(exc), operation_id),
            )
            _commit(conn)
            if isinstance(exc, ManagerObjectDeletionError):
                raise
            raise ManagerObjectDeletionError("Object storage deletion failed") from exc
        _mark_objects_deleted(conn, operation_id)

        if is_sqlite(conn):
            deleted = _purge_relational(conn, manager_id, operation_id)
            conn.commit()
        else:
            transaction = getattr(conn, "transaction", None)
            with transaction() if callable(transaction) else nullcontext():
                deleted = _purge_relational(conn, manager_id, operation_id)
    except Exception:
        _rollback(conn)
        raise
    finally:
        if is_sqlite(conn) and getattr(conn, "in_transaction", False):
            _rollback(conn)
        _release_manager_lock(conn, manager_id)
    return ManagerDeletionResult(deleted=deleted, operation_id=operation_id)
