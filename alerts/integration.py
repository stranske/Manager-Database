"""Helpers that ETL flows can call to evaluate rules and persist alert history.

These helpers document the intended wiring points for ingestion flows without
modifying the flows yet. Delivery remains out of scope until S8-02.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from alerts.db import insert_alert_history
from alerts.engine import AlertEngine
from alerts.models import AlertEvent


def build_new_filing_event_from_record(
    filing: Mapping[str, Any],
    *,
    manager_id: int | None,
    filing_id: int | None = None,
    payload: Mapping[str, Any] | None = None,
    occurred_at: datetime | None = None,
) -> AlertEvent:
    """Build a `new_filing` event directly from an EDGAR filing record.

    This keeps the ETL integration thin by accepting the same mapping shape
    returned by the adapter and extracting the canonical alert payload fields.
    """

    merged_payload = dict(payload or {})
    for key in ("accession", "cik", "source", "url"):
        value = filing.get(key)
        if value is not None:
            merged_payload.setdefault(key, value)

    filing_type = filing.get("type") or filing.get("form")
    filed_date = filing.get("filed_date") or filing.get("filed")
    return build_new_filing_event(
        filing_id=filing_id,
        manager_id=manager_id,
        filing_type=str(filing_type) if filing_type is not None else None,
        filed_date=str(filed_date) if filed_date is not None else None,
        payload=merged_payload,
        occurred_at=occurred_at,
    )


def build_new_filing_event(
    *,
    filing_id: int | None,
    manager_id: int | None,
    filing_type: str | None = None,
    filed_date: str | None = None,
    payload: Mapping[str, Any] | None = None,
    occurred_at: datetime | None = None,
) -> AlertEvent:
    """Build the `new_filing` event that ETL ingestion should emit.

    Intended integration point for `etl/edgar_flow.py` after a filing is stored:

    ```python
    event = build_new_filing_event(
        filing_id=filing_id,
        manager_id=manager_id,
        filing_type=str(filing.get("form") or "13F-HR"),
        filed_date=filing.get("filed"),
    )
    alert_ids = evaluate_and_record_alerts(conn, event)
    ```
    """

    event_payload = dict(payload or {})
    normalized_type = (filing_type or event_payload.get("type") or "").strip()
    if not normalized_type:
        raise ValueError("filing_type is required for new_filing alerts.")

    event_payload.setdefault("type", normalized_type)
    if filing_id is not None:
        event_payload.setdefault("filing_id", filing_id)
    if filed_date:
        event_payload.setdefault("filed_date", filed_date)

    return AlertEvent(
        event_type="new_filing",
        manager_id=manager_id,
        payload=event_payload,
        occurred_at=occurred_at or datetime.now(UTC),
    )


def evaluate_and_record_alerts(conn: Any, event: AlertEvent) -> list[int]:
    """Evaluate one event and persist any matches to `alert_history`.

    This is the generic persistence hook for ETL flows. It intentionally records
    matches only; downstream delivery will be added separately.
    """

    fired = AlertEngine(conn).evaluate(event)
    return insert_alert_history(conn, fired)


def evaluate_and_record_new_filing_alerts(
    conn: Any,
    *,
    filing_id: int | None,
    manager_id: int | None,
    filing_type: str | None = None,
    filed_date: str | None = None,
    payload: Mapping[str, Any] | None = None,
    occurred_at: datetime | None = None,
) -> list[int]:
    """Convenience wrapper for the `new_filing` ETL integration point."""

    event = build_new_filing_event(
        filing_id=filing_id,
        manager_id=manager_id,
        filing_type=filing_type,
        filed_date=filed_date,
        payload=payload,
        occurred_at=occurred_at,
    )
    return evaluate_and_record_alerts(conn, event)
