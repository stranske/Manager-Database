"""Rule evaluation engine for alert rules."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from alerts.db import deserialize_json_object, ensure_alert_tables, placeholder, rule_from_row
from alerts.models import AlertEvent, AlertRule, FiredAlert
from utils.numeric import finite_float_or_none


def _as_float(value: Any) -> float | None:
    return finite_float_or_none(value)


def _as_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _event_age_hours(event: AlertEvent) -> float:
    occurred_at = event.occurred_at
    if occurred_at.tzinfo is None:
        occurred_at = occurred_at.replace(tzinfo=UTC)
    return (datetime.now(UTC) - occurred_at).total_seconds() / 3600.0


class AlertEngine:
    """Load enabled rules from the database and evaluate events against them."""

    def __init__(self, db_conn: Any):
        self.db = db_conn
        ensure_alert_tables(self.db)

    def _load_rules(self, event_type: str) -> list[AlertRule]:
        ph = placeholder(self.db)
        enabled_value = 1 if ph == "?" else True
        cursor = self.db.execute(
            f"""SELECT rule_id, name, description, event_type, condition_json, channels, enabled,
                       manager_id, created_by, created_at, updated_at
                  FROM alert_rules
                  WHERE enabled = {ph} AND event_type = {ph}
                  ORDER BY rule_id ASC""",
            (enabled_value, event_type),
        )
        return [rule_from_row(row) for row in cursor.fetchall()]

    def evaluate(self, event: AlertEvent) -> list[FiredAlert]:
        fired: list[FiredAlert] = []
        for rule in self._load_rules(event.event_type):
            if rule.manager_id is not None and rule.manager_id != event.manager_id:
                continue
            if not self._evaluate_condition(rule.condition_json, event):
                continue
            fired.append(FiredAlert(rule=rule, event=event, channels=rule.channels))
        return fired

    def _evaluate_condition(self, condition: dict[str, Any], event: AlertEvent) -> bool:
        if not condition:
            return True

        payload = event.payload
        for key, expected in condition.items():
            if key == "value_usd_gt":
                value = _as_float(payload.get("value_usd"))
                threshold = _as_float(expected)
                if value is None or threshold is None or value <= threshold:
                    return False
                continue
            if key == "delta_type":
                if str(payload.get("delta_type") or "") != str(expected):
                    return False
                continue
            if key == "news_count_gt":
                count = _as_int(payload.get("news_count"))
                if count is None or count <= int(expected):
                    return False
                continue
            if key == "manager_count_gte":
                count = _as_int(payload.get("manager_count"))
                if count is None or count < int(expected):
                    return False
                continue
            if key == "any_new_filing":
                if bool(expected) and event.event_type != "new_filing":
                    return False
                continue
            if key == "time_window_hours":
                hours = _as_float(expected)
                if hours is None or _event_age_hours(event) > hours:
                    return False
                continue
            if key == "min_ownership_pct":
                ownership = finite_float_or_none(
                    payload.get("ownership_pct"), min_value=0.0, max_value=100.0
                )
                threshold = finite_float_or_none(expected, min_value=0.0, max_value=100.0)
                if ownership is None or threshold is None or ownership < threshold:
                    return False
                continue
            if key == "min_delta_pct":
                delta = _as_float(payload.get("delta_pct"))
                threshold = finite_float_or_none(expected, min_value=0.0, max_value=100.0)
                if delta is None or threshold is None or abs(delta) < threshold:
                    return False
                continue
            if key == "threshold_crossed":
                threshold = finite_float_or_none(
                    payload.get("threshold_crossed"), min_value=0.0, max_value=100.0
                )
                expected_threshold = finite_float_or_none(expected, min_value=0.0, max_value=100.0)
                if (
                    threshold is None
                    or expected_threshold is None
                    or threshold != expected_threshold
                ):
                    return False
                continue

            actual = payload.get(key)
            if isinstance(actual, dict):
                actual = deserialize_json_object(actual)
            if actual != expected:
                return False
        return True
