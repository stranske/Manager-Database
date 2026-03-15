"""Alert message formatting helpers shared by delivery channels."""

from __future__ import annotations

from datetime import UTC, datetime
from html import escape
from typing import Any

from alerts.models import FiredAlert

_PAYLOAD_ORDER = (
    "manager_name",
    "filing_id",
    "type",
    "filed_date",
    "subject_company",
    "subject_cusip",
    "cusip",
    "delta_type",
    "value_usd",
    "ownership_pct",
    "previous_pct",
    "delta_pct",
    "threshold_crossed",
    "manager_count",
    "news_count",
    "consensus_direction",
    "report_date",
)


def _display_manager(alert: FiredAlert) -> str:
    manager_name = str(alert.event.payload.get("manager_name") or "").strip()
    if manager_name:
        return manager_name
    if alert.event.manager_id is not None:
        return f"Manager {alert.event.manager_id}"
    return "All managers"


def _display_timestamp(timestamp: datetime) -> str:
    value = timestamp
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _payload_rows(payload: dict[str, Any]) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    seen: set[str] = set()

    for key in _PAYLOAD_ORDER:
        value = payload.get(key)
        if value in (None, "", []):
            continue
        rows.append((key.replace("_", " ").title(), str(value)))
        seen.add(key)

    for key, value in sorted(payload.items()):
        if key in seen or value in (None, "", [], {}):
            continue
        if isinstance(value, (dict, list)):
            continue
        rows.append((key.replace("_", " ").title(), str(value)))
    return rows


def format_plain_text(alert: FiredAlert) -> str:
    """Format a concise plaintext representation usable across channels."""
    lines = [
        f"Rule: {alert.rule.name}",
        f"Event type: {alert.event.event_type}",
        f"Manager: {_display_manager(alert)}",
        f"Occurred at: {_display_timestamp(alert.event.occurred_at)}",
    ]
    for label, value in _payload_rows(alert.event.payload):
        lines.append(f"{label}: {value}")
    return "\n".join(lines)


def format_email_html(alert: FiredAlert) -> str:
    """Render a lightweight HTML email body for one fired alert."""
    rows = "".join(
        "<tr>"
        f"<th align='left' style='padding:4px 8px;border-bottom:1px solid #ddd'>{escape(label)}</th>"
        f"<td style='padding:4px 8px;border-bottom:1px solid #ddd'>{escape(value)}</td>"
        "</tr>"
        for label, value in _payload_rows(alert.event.payload)
    )
    if not rows:
        rows = (
            "<tr><td colspan='2' style='padding:4px 8px;border-bottom:1px solid #ddd'>"
            "No additional payload fields"
            "</td></tr>"
        )

    return (
        "<html><body style='font-family:Arial,sans-serif'>"
        f"<h2>{escape(alert.rule.name)}</h2>"
        f"<p><strong>Event type:</strong> {escape(alert.event.event_type)}<br>"
        f"<strong>Manager:</strong> {escape(_display_manager(alert))}<br>"
        f"<strong>Occurred at:</strong> {escape(_display_timestamp(alert.event.occurred_at))}</p>"
        "<table style='border-collapse:collapse'>"
        f"{rows}"
        "</table>"
        "</body></html>"
    )


def format_slack_blocks(alert: FiredAlert) -> dict[str, Any]:
    """Render a Slack-compatible Block Kit payload."""
    field_lines = [
        f"*{escape(label)}*\n{escape(value)}"
        for label, value in _payload_rows(alert.event.payload)[:10]
    ]
    blocks: list[dict[str, Any]] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": alert.rule.name[:150]},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Event type:* `{alert.event.event_type}`\n"
                    f"*Manager:* {_display_manager(alert)}\n"
                    f"*Occurred at:* {_display_timestamp(alert.event.occurred_at)}"
                ),
            },
        },
    ]
    if field_lines:
        blocks.append(
            {
                "type": "section",
                "fields": [{"type": "mrkdwn", "text": line} for line in field_lines],
            }
        )
    return {"text": format_plain_text(alert), "blocks": blocks}
