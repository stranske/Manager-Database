"""Automated filings, news, and alert digest generation."""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from email.message import EmailMessage
from html import escape
from pathlib import Path
from typing import Any

import httpx
from prefect import flow, task

from adapters.base import connect_db, get_placeholder, get_table_columns, table_exists
from alerts.channels import DeliveryResult, send_email_message_via_smtp
from alerts.db import deserialize_json_object
from etl.logging_setup import configure_logging, log_outcome
from tools.run_contract import new_run_id, write_artifact_bundle

configure_logging("digest_flow")
logger = logging.getLogger(__name__)


@dataclass(slots=True)
class DigestFiling:
    manager_name: str
    filing_type: str
    filed_date: str
    source: str
    url: str | None = None


@dataclass(slots=True)
class DigestNewsItem:
    manager_name: str
    headline: str
    source: str
    published_at: str
    url: str | None = None


@dataclass(slots=True)
class DigestAlert:
    rule_name: str
    event_type: str
    fired_at: str
    summary: str
    manager_name: str | None = None


@dataclass(slots=True)
class DigestDocument:
    generated_at: datetime
    lookback_hours: int
    filings: list[DigestFiling] = field(default_factory=list)
    news: list[DigestNewsItem] = field(default_factory=list)
    alerts: list[DigestAlert] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not (self.filings or self.news or self.alerts)


def _coerce_utc(value: datetime | None) -> datetime:
    current = value or datetime.now(UTC)
    if current.tzinfo is None:
        return current.replace(tzinfo=UTC)
    return current.astimezone(UTC)


def _row_value(row: Any, index: int) -> Any:
    return row[index] if not isinstance(row, sqlite3.Row) else row[index]


def _payload_summary(payload: dict[str, Any]) -> str:
    for key in ("summary", "headline", "type", "filing_type", "delta_type"):
        value = payload.get(key)
        if value not in (None, "", []):
            return str(value)
    if payload.get("news_count"):
        return f"{payload['news_count']} manager-linked news items"
    if payload.get("threshold_crossed"):
        return f"Threshold crossed: {payload['threshold_crossed']}"
    return "Important alert event"


@task
def build_digest(
    conn: Any, lookback_hours: int = 24, now: datetime | None = None
) -> DigestDocument:
    """Query recent filings, news, and unacknowledged alerts into one digest."""
    generated_at = _coerce_utc(now)
    window_start = generated_at - timedelta(hours=lookback_hours)
    ph = get_placeholder(conn)
    digest = DigestDocument(generated_at=generated_at, lookback_hours=lookback_hours)

    filing_cols = get_table_columns(conn, "filings")
    if {"manager_id", "type", "source"} <= filing_cols and table_exists(conn, "managers"):
        filter_expr = "f.created_at" if "created_at" in filing_cols else "f.filed_date"
        filter_value = (
            window_start.isoformat()
            if "created_at" in filing_cols
            else window_start.date().isoformat()
        )
        display_date_expr = (
            "COALESCE(f.filed_date, f.created_at)"
            if "created_at" in filing_cols
            else "f.filed_date"
        )
        rows = conn.execute(
            f"""SELECT COALESCE(m.name, 'Manager ' || CAST(f.manager_id AS TEXT)), f.type, {display_date_expr},
                       f.source, f.url
                  FROM filings AS f
                  LEFT JOIN managers AS m ON m.manager_id = f.manager_id
                 WHERE {filter_expr} >= {ph}
                 ORDER BY {filter_expr} DESC, f.filing_id DESC""",
            (filter_value,),
        ).fetchall()
        digest.filings = [
            DigestFiling(
                manager_name=str(_row_value(row, 0)),
                filing_type=str(_row_value(row, 1)),
                filed_date=str(_row_value(row, 2)),
                source=str(_row_value(row, 3)),
                url=str(_row_value(row, 4)) if _row_value(row, 4) else None,
            )
            for row in rows
        ]

    news_cols = get_table_columns(conn, "news_items")
    if {"manager_id", "published_at", "source", "headline"} <= news_cols:
        rows = conn.execute(
            f"""SELECT COALESCE(
                       m.name,
                       CASE
                           WHEN n.manager_id IS NOT NULL
                           THEN 'Manager ' || CAST(n.manager_id AS TEXT)
                       END,
                       'Unlinked manager'
                   ),
                       n.headline, n.source, n.published_at, n.url
                  FROM news_items AS n
                  LEFT JOIN managers AS m ON m.manager_id = n.manager_id
                 WHERE n.published_at >= {ph}
                 ORDER BY n.published_at DESC, n.news_id DESC""",
            (window_start.isoformat(),),
        ).fetchall()
        digest.news = [
            DigestNewsItem(
                manager_name=str(_row_value(row, 0)),
                headline=str(_row_value(row, 1)),
                source=str(_row_value(row, 2)),
                published_at=str(_row_value(row, 3)),
                url=str(_row_value(row, 4)) if _row_value(row, 4) else None,
            )
            for row in rows
        ]

    alert_cols = get_table_columns(conn, "alert_history")
    if {"rule_id", "fired_at", "event_type", "payload_json", "acknowledged"} <= alert_cols:
        rows = conn.execute(
            f"""SELECT COALESCE(ar.name, ah.event_type), ah.event_type, ah.fired_at,
                       ah.payload_json, m.name
                  FROM alert_history AS ah
                  LEFT JOIN alert_rules AS ar ON ar.rule_id = ah.rule_id
                  LEFT JOIN managers AS m ON m.manager_id = ar.manager_id
                 WHERE ah.acknowledged = {ph} AND ah.fired_at >= {ph}
                 ORDER BY ah.fired_at DESC, ah.alert_id DESC""",
            (False if not isinstance(conn, sqlite3.Connection) else 0, window_start.isoformat()),
        ).fetchall()
        digest.alerts = [
            DigestAlert(
                rule_name=str(_row_value(row, 0)),
                event_type=str(_row_value(row, 1)),
                fired_at=str(_row_value(row, 2)),
                summary=_payload_summary(deserialize_json_object(_row_value(row, 3))),
                manager_name=str(_row_value(row, 4)) if _row_value(row, 4) else None,
            )
            for row in rows
        ]

    return digest


def render_digest_plain_text(digest: DigestDocument) -> str:
    """Render a deterministic plaintext digest."""
    lines = [
        f"Manager activity digest ({digest.lookback_hours}h)",
        f"Generated at: {digest.generated_at.isoformat()}",
        "",
    ]
    if digest.is_empty:
        lines.append("No filings, manager-linked news, or unacknowledged alerts were found.")
        return "\n".join(lines)

    lines.append(f"Filings ({len(digest.filings)})")
    for filing in digest.filings:
        lines.append(
            f"- {filing.manager_name}: {filing.filing_type} filed {filing.filed_date} via {filing.source}"
        )
    lines.extend(["", f"News ({len(digest.news)})"])
    for news_item in digest.news:
        lines.append(
            f"- {news_item.manager_name}: {news_item.headline} "
            f"({news_item.source}, {news_item.published_at})"
        )
    lines.extend(["", f"Important alerts ({len(digest.alerts)})"])
    for alert in digest.alerts:
        manager = f" [{alert.manager_name}]" if alert.manager_name else ""
        lines.append(f"- {alert.rule_name}{manager}: {alert.summary} ({alert.fired_at})")
    return "\n".join(lines)


def render_digest_html(digest: DigestDocument) -> str:
    """Render a lightweight HTML digest body."""
    if digest.is_empty:
        body = "<p>No filings, manager-linked news, or unacknowledged alerts were found.</p>"
    else:
        filing_rows = "".join(
            "<li>"
            f"{escape(item.manager_name)}: {escape(item.filing_type)} filed "
            f"{escape(item.filed_date)} via {escape(item.source)}"
            "</li>"
            for item in digest.filings
        )
        news_rows = "".join(
            "<li>"
            f"{escape(item.manager_name)}: {escape(item.headline)} "
            f"({escape(item.source)}, {escape(item.published_at)})"
            "</li>"
            for item in digest.news
        )
        alert_rows = "".join(
            "<li>"
            f"{escape(item.rule_name)}: {escape(item.summary)} "
            f"({escape(item.fired_at)})"
            "</li>"
            for item in digest.alerts
        )
        body = (
            f"<h3>Filings ({len(digest.filings)})</h3><ul>{filing_rows}</ul>"
            f"<h3>News ({len(digest.news)})</h3><ul>{news_rows}</ul>"
            f"<h3>Important alerts ({len(digest.alerts)})</h3><ul>{alert_rows}</ul>"
        )
    return (
        "<html><body style='font-family:Arial,sans-serif'>"
        f"<h2>Manager activity digest ({digest.lookback_hours}h)</h2>"
        f"<p><strong>Generated at:</strong> {escape(digest.generated_at.isoformat())}</p>"
        f"{body}</body></html>"
    )


def build_digest_email_message(
    digest: DigestDocument, *, sender: str, recipients: list[str]
) -> EmailMessage:
    message = EmailMessage()
    message["Subject"] = f"Manager activity digest ({digest.lookback_hours}h)"
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message.set_content(render_digest_plain_text(digest))
    message.add_alternative(render_digest_html(digest), subtype="html")
    return message


def _split_csv(raw: str | None) -> list[str]:
    if raw is None:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


async def deliver_digest_email(digest: DigestDocument, *, dry_run: bool = False) -> DeliveryResult:
    """Send the digest through the configured email provider, or skip safely."""
    if dry_run:
        return DeliveryResult(channel="email", success=True, skipped=True, error_message="dry-run")

    sender = (os.getenv("DIGEST_EMAIL_FROM") or os.getenv("ALERT_EMAIL_FROM") or "").strip()
    recipients = _split_csv(os.getenv("DIGEST_EMAIL_TO") or os.getenv("ALERT_EMAIL_TO"))
    if not sender:
        return DeliveryResult(
            channel="email",
            success=True,
            skipped=True,
            error_message="DIGEST_EMAIL_FROM is not configured",
        )
    if not recipients:
        return DeliveryResult(
            channel="email",
            success=True,
            skipped=True,
            error_message="DIGEST_EMAIL_TO is not configured",
        )

    provider = (
        os.getenv("DIGEST_EMAIL_PROVIDER") or os.getenv("ALERT_EMAIL_PROVIDER") or "smtp"
    ).lower()
    if provider == "sendgrid":
        api_key = os.getenv("SENDGRID_API_KEY")
        if not api_key:
            return DeliveryResult(
                channel="email",
                success=True,
                skipped=True,
                error_message="SENDGRID_API_KEY is not configured",
            )
        payload = {
            "personalizations": [{"to": [{"email": recipient} for recipient in recipients]}],
            "from": {"email": sender},
            "subject": f"Manager activity digest ({digest.lookback_hours}h)",
            "content": [
                {"type": "text/plain", "value": render_digest_plain_text(digest)},
                {"type": "text/html", "value": render_digest_html(digest)},
            ],
        }
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    "https://api.sendgrid.com/v3/mail/send",
                    headers={"Authorization": f"Bearer {api_key}"},
                    json=payload,
                )
        except httpx.HTTPError as exc:
            return DeliveryResult(
                channel="email",
                success=False,
                error_message=f"SendGrid digest delivery failed: {exc}",
            )
        if response.status_code not in (200, 202):
            return DeliveryResult(
                channel="email",
                success=False,
                error_message=f"SendGrid digest delivery failed: HTTP {response.status_code}",
            )
        return DeliveryResult(channel="email", success=True)

    smtp_host = (os.getenv("SMTP_HOST") or "").strip()
    if not smtp_host:
        return DeliveryResult(
            channel="email", success=True, skipped=True, error_message="SMTP_HOST is not configured"
        )
    message = build_digest_email_message(digest, sender=sender, recipients=recipients)
    try:
        await asyncio.to_thread(
            send_email_message_via_smtp,
            message,
            host=smtp_host,
            port=int(os.getenv("SMTP_PORT", "587")),
            username=os.getenv("SMTP_USER"),
            password=os.getenv("SMTP_PASSWORD"),
            use_tls=os.getenv("ALERT_EMAIL_USE_TLS", "true").strip().lower() != "false",
            timeout_seconds=10.0,
        )
    except Exception as exc:  # pragma: no cover - defensive against network stack specifics
        return DeliveryResult(
            channel="email",
            success=False,
            error_message=f"SMTP digest delivery failed: {exc}",
        )
    return DeliveryResult(channel="email", success=True)


@flow
async def digest_flow(
    db_path: str | None = None,
    lookback_hours: int | None = None,
    dry_run: bool | None = None,
    output_path: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build and optionally deliver the automated analyst digest."""
    resolved_lookback = (
        int(os.getenv("DIGEST_LOOKBACK_HOURS", "24")) if lookback_hours is None else lookback_hours
    )
    if resolved_lookback <= 0:
        raise ValueError("lookback_hours must be greater than 0")
    resolved_dry_run = (
        os.getenv("DIGEST_DRY_RUN", "true").strip().lower() != "false"
        if dry_run is None
        else dry_run
    )
    conn = connect_db(db_path)
    try:
        digest = build_digest.fn(conn, resolved_lookback, now)
    finally:
        conn.close()

    rendered = render_digest_plain_text(digest)
    resolved_output = output_path or os.getenv("DIGEST_OUTPUT_PATH")
    artifacts: list[dict[str, Any]] = []
    if resolved_output:
        Path(resolved_output).write_text(rendered, encoding="utf-8")
    else:
        artifacts = write_artifact_bundle(
            new_run_id(),
            "digest",
            {"digest.txt": rendered},
            inputs={"lookback_hours": resolved_lookback},
        )

    delivery = await deliver_digest_email(digest, dry_run=resolved_dry_run)
    log_outcome(
        logger,
        "Digest flow completed",
        has_data=not digest.is_empty,
        extra={
            "lookback_hours": resolved_lookback,
            "filings": len(digest.filings),
            "news": len(digest.news),
            "alerts": len(digest.alerts),
            "dry_run": resolved_dry_run,
            "delivery_skipped": delivery.skipped,
        },
    )
    return {
        "lookback_hours": resolved_lookback,
        "filings": len(digest.filings),
        "news": len(digest.news),
        "alerts": len(digest.alerts),
        "empty": digest.is_empty,
        "dry_run": resolved_dry_run,
        "delivery": {
            "channel": delivery.channel,
            "success": delivery.success,
            "skipped": delivery.skipped,
            "error_message": delivery.error_message,
        },
        "rendered": rendered,
        "artifacts": artifacts,
    }


digest_deployment = digest_flow.to_deployment(name="manager-digest-daily", cron="0 13 * * *")


if __name__ == "__main__":
    asyncio.run(digest_flow())
