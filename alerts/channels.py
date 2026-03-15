"""Notification channel implementations for fired alerts."""

from __future__ import annotations

import asyncio
import logging
import os
import smtplib
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Any

import httpx

from alerts.formatter import format_email_html, format_plain_text, format_slack_blocks
from alerts.models import FiredAlert

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class DeliveryResult:
    channel: str
    success: bool
    error_message: str | None = None
    skipped: bool = False


class NotificationChannel(ABC):
    channel_name: str

    @abstractmethod
    async def deliver(self, alert: FiredAlert) -> DeliveryResult:
        """Send one fired alert through this channel."""


def _split_csv(raw: str | None) -> list[str]:
    if raw is None:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


def _warn_skip(channel: str, reason: str) -> DeliveryResult:
    logger.warning("Skipping alert delivery", extra={"channel": channel, "reason": reason})
    return DeliveryResult(channel=channel, success=True, skipped=True, error_message=reason)


def _build_email_message(
    alert: FiredAlert,
    *,
    sender: str,
    recipients: list[str],
) -> EmailMessage:
    message = EmailMessage()
    message["Subject"] = alert.rule.name
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message.set_content(format_plain_text(alert))
    message.add_alternative(format_email_html(alert), subtype="html")
    return message


def _send_via_smtp(
    message: EmailMessage,
    *,
    host: str,
    port: int,
    username: str | None,
    password: str | None,
    use_tls: bool,
    timeout_seconds: float,
) -> None:
    with smtplib.SMTP(host, port, timeout=timeout_seconds) as smtp:
        if use_tls:
            smtp.starttls()
        if username:
            smtp.login(username, password or "")
        smtp.send_message(message)


class EmailChannel(NotificationChannel):
    """Send HTML email alerts using SMTP or SendGrid."""

    channel_name = "email"

    def __init__(
        self,
        *,
        provider: str | None = None,
        sender: str | None = None,
        recipients: list[str] | None = None,
        smtp_host: str | None = None,
        smtp_port: int | None = None,
        smtp_user: str | None = None,
        smtp_password: str | None = None,
        sendgrid_api_key: str | None = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        configured_provider = (
            provider if provider is not None else os.getenv("ALERT_EMAIL_PROVIDER")
        )
        self.provider = (configured_provider or "smtp").strip().lower()
        self.sender = (sender or os.getenv("ALERT_EMAIL_FROM") or "").strip()
        self.recipients = recipients or _split_csv(os.getenv("ALERT_EMAIL_TO"))
        self.smtp_host = (smtp_host or os.getenv("SMTP_HOST") or "").strip()
        self.smtp_port = smtp_port or int(os.getenv("SMTP_PORT", "587"))
        self.smtp_user = smtp_user or os.getenv("SMTP_USER")
        self.smtp_password = smtp_password or os.getenv("SMTP_PASSWORD")
        self.sendgrid_api_key = sendgrid_api_key or os.getenv("SENDGRID_API_KEY")
        self.timeout_seconds = timeout_seconds
        self.use_tls = os.getenv("ALERT_EMAIL_USE_TLS", "true").strip().lower() != "false"

    async def deliver(self, alert: FiredAlert) -> DeliveryResult:
        if not self.sender:
            return _warn_skip(self.channel_name, "ALERT_EMAIL_FROM is not configured")
        if not self.recipients:
            return _warn_skip(self.channel_name, "ALERT_EMAIL_TO is not configured")

        if self.provider == "sendgrid":
            return await self._deliver_sendgrid(alert)
        return await self._deliver_smtp(alert)

    async def _deliver_smtp(self, alert: FiredAlert) -> DeliveryResult:
        if not self.smtp_host:
            return _warn_skip(self.channel_name, "SMTP_HOST is not configured")

        message = _build_email_message(alert, sender=self.sender, recipients=self.recipients)
        try:
            await asyncio.to_thread(
                _send_via_smtp,
                message,
                host=self.smtp_host,
                port=self.smtp_port,
                username=self.smtp_user,
                password=self.smtp_password,
                use_tls=self.use_tls,
                timeout_seconds=self.timeout_seconds,
            )
        except Exception as exc:  # pragma: no cover - defensive against network stack specifics
            return DeliveryResult(
                channel=self.channel_name,
                success=False,
                error_message=f"SMTP delivery failed: {exc}",
            )
        return DeliveryResult(channel=self.channel_name, success=True)

    async def _deliver_sendgrid(self, alert: FiredAlert) -> DeliveryResult:
        if not self.sendgrid_api_key:
            return _warn_skip(self.channel_name, "SENDGRID_API_KEY is not configured")

        message = {
            "personalizations": [{"to": [{"email": recipient} for recipient in self.recipients]}],
            "from": {"email": self.sender},
            "subject": alert.rule.name,
            "content": [
                {"type": "text/plain", "value": format_plain_text(alert)},
                {"type": "text/html", "value": format_email_html(alert)},
            ],
        }
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.post(
                    "https://api.sendgrid.com/v3/mail/send",
                    headers={"Authorization": f"Bearer {self.sendgrid_api_key}"},
                    json=message,
                )
        except httpx.HTTPError as exc:
            return DeliveryResult(
                channel=self.channel_name,
                success=False,
                error_message=f"SendGrid delivery failed: {exc}",
            )

        if response.status_code not in (200, 202):
            return DeliveryResult(
                channel=self.channel_name,
                success=False,
                error_message=f"SendGrid delivery failed: HTTP {response.status_code}",
            )
        return DeliveryResult(channel=self.channel_name, success=True)


class SlackChannel(NotificationChannel):
    """Send Slack webhook notifications with simple rate limiting and retry."""

    channel_name = "slack"

    def __init__(
        self,
        *,
        webhook_url: str | None = None,
        timeout_seconds: float = 10.0,
        min_interval_seconds: float = 1.0,
    ) -> None:
        self.webhook_url = (webhook_url or os.getenv("ALERT_SLACK_WEBHOOK_URL") or "").strip()
        self.timeout_seconds = timeout_seconds
        self.min_interval_seconds = min_interval_seconds
        self._lock = asyncio.Lock()
        self._last_sent_monotonic = 0.0

    async def deliver(self, alert: FiredAlert) -> DeliveryResult:
        if not self.webhook_url:
            return _warn_skip(self.channel_name, "ALERT_SLACK_WEBHOOK_URL is not configured")

        payload = format_slack_blocks(alert)
        await self._respect_rate_limit()
        return await self._post_with_retry(payload)

    async def _respect_rate_limit(self) -> None:
        async with self._lock:
            delay = self.min_interval_seconds - (time.monotonic() - self._last_sent_monotonic)
            if delay > 0:
                await asyncio.sleep(delay)
            self._last_sent_monotonic = time.monotonic()

    async def _post_with_retry(self, payload: dict[str, Any]) -> DeliveryResult:
        last_error: str | None = None
        for attempt in range(2):
            try:
                async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                    response = await client.post(self.webhook_url, json=payload)
            except httpx.HTTPError as exc:
                last_error = f"Slack delivery failed: {exc}"
            else:
                if 200 <= response.status_code < 300:
                    return DeliveryResult(channel=self.channel_name, success=True)
                if response.status_code not in (429, 500, 502, 503, 504):
                    return DeliveryResult(
                        channel=self.channel_name,
                        success=False,
                        error_message=f"Slack delivery failed: HTTP {response.status_code}",
                    )
                last_error = f"Slack delivery failed: HTTP {response.status_code}"

            if attempt == 0:
                await asyncio.sleep(self.min_interval_seconds)

        return DeliveryResult(channel=self.channel_name, success=False, error_message=last_error)


class StreamlitChannel(NotificationChannel):
    """In-app delivery is represented by the alert_history record itself."""

    channel_name = "streamlit"

    async def deliver(self, alert: FiredAlert) -> DeliveryResult:
        _ = alert
        return DeliveryResult(channel=self.channel_name, success=True)


def build_configured_channels() -> dict[str, NotificationChannel]:
    """Build the default channel set from environment configuration."""
    return {
        "email": EmailChannel(),
        "slack": SlackChannel(),
        "streamlit": StreamlitChannel(),
    }
