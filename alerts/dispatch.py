"""Alert delivery orchestration for fired alerts."""

from __future__ import annotations

import logging
from typing import Any

from alerts.channels import DeliveryResult, NotificationChannel
from alerts.db import (
    claim_delivery,
    insert_pending_alert,
    record_delivery_error,
    record_delivery_success,
)
from alerts.models import FiredAlert

logger = logging.getLogger(__name__)


class AlertDispatcher:
    def __init__(self, db_conn: Any, channels: dict[str, NotificationChannel]):
        self.db = db_conn
        self.channels = channels

    async def dispatch(self, fired_alerts: list[FiredAlert]) -> list[int]:
        """Dispatch a batch of fired alerts and return alert_history ids."""
        alert_ids: list[int] = []
        for alert in fired_alerts:
            alert_ids.append(await self.dispatch_single(alert))
        return alert_ids

    async def dispatch_single(self, alert: FiredAlert) -> int:
        """Persist an outbox record and attempt each channel at most once.

        This method commits the supplied connection. Callers must commit domain
        writes first. A claim without a recorded outcome means delivery is
        unknown and requires reconciliation, never an automatic resend.
        """
        alert_id = insert_pending_alert(self.db, alert)

        # The streamlit channel uses the persisted alert row as the in-app inbox record.
        for channel_name in alert.channels:
            channel = self.channels.get(channel_name)
            if channel is None:
                logger.warning(
                    "Alert channel not registered; skipping delivery",
                    extra={"channel": channel_name, "rule_id": alert.rule.rule_id},
                )
                continue

            if not claim_delivery(self.db, alert_id, channel_name):
                continue
            result = await channel.deliver(alert)
            self._record_result(alert_id, result)
            self.db.commit()
        return alert_id

    def _record_result(self, alert_id: int, result: DeliveryResult) -> None:
        if result.success and not result.skipped:
            record_delivery_success(self.db, alert_id, result.channel)
            return
        if result.success:
            return

        record_delivery_error(
            self.db,
            alert_id,
            result.channel,
            result.error_message or "Unknown delivery failure",
        )
