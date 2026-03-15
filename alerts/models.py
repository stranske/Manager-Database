"""Canonical alert models shared by API, ETL, and UI layers."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator

ALERT_EVENT_TYPES = (
    "new_filing",
    "large_delta",
    "news_spike",
    "crowded_trade_change",
    "contrarian_signal",
    "missing_filing",
    "etl_failure",
    "activism_event",
)
ALERT_CHANNELS = ("email", "slack", "streamlit")
_CHANNEL_ALIASES = {
    "in_app": "streamlit",
    "webhook": "slack",
}


def normalize_event_type(value: str) -> str:
    normalized = value.strip()
    if normalized not in ALERT_EVENT_TYPES:
        raise ValueError(f"Unsupported event_type: {normalized}")
    return normalized


def normalize_channels(channels: list[str]) -> list[str]:
    normalized: list[str] = []
    for channel in channels:
        candidate = channel.strip()
        if not candidate:
            raise ValueError("Channels cannot be empty.")
        candidate = _CHANNEL_ALIASES.get(candidate, candidate)
        if candidate not in ALERT_CHANNELS:
            raise ValueError(f"Unsupported channel: {candidate}")
        if candidate not in normalized:
            normalized.append(candidate)
    if not normalized:
        raise ValueError("At least one delivery channel is required.")
    return normalized


class AlertRuleBase(BaseModel):
    name: str = Field(..., description="Rule name")
    description: str | None = Field(None, description="Optional human-readable description")
    event_type: str = Field(..., description="Alert event type")
    condition_json: dict[str, Any] = Field(default_factory=dict, description="Rule condition JSON")
    channels: list[str] = Field(default_factory=lambda: ["streamlit"])
    enabled: bool = Field(True, description="Whether the rule is enabled")
    manager_id: int | None = Field(None, description="Optional manager filter")
    created_by: str | None = Field(None, description="Optional rule author")

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Rule name is required.")
        return normalized

    @field_validator("event_type")
    @classmethod
    def _validate_event_type(cls, value: str) -> str:
        return normalize_event_type(value)

    @field_validator("channels")
    @classmethod
    def _validate_channels(cls, value: list[str]) -> list[str]:
        return normalize_channels(value)


class AlertRuleCreate(AlertRuleBase):
    """Payload for creating an alert rule."""


class AlertRuleUpdate(BaseModel):
    """Patch payload for an existing alert rule."""

    name: str | None = None
    description: str | None = None
    condition_json: dict[str, Any] | None = None
    channels: list[str] | None = None
    enabled: bool | None = None
    created_by: str | None = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str | None) -> str | None:
        if value is None:
            return value
        normalized = value.strip()
        if not normalized:
            raise ValueError("Rule name is required.")
        return normalized

    @field_validator("channels")
    @classmethod
    def _validate_channels(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return value
        return normalize_channels(value)


class AlertRule(AlertRuleBase):
    """Stored alert rule."""

    rule_id: int
    created_at: datetime
    updated_at: datetime


class AlertEvent(BaseModel):
    """Input event evaluated against enabled alert rules."""

    event_type: str
    manager_id: int | None = None
    payload: dict[str, Any]
    occurred_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Event timestamp used by time-window conditions",
    )

    @field_validator("event_type")
    @classmethod
    def _validate_event_type(cls, value: str) -> str:
        return normalize_event_type(value)


class FiredAlert(BaseModel):
    """A rule/event pair ready to be recorded or delivered."""

    rule: AlertRule
    event: AlertEvent
    channels: list[str]

    @field_validator("channels")
    @classmethod
    def _validate_channels(cls, value: list[str]) -> list[str]:
        return normalize_channels(value)
