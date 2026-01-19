"""Logging configuration for ETL flows."""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import boto3

try:  # pragma: no cover - optional dependency for structured logs
    from pythonjsonlogger import jsonlogger as _jsonlogger
except ImportError:  # pragma: no cover
    _jsonlogger = None  # type: ignore[assignment]

jsonlogger = _jsonlogger

_LOGGING_CONFIGURED = False


class _ServiceFilter(logging.Filter):
    def __init__(self, service: str | None):
        super().__init__()
        self._service = service or "manager-database"

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "service"):
            record.service = self._service
        return True


class CloudWatchHandler(logging.Handler):
    """Send logs to AWS CloudWatch Logs."""

    def __init__(
        self,
        log_group: str,
        log_stream: str,
        *,
        region_name: str | None = None,
        endpoint_url: str | None = None,
    ) -> None:
        super().__init__()
        self._log_group = log_group
        self._log_stream = log_stream
        self._client = boto3.client("logs", region_name=region_name, endpoint_url=endpoint_url)
        self._sequence_token: str | None = None
        self._ensure_log_stream()

    def _ensure_log_stream(self) -> None:
        try:
            self._client.create_log_group(logGroupName=self._log_group)
        except self._client.exceptions.ResourceAlreadyExistsException:
            pass
        try:
            self._client.create_log_stream(
                logGroupName=self._log_group,
                logStreamName=self._log_stream,
            )
        except self._client.exceptions.ResourceAlreadyExistsException:
            pass

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
            event = {"timestamp": int(record.created * 1000), "message": message}
            kwargs: dict[str, Any] = {
                "logGroupName": self._log_group,
                "logStreamName": self._log_stream,
                "logEvents": [event],
            }
            if self._sequence_token is not None:
                kwargs["sequenceToken"] = self._sequence_token
            response = self._client.put_log_events(**kwargs)
            self._sequence_token = response.get("nextSequenceToken")
        except Exception:
            self.handleError(record)


def _build_formatter() -> logging.Formatter:
    if jsonlogger is None:
        return logging.Formatter(fmt="%(asctime)s %(levelname)s %(name)s %(service)s %(message)s")
    return jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(service)s %(message)s")


def configure_logging(service_name: str | None = None) -> None:
    """Configure structured logging with optional CloudWatch output."""
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    root = logging.getLogger()
    root.setLevel(log_level)

    formatter = _build_formatter()
    service_filter = _ServiceFilter(service_name)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.addFilter(service_filter)
    root.addHandler(stream_handler)

    log_group = os.getenv("CLOUDWATCH_LOG_GROUP")
    if log_group:
        log_stream = os.getenv(
            "CLOUDWATCH_LOG_STREAM",
            f"{service_name or 'manager-database'}-{int(time.time())}",
        )
        region_name = os.getenv("AWS_REGION")
        endpoint_url = os.getenv("CLOUDWATCH_ENDPOINT")
        cw_handler = CloudWatchHandler(
            log_group,
            log_stream,
            region_name=region_name,
            endpoint_url=endpoint_url,
        )
        cw_handler.setFormatter(formatter)
        cw_handler.addFilter(service_filter)
        root.addHandler(cw_handler)

    _LOGGING_CONFIGURED = True


def reset_logging() -> None:
    """Reset logging configuration for tests."""
    global _LOGGING_CONFIGURED
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    _LOGGING_CONFIGURED = False


def log_outcome(
    logger: logging.Logger,
    message: str,
    *,
    has_data: bool | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    """Log completion outcomes at info or warning levels."""
    level = logging.INFO
    if has_data is False:
        level = logging.WARNING
    logger.log(level, message, extra=extra)
