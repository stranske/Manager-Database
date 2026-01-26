"""Parse external API responses for the /api/data endpoint."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ParseResult:
    """Normalized parse outcome for upstream responses."""

    ok: bool
    data: Any | None
    error: str | None


def _malformed_result() -> ParseResult:
    return ParseResult(
        ok=False,
        data=None,
        error="Malformed JSON response from upstream.",
    )


def parse_response(raw: str | bytes | None) -> ParseResult:
    """Parse upstream JSON, returning structured errors for malformed payloads."""
    if raw is None:
        return _malformed_result()
    if isinstance(raw, bytes):
        raw_text = raw.decode("utf-8", errors="replace")
    else:
        raw_text = raw
    if not raw_text.strip():
        return _malformed_result()
    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError:
        return _malformed_result()
    if not isinstance(payload, dict):
        return _malformed_result()
    if "data" not in payload:
        return _malformed_result()
    return ParseResult(ok=True, data=payload["data"], error=None)


# Alias for parity with task naming.
def parseResponse(raw: str | bytes | None) -> ParseResult:  # noqa: N802
    return parse_response(raw)
