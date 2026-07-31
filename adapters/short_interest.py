"""FINRA-style short-interest payload normalization for held issuers (#1470)."""

from __future__ import annotations

import csv
import json
import math
import os
from collections.abc import Callable, Iterable, Mapping
from datetime import date, datetime
from io import StringIO
from typing import Any
from urllib.request import Request, urlopen

DEFAULT_FINRA_SHORT_INTEREST_URL = "https://api.finra.org/data/group/otcMarket/name/shortInterest"
HTTP_TIMEOUT_SECONDS = 15


class ShortInterestFetchError(RuntimeError):
    """Raised when a short-interest response cannot be fetched or parsed."""


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _first_present(raw: Mapping[str, Any], *keys: str) -> Any:
    """Return the first key whose value is present, keeping numeric zero."""
    for key in keys:
        value = raw.get(key)
        if value is not None and value != "":
            return value
    return None


def _finite_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(str(value).replace(",", "").replace("%", ""))
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _date_value(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = _text(value)
    if text is None:
        return None
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return _calendar_date(text[:10])
    digits = "".join(char for char in text if char.isdigit())
    if len(digits) < 8:
        return None
    return _calendar_date(f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}")


def _calendar_date(text: str) -> str | None:
    """Return ``text`` only when it is a real calendar date; ``report_date`` is NOT NULL."""
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return None


def normalize_short_interest_row(
    raw: Mapping[str, Any], *, default_ticker: str | None = None
) -> dict[str, Any] | None:
    """Normalize one FINRA/exchange row into the stable short-interest contract."""
    ticker = _text(
        raw.get("ticker") or raw.get("symbol") or raw.get("issueSymbol") or default_ticker
    )
    if ticker is None:
        return None
    report_date = _date_value(_first_present(raw, "report_date", "settlementDate", "date"))
    if report_date is None:
        return None
    short_interest = _finite_float(
        _first_present(raw, "short_interest", "shortInterest", "shortInterestQuantity")
    )
    float_shares = _finite_float(
        _first_present(raw, "float_shares", "floatShares", "sharesOutstanding")
    )
    short_interest_pct = _finite_float(
        _first_present(raw, "short_interest_pct", "shortInterestPct", "shortInterestPercent")
    )
    if (
        short_interest_pct is None
        and short_interest is not None
        and float_shares
        and float_shares > 0
    ):
        short_interest_pct = short_interest / float_shares * 100
    if short_interest is None and short_interest_pct is None:
        return None
    return {
        "ticker": ticker.upper(),
        "cusip": _text(raw.get("cusip") or raw.get("cusipNumber")),
        "short_interest": short_interest,
        "float_shares": float_shares,
        "short_interest_pct": short_interest_pct,
        "report_date": report_date,
        "source": _text(raw.get("source")) or "finra",
    }


def normalize_short_interest_rows(
    rows: Iterable[Mapping[str, Any]], *, default_ticker: str | None = None
) -> list[dict[str, Any]]:
    return [
        normalized
        for raw in rows
        if (normalized := normalize_short_interest_row(raw, default_ticker=default_ticker))
        is not None
    ]


def _decode_payload(payload: str) -> list[Mapping[str, Any]]:
    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError:
        return list(csv.DictReader(StringIO(payload)))
    if isinstance(decoded, list):
        return [row for row in decoded if isinstance(row, Mapping)]
    if isinstance(decoded, Mapping):
        for key in ("data", "results", "items"):
            rows = decoded.get(key)
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, Mapping)]
    raise ShortInterestFetchError("short-interest response did not contain rows")


def short_interest_url() -> str:
    """Resolve the feed URL, treating a blank override as unset."""
    return os.getenv("FINRA_SHORT_INTEREST_URL", "").strip() or DEFAULT_FINRA_SHORT_INTEREST_URL


def _default_fetcher(ticker: str) -> list[Mapping[str, Any]]:
    url = short_interest_url()
    request = Request(
        url, headers={"Accept": "application/json", "User-Agent": "Manager-Database/1.0"}
    )
    try:
        with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:  # noqa: S310
            payload = response.read().decode("utf-8")
    except OSError as exc:  # pragma: no cover - live endpoint variability
        raise ShortInterestFetchError(f"short-interest fetch failed for {ticker}: {exc}") from exc
    return _decode_payload(payload)


Fetcher = Callable[[str], Iterable[Mapping[str, Any]]]


def fetch_short_interest(
    ticker: str,
    *,
    fetcher: Fetcher | None = None,
) -> list[dict[str, Any]]:
    """Fetch and normalize records for a ticker; stubs keep tests/network policy deterministic."""
    active = fetcher or _default_fetcher
    try:
        rows = active(ticker)
    except ShortInterestFetchError:
        raise
    except Exception as exc:
        raise ShortInterestFetchError(f"short-interest fetch failed for {ticker}: {exc}") from exc
    return normalize_short_interest_rows(rows, default_ticker=ticker)
