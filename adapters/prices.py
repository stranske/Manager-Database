"""Free-source daily close price adapter with a local cache (#1464 / design #1401).

Prices come from a free provider (Stooq by default, yfinance when installed) and are
cached in ``price_cache`` keyed by ``resolved_ticker`` (OpenFIGI, #1374).

INTERNAL USE ONLY: the owner decision on #1464 permits a free price source for
internal metrics. Do not redistribute these prices or expose them through the
public API - only derived backtest statistics may be surfaced.
"""

from __future__ import annotations

import csv
import logging
import sqlite3
from collections.abc import Callable, Iterable, Mapping
from datetime import date, timedelta
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

from adapters.base import get_placeholder
from utils.numeric import finite_float_or_none

logger = logging.getLogger(__name__)

DEFAULT_SOURCE = "stooq"
DEFAULT_MAX_STALENESS_DAYS = 7
_STOOQ_URL = "https://stooq.com/q/d/l/?s={symbol}&d1={start}&d2={end}&i=d"
_HTTP_TIMEOUT_SECONDS = 15

# A fetcher maps a ticker + inclusive date window to {date: close}. Any provider
# that satisfies this shape can be injected, which is what keeps tests offline.
PriceFetcher = Callable[[str, date, date], Mapping[date, float]]


def _is_missing_postgres_table_error(exc: Exception) -> bool:
    message = str(exc)
    return (
        "does not exist" in message
        or getattr(exc, "pgcode", None) == "42P01"
        or "UndefinedTable" in exc.__class__.__name__
    )


def ensure_price_cache_table(conn: Any) -> None:
    """Create ``price_cache`` on SQLite; fail fast when Postgres has no schema."""
    if isinstance(conn, sqlite3.Connection):
        conn.execute("""CREATE TABLE IF NOT EXISTS price_cache (
                ticker TEXT NOT NULL,
                price_date DATE NOT NULL,
                source TEXT NOT NULL DEFAULT 'stooq',
                close_usd REAL,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, price_date, source)
            )""")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_price_cache_ticker_date "
            "ON price_cache(ticker, price_date)"
        )
        return

    try:
        conn.execute("SELECT 1 FROM price_cache LIMIT 1")
    except Exception as exc:
        if _is_missing_postgres_table_error(exc):
            raise RuntimeError(
                "price_cache table is missing on Postgres; apply schema migrations first"
            ) from exc
        raise


def _coerce_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def fetch_stooq_prices(ticker: str, start: date, end: date) -> Mapping[date, float]:
    """Fetch daily closes from Stooq's free CSV endpoint.

    Network failures are not fatal: they degrade to an empty result so a backtest
    reports a skipped position rather than crashing (#1299 finite/graceful rule).
    """
    symbol = ticker.strip().lower()
    if not symbol:
        return {}
    # Stooq expects US tickers suffixed with `.us`.
    if "." not in symbol:
        symbol = f"{symbol}.us"
    url = _STOOQ_URL.format(
        symbol=symbol,
        start=start.strftime("%Y%m%d"),
        end=end.strftime("%Y%m%d"),
    )
    try:
        request = Request(url, headers={"User-Agent": "manager-database/price-adapter"})
        with urlopen(request, timeout=_HTTP_TIMEOUT_SECONDS) as response:  # noqa: S310
            payload = response.read().decode("utf-8", errors="replace")
    except (URLError, TimeoutError, OSError):
        logger.warning("Stooq price fetch failed", extra={"ticker": ticker}, exc_info=True)
        return {}

    prices: dict[date, float] = {}
    for row in csv.DictReader(payload.splitlines()):
        row_date = _coerce_date(row.get("Date"))
        close = finite_float_or_none(row.get("Close"), min_value=0.0)
        if row_date is not None and close is not None:
            prices[row_date] = close
    return prices


def fetch_yfinance_prices(ticker: str, start: date, end: date) -> Mapping[date, float]:
    """Fetch daily closes via yfinance when the optional dependency is installed."""
    try:
        import yfinance  # type: ignore[import-not-found]
    except ImportError:
        logger.debug("yfinance is not installed; no prices fetched", extra={"ticker": ticker})
        return {}

    try:
        frame = yfinance.Ticker(ticker).history(
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),
            auto_adjust=True,
        )
    except Exception:
        logger.warning("yfinance price fetch failed", extra={"ticker": ticker}, exc_info=True)
        return {}

    prices: dict[date, float] = {}
    for index, close in getattr(frame, "Close", {}).items():
        # pandas gives a Timestamp; plain dates come through unchanged.
        to_date = getattr(index, "date", None)
        row_date = _coerce_date(to_date() if callable(to_date) else index)
        parsed = finite_float_or_none(close, min_value=0.0)
        if row_date is not None and parsed is not None:
            prices[row_date] = parsed
    return prices


_FETCHERS: dict[str, PriceFetcher] = {
    "stooq": fetch_stooq_prices,
    "yfinance": fetch_yfinance_prices,
}


class PriceAdapter:
    """Cached daily-close lookups keyed by ``resolved_ticker``.

    ``close_on_or_before`` implements as-of pricing: markets are shut on weekends
    and holidays, so a decision date frequently has no print of its own. Returning
    the most recent close within ``max_staleness_days`` keeps a backtest running
    without silently inventing a price.
    """

    def __init__(
        self,
        conn: Any,
        *,
        source: str = DEFAULT_SOURCE,
        fetcher: PriceFetcher | None = None,
        max_staleness_days: int = DEFAULT_MAX_STALENESS_DAYS,
        use_cache: bool = True,
    ) -> None:
        self.conn = conn
        self.source = source
        self.max_staleness_days = max(0, int(max_staleness_days))
        self.use_cache = use_cache
        self._fetcher = fetcher if fetcher is not None else _FETCHERS.get(source)
        self._missing: set[tuple[str, date]] = set()
        if self.use_cache:
            ensure_price_cache_table(conn)

    def _cached_window(self, ticker: str, start: date, end: date) -> dict[date, float]:
        if not self.use_cache:
            return {}
        ph = get_placeholder(self.conn)
        rows = self.conn.execute(
            "SELECT price_date, close_usd FROM price_cache "
            f"WHERE ticker = {ph} AND source = {ph} "
            f"AND price_date >= {ph} AND price_date <= {ph}",
            (ticker, self.source, start.isoformat(), end.isoformat()),
        ).fetchall()
        window: dict[date, float] = {}
        for row in rows:
            row_date = _coerce_date(row[0])
            close = finite_float_or_none(row[1], min_value=0.0)
            if row_date is not None and close is not None:
                window[row_date] = close
        return window

    def _store(self, ticker: str, prices: Mapping[date, float]) -> None:
        if not self.use_cache or not prices:
            return
        ph = get_placeholder(self.conn)
        conflict = (
            "ON CONFLICT(ticker, price_date, source) DO UPDATE SET close_usd = excluded.close_usd"
        )
        for price_date, close in prices.items():
            self.conn.execute(
                "INSERT INTO price_cache(ticker, price_date, source, close_usd) "
                f"VALUES ({ph}, {ph}, {ph}, {ph}) {conflict}",
                (ticker, price_date.isoformat(), self.source, float(close)),
            )

    def close_on_or_before(self, ticker: str | None, on: date) -> float | None:
        """Return the latest cached/fetched close at or before ``on``.

        Returns ``None`` - never raises - when the ticker is unknown, the provider
        has no print in the window, or the value is non-finite.
        """
        if not ticker:
            return None
        symbol = str(ticker).strip().upper()
        if not symbol:
            return None

        start = on - timedelta(days=self.max_staleness_days)
        window = self._cached_window(symbol, start, on)
        if not window and (symbol, on) not in self._missing:
            fetched = self._fetch(symbol, start, on)
            if fetched:
                self._store(symbol, fetched)
                window = {d: p for d, p in fetched.items() if start <= d <= on}

        if not window:
            self._missing.add((symbol, on))
            logger.warning(
                "No price available; position will be skipped",
                extra={"ticker": symbol, "as_of": on.isoformat(), "source": self.source},
            )
            return None
        return window[max(window)]

    def _fetch(self, ticker: str, start: date, end: date) -> Mapping[date, float]:
        if self._fetcher is None:
            logger.warning("No price fetcher configured", extra={"source": self.source})
            return {}
        try:
            raw = self._fetcher(ticker, start, end)
        except Exception:
            logger.warning("Price fetch raised", extra={"ticker": ticker}, exc_info=True)
            return {}
        # Sanitize at the provider boundary so a NaN/inf quote can never reach the
        # cache or a return calculation, whichever provider produced it (#1299).
        clean: dict[date, float] = {}
        for raw_date, raw_close in raw.items():
            price_date = _coerce_date(raw_date)
            close = finite_float_or_none(raw_close, min_value=0.0)
            if price_date is None or close is None:
                logger.warning(
                    "Discarding unusable quote",
                    extra={"ticker": ticker, "price_date": str(raw_date)},
                )
                continue
            clean[price_date] = close
        return clean

    def warm_cache(self, tickers: Iterable[str], start: date, end: date) -> int:
        """Pre-fetch a window for several tickers; returns the number of closes stored."""
        stored = 0
        for ticker in tickers:
            if not ticker:
                continue
            symbol = str(ticker).strip().upper()
            prices = self._fetch(symbol, start, end)
            self._store(symbol, prices)
            stored += len(prices)
        return stored


def build_price_adapter(conn: Any, *, source: str = DEFAULT_SOURCE, **kwargs: Any) -> PriceAdapter:
    """Convenience constructor so flows do not import provider details."""
    return PriceAdapter(conn, source=source, **kwargs)
