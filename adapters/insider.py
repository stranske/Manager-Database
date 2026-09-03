"""Form-4 / insider transaction fetch + normalize helpers (edgartools-backed)."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from datetime import date, datetime, timedelta
from typing import Any

ROW_KEYS = (
    "issuer_cik",
    "ticker",
    "insider_name",
    "txn_code",
    "shares",
    "txn_date",
    "acquired_disposed",
)


class InsiderFetchError(RuntimeError):
    """Raised when an issuer's Form-4 fetch fails."""


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_date(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        candidate = text[:10]
        try:
            date.fromisoformat(candidate)
        except ValueError:
            return None
        return candidate
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        # The digit fallback assumes YYYYMMDD, which is what EDGAR emits. A US-format value like
        # "08/30/2026" yields the same eight digits in a different order and produced the
        # well-formed-LOOKING string "0830-20-26" — month 20, which `date.fromisoformat` rejects.
        # That escaped this function as a return value and raised inside
        # `net_direction_for_rows(..., lookback_days=...)`, so one ordinary US-format cell crashed
        # the whole direction calculation rather than being skipped as unreadable.
        candidate = f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
        try:
            date.fromisoformat(candidate)
        except ValueError:
            return None
        return candidate
    return None


def _normalize_acquired_disposed(value: Any, txn_code: str | None = None) -> str | None:
    text = _as_str(value)
    if text:
        upper = text.upper()
        if upper in {"A", "ACQUIRED", "BUY", "PURCHASE"}:
            return "A"
        if upper in {"D", "DISPOSED", "SELL", "SALE"}:
            return "D"
        if upper[:1] in {"A", "D"}:
            return upper[:1]
    code = (_as_str(txn_code) or "").upper()
    # Common Form-4 codes: P/A often acquire; S/D/G often dispose.
    if code in {"P", "A", "M", "I"}:
        return "A"
    if code in {"S", "D", "G", "F"}:
        return "D"
    return None


def normalize_form4_row(
    raw: Mapping[str, Any], *, default_ticker: str | None = None
) -> dict[str, Any]:
    """Normalize one raw Form-4 row into the stable insider contract."""
    issuer_cik = _as_str(
        raw.get("issuer_cik") or raw.get("cik") or raw.get("issuerCik") or raw.get("issuer")
    )
    if issuer_cik:
        issuer_cik = issuer_cik.zfill(10) if issuer_cik.isdigit() else issuer_cik
    ticker = _as_str(raw.get("ticker") or raw.get("Ticker") or raw.get("symbol") or default_ticker)
    insider_name = _as_str(
        raw.get("insider_name")
        or raw.get("Insider")
        or raw.get("owner")
        or raw.get("reporting_owner")
        or raw.get("reportingOwner")
    )
    txn_code = _as_str(
        raw.get("txn_code") or raw.get("Code") or raw.get("transaction_code") or raw.get("code")
    )
    shares = None
    for key in ("shares", "Shares", "transactionShares", "shares_traded"):
        shares = _as_float(raw.get(key))
        if shares is not None:
            break
    txn_date = _as_date(
        raw.get("txn_date")
        or raw.get("Date")
        or raw.get("transaction_date")
        or raw.get("transactionDate")
    )
    acquired_disposed = _normalize_acquired_disposed(
        raw.get("acquired_disposed")
        or raw.get("acquiredDisposedCode")
        or raw.get("acquisition_or_disposition"),
        txn_code,
    )
    return {
        "issuer_cik": issuer_cik,
        "ticker": ticker,
        "insider_name": insider_name,
        "txn_code": txn_code,
        "shares": shares,
        "txn_date": txn_date,
        "acquired_disposed": acquired_disposed,
    }


def normalize_form4_rows(
    rows: Iterable[Mapping[str, Any]], *, default_ticker: str | None = None
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for raw in rows:
        row = normalize_form4_row(raw, default_ticker=default_ticker)
        if not row.get("issuer_cik"):
            continue
        normalized.append(row)
    return normalized


def _default_edgartools_fetcher(issuer: str, *, lookback_days: int) -> list[dict[str, Any]]:
    """Fetch recent Form-4 transactions for an issuer via edgartools when installed."""
    try:
        from edgar import Company  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - environment-dependent
        raise InsiderFetchError("edgartools is required for Form-4 fetch") from exc

    try:
        company = Company(issuer)
        filings = company.get_filings(form="4")
        if hasattr(filings, "head"):
            filings = filings.head(40)
        cutoff = date.today() - timedelta(days=lookback_days)
        out: list[dict[str, Any]] = []
        for filing in filings:
            try:
                form4 = filing.obj() if hasattr(filing, "obj") else filing
            except Exception:
                continue
            filed = _as_date(getattr(filing, "filing_date", None) or getattr(filing, "filed", None))
            if filed and date.fromisoformat(filed) < cutoff:
                continue
            ticker = _as_str(getattr(company, "tickers", None))
            tickers_attr = getattr(company, "tickers", None)
            if isinstance(tickers_attr, (list, tuple)):
                ticker = _as_str(tickers_attr[0]) if tickers_attr else None
            transactions = getattr(form4, "transactions", None) or getattr(
                form4, "non_derivative_transactions", None
            )
            if transactions is None and hasattr(form4, "to_dataframe"):
                try:
                    frame = form4.to_dataframe()
                    transactions = frame.to_dict(orient="records") if frame is not None else []
                except Exception:
                    transactions = []
            owner = None
            for attr in ("reporting_owner", "owner", "reporting_owners"):
                owner = getattr(form4, attr, None)
                if owner:
                    break
            owner_name = None
            if isinstance(owner, str):
                owner_name = owner
            elif owner is not None:
                owner_name = _as_str(
                    getattr(owner, "name", None)
                    or getattr(owner, "owner_name", None)
                    or (owner[0] if isinstance(owner, (list, tuple)) and owner else None)
                )
            cik = _as_str(getattr(company, "cik", None) or issuer)
            for txn in transactions or []:
                if isinstance(txn, Mapping):
                    raw = dict(txn)
                else:
                    raw = {
                        "txn_code": getattr(txn, "transaction_code", None)
                        or getattr(txn, "code", None),
                        "shares": getattr(txn, "shares", None)
                        or getattr(txn, "transaction_shares", None),
                        "txn_date": getattr(txn, "transaction_date", None)
                        or getattr(txn, "date", None)
                        or filed,
                        "acquired_disposed": getattr(txn, "acquired_or_disposed", None)
                        or getattr(txn, "acquisition_or_disposition", None),
                        "insider_name": owner_name,
                    }
                raw.setdefault("issuer_cik", cik)
                raw.setdefault("ticker", ticker)
                raw.setdefault("insider_name", owner_name)
                raw.setdefault("txn_date", filed)
                out.append(raw)
        return out
    except InsiderFetchError:
        raise
    except Exception as exc:  # pragma: no cover - live SEC variability
        raise InsiderFetchError(f"Form-4 fetch failed for {issuer}: {exc}") from exc


Fetcher = Callable[..., list[Mapping[str, Any]]]


def fetch_form4_transactions(
    issuer: str,
    *,
    lookback_days: int = 90,
    ticker: str | None = None,
    fetcher: Fetcher | None = None,
) -> list[dict[str, Any]]:
    """Fetch and normalize Form-4 rows for one issuer (CIK or ticker)."""
    active = fetcher or _default_edgartools_fetcher
    try:
        raw_rows = active(issuer, lookback_days=lookback_days)
    except TypeError:
        # Allow simple stubs: fetcher(issuer) -> rows
        raw_rows = active(issuer)  # type: ignore[misc, call-arg]
    return normalize_form4_rows(raw_rows, default_ticker=ticker)


def net_direction_for_rows(
    rows: Iterable[Mapping[str, Any]], *, lookback_days: int | None = None
) -> str:
    """
    Derive net insider direction from normalized rows.

    Returns one of: "net buy", "net sell", "flat", "unknown".
    """
    cutoff: date | None = None
    if lookback_days is not None:
        cutoff = date.today() - timedelta(days=lookback_days)

    buy = 0.0
    sell = 0.0
    saw_any = False
    for row in rows:
        txn_date = _as_date(row.get("txn_date"))
        if cutoff and txn_date and date.fromisoformat(txn_date) < cutoff:
            continue
        shares = _as_float(row.get("shares")) or 0.0
        ad = _normalize_acquired_disposed(
            row.get("acquired_disposed"), _as_str(row.get("txn_code"))
        )
        if ad is None:
            continue
        saw_any = True
        if ad == "A":
            buy += abs(shares)
        elif ad == "D":
            sell += abs(shares)
    if not saw_any:
        return "unknown"
    if buy > sell:
        return "net buy"
    if sell > buy:
        return "net sell"
    return "flat"
