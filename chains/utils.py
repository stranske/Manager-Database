"""Shared formatting and context-budget helpers for chain prompts."""

from __future__ import annotations

from typing import Any


def format_holdings_table(holdings: list[dict[str, Any]], max_rows: int = 20) -> str:
    """Format holdings rows as a readable plain-text table."""
    if not holdings:
        return "(no holdings found)"

    header = "rank | issuer | cusip | shares | value_usd"
    divider = "-----|--------|-------|--------|----------"
    lines = [header, divider]

    for idx, item in enumerate(holdings[:max_rows], start=1):
        issuer = str(item.get("name_of_issuer") or "").strip() or "UNKNOWN"
        cusip = str(item.get("cusip") or "").strip() or "N/A"
        shares = int(item.get("shares") or 0)
        value = float(item.get("value_usd") or 0)
        lines.append(f"{idx} | {issuer} | {cusip} | {shares:,} | {value:,.2f}")

    return "\n".join(lines)


def format_delta_summary(diffs: list[dict[str, Any]]) -> str:
    """Format daily_diffs rows into grouped ADD/EXIT/INCREASE/DECREASE lines."""
    if not diffs:
        return "No prior-period changes available."

    buckets: dict[str, list[str]] = {
        "ADD": [],
        "EXIT": [],
        "INCREASE": [],
        "DECREASE": [],
        "OTHER": [],
    }

    for diff in diffs:
        delta_type = str(diff.get("delta_type") or "OTHER").upper()
        key = delta_type if delta_type in buckets else "OTHER"
        issuer = str(diff.get("name_of_issuer") or diff.get("cusip") or "UNKNOWN")
        prev_value = float(diff.get("value_prev") or 0)
        curr_value = float(diff.get("value_curr") or 0)
        buckets[key].append(f"{issuer} (${prev_value:,.0f} -> ${curr_value:,.0f})")

    lines: list[str] = []
    for key in ("ADD", "EXIT", "INCREASE", "DECREASE", "OTHER"):
        entries = buckets[key]
        if not entries:
            continue
        sample = "; ".join(entries[:10])
        suffix = "" if len(entries) <= 10 else f"; ... (+{len(entries) - 10} more)"
        lines.append(f"{key}: {sample}{suffix}")

    return "\n".join(lines) if lines else "No prior-period changes available."


def estimate_token_count(text: str) -> int:
    """Estimate token count using a conservative 4-chars-per-token heuristic."""
    if not text:
        return 0
    return max(1, (len(text) + 3) // 4)


def truncate_context(text: str, max_tokens: int = 4000) -> str:
    """Truncate context text to fit token budget using rough 1 token ~= 4 chars."""
    if max_tokens <= 0:
        return ""
    if estimate_token_count(text) <= max_tokens:
        return text

    max_chars = max_tokens * 4
    tail = "\n[TRUNCATED]"
    if max_chars <= len(tail):
        return tail[:max_chars]
    return f"{text[: max_chars - len(tail)]}{tail}"
