"""Identifier normalization helpers."""

from __future__ import annotations

from typing import Any


def normalize_cik(raw: Any) -> str:
    """Normalize an EDGAR CIK to the repository's zero-padded 10-digit form."""
    cik = "" if raw is None else str(raw).strip()
    if not cik:
        return ""
    digits = "".join(ch for ch in cik if ch.isdigit())
    if not digits:
        return ""
    return digits.zfill(10)
