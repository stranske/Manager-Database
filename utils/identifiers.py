"""Identifier normalization helpers."""

from __future__ import annotations

from typing import Any


def normalize_cik(raw: Any) -> str:
    """Normalize an EDGAR CIK to the repository's zero-padded 10-digit form."""
    if raw is None:
        return ""
    if isinstance(raw, float) and raw.is_integer():
        raw = int(raw)
    cik = str(raw).strip()
    if not cik:
        return ""
    digits = "".join(ch for ch in cik if ch.isdigit())
    if not digits:
        return ""
    if len(digits) > 10:
        digits = digits[-10:]
    return digits.zfill(10)
