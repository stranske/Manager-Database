"""Numeric parsing helpers for external trust boundaries."""

from __future__ import annotations

import math
from typing import Any


def parse_finite_float(
    value: Any,
    *,
    min_value: float | None = None,
    max_value: float | None = None,
    allow_none: bool = True,
) -> float | None:
    """Parse a finite float and optionally enforce inclusive bounds."""
    if value in (None, ""):
        if allow_none:
            return None
        raise ValueError("numeric value is required")

    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"non-finite numeric value: {value!r}")
    if min_value is not None and parsed < min_value:
        raise ValueError(f"numeric value below minimum {min_value}: {value!r}")
    if max_value is not None and parsed > max_value:
        raise ValueError(f"numeric value above maximum {max_value}: {value!r}")
    return parsed


def finite_float_or_none(
    value: Any,
    *,
    min_value: float | None = None,
    max_value: float | None = None,
) -> float | None:
    """Parse a finite float, returning None for invalid or out-of-range input."""
    try:
        return parse_finite_float(value, min_value=min_value, max_value=max_value)
    except (TypeError, ValueError):
        return None
