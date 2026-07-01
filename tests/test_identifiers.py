from __future__ import annotations

from utils.identifiers import normalize_cik


def test_normalize_cik_preserves_integer_float_values() -> None:
    assert normalize_cik(1067983.0) == "0001067983"


def test_normalize_cik_constrains_oversized_digit_strings() -> None:
    assert normalize_cik("CIK 000000001067983") == "0001067983"
