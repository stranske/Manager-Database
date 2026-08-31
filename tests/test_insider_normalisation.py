"""Normalising SEC Form 4 rows — where a wrong answer inverts an insider signal.

`adapters/insider.py` sits at 44.7%, and the unexercised statements are the coercions that decide
whether an insider BOUGHT or SOLD and on what date. Nothing here raises by design: a
mis-classified transaction code reports insiders accumulating a position they are actually
exiting, and the dashboard shows it as a signal.

ONE OF THESE TESTS FOUND A CRASH, and this file ships the fix beside it. `_as_date` fell back to
stripping digits and formatting them as YYYY-MM-DD, so a US-format cell "08/30/2026" produced the
well-formed-LOOKING string "0830-20-26". `date.fromisoformat` rejects month 20 — inside
`net_direction_for_rows(..., lookback_days=...)`, which does not catch it. One ordinary US-format
date crashed the whole direction calculation instead of being skipped as unreadable.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from adapters.insider import (
    _as_date,
    _as_float,
    _normalize_acquired_disposed,
    net_direction_for_rows,
)

# ---------------------------------------------------------------------------------------------
# Dates.
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value, expected",
    [
        ("2026-08-30", "2026-08-30"),
        ("2026-08-30T12:00:00", "2026-08-30"),
        ("20260830", "2026-08-30"),
        (date(2026, 8, 30), "2026-08-30"),
        (datetime(2026, 8, 30, 1, 2), "2026-08-30"),
    ],
)
def test_the_shapes_edgar_emits_are_parsed(value, expected):
    assert _as_date(value) == expected


def test_a_us_format_date_is_refused_rather_than_mangled():
    """THE CRASH THIS FILE FIXES.

    "08/30/2026" carries the same eight digits as an EDGAR date in a different order, so the
    digit fallback built "0830-20-26" — month 20. That is not merely wrong, it is unparseable,
    and it escaped as a RETURN VALUE into `net_direction_for_rows`, which calls
    `date.fromisoformat` on it without catching anything.

    Refusing it is the honest answer: the function's contract is "a date or None", and guessing
    between US and ISO ordering from digits alone cannot be done safely.
    """
    assert _as_date("08/30/2026") is None


def test_a_malformed_date_does_not_crash_the_direction_calculation():
    """The consequence, asserted end to end rather than only at the unit.

    Before the fix this raised `ValueError: month must be in 1..12` and took the whole insider
    signal down with it.
    """
    rows = [{"shares": 10, "acquired_disposed": "A", "txn_date": "08/30/2026"}]
    assert net_direction_for_rows(rows, lookback_days=90) == "net buy"


@pytest.mark.parametrize("value", [None, "", "nonsense", "2026-8-3", "1234567"])
def test_unreadable_dates_are_none(value):
    assert _as_date(value) is None


# ---------------------------------------------------------------------------------------------
# Direction: the field that says bought or sold.
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("text", ["A", "a", "ACQUIRED", "acquired", "BUY", "purchase"])
def test_acquisition_words_map_to_a(text):
    assert _normalize_acquired_disposed(text) == "A"


@pytest.mark.parametrize("text", ["D", "d", "DISPOSED", "disposed", "SELL", "sale"])
def test_disposal_words_map_to_d(text):
    assert _normalize_acquired_disposed(text) == "D"


@pytest.mark.parametrize("code, expected", [("P", "A"), ("A", "A"), ("M", "A"), ("I", "A")])
def test_acquiring_form4_codes_map_to_a(code, expected):
    """P is an open-market purchase, M an option exercise. Reading either as a sale inverts the
    signal for the most common insider events there are."""
    assert _normalize_acquired_disposed(None, code) == expected


@pytest.mark.parametrize("code, expected", [("S", "D"), ("D", "D"), ("G", "D"), ("F", "D")])
def test_disposing_form4_codes_map_to_d(code, expected):
    assert _normalize_acquired_disposed(None, code) == expected


def test_an_unknown_code_is_none_rather_than_a_default_direction():
    """Defaulting either way manufactures a signal from a code nobody classified."""
    assert _normalize_acquired_disposed(None, "Z") is None
    assert _normalize_acquired_disposed(None, None) is None


def test_an_explicit_field_wins_over_the_transaction_code():
    """The filing's own A/D column is more direct evidence than a code table's inference."""
    assert _normalize_acquired_disposed("D", "P") == "D"


def test_a_word_beginning_with_a_or_d_is_read_by_its_first_letter():
    """DOCUMENTED BEHAVIOUR WITH A SHARP EDGE, pinned rather than changed.

    The prefix fallback maps any A-word to acquired, so "Adjusted" — a real Form-4 concept that
    is NOT a purchase — becomes "A". Narrowing it is a judgement about which words appear in real
    filings, so this records the behaviour and the hazard rather than silently altering a
    classification that feeds a signal.
    """
    assert _normalize_acquired_disposed("Adjusted") == "A"


# ---------------------------------------------------------------------------------------------
# Aggregation.
# ---------------------------------------------------------------------------------------------


def test_more_bought_than_sold_is_a_net_buy():
    rows = [
        {"shares": 100, "acquired_disposed": "A"},
        {"shares": 40, "acquired_disposed": "D"},
    ]
    assert net_direction_for_rows(rows) == "net buy"


def test_more_sold_than_bought_is_a_net_sell():
    rows = [
        {"shares": 40, "acquired_disposed": "A"},
        {"shares": 100, "acquired_disposed": "D"},
    ]
    assert net_direction_for_rows(rows) == "net sell"


def test_equal_volumes_are_flat_not_unknown():
    """Flat is a measurement; unknown is the absence of one. Collapsing them would report every
    balanced quarter as "no data"."""
    rows = [
        {"shares": 50, "acquired_disposed": "A"},
        {"shares": 50, "acquired_disposed": "D"},
    ]
    assert net_direction_for_rows(rows) == "flat"


def test_no_rows_is_unknown_not_flat():
    assert net_direction_for_rows([]) == "unknown"


def test_rows_with_no_classifiable_direction_are_unknown():
    """Rows existed but none could be read, which is not the same as a balanced book."""
    assert net_direction_for_rows([{"shares": 10, "acquired_disposed": "?"}]) == "unknown"


def test_the_lookback_window_excludes_older_transactions():
    """A stale filing must not keep signalling; the window is what makes the signal current."""
    rows = [
        {"shares": 100, "acquired_disposed": "A", "txn_date": "2000-01-01"},
        {"shares": 10, "acquired_disposed": "D", "txn_date": date.today().isoformat()},
    ]
    assert net_direction_for_rows(rows, lookback_days=30) == "net sell"


# ---------------------------------------------------------------------------------------------
# Numbers.
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("value, expected", [("1.5", 1.5), (2, 2.0), (0, 0.0), ("-3", -3.0)])
def test_numeric_values_are_coerced(value, expected):
    assert _as_float(value) == expected


@pytest.mark.parametrize("value", [None, "", "abc", [1], {}])
def test_unreadable_numbers_are_none_not_zero(value):
    """Zero shares is a real filing; unreadable is not. Coercing to 0.0 would let a broken cell
    silently balance a real transaction on the other side."""
    assert _as_float(value) is None
