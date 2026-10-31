"""Autopilot round 1842: daily_diff_flow, managers JSON helpers, dashboard styling."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from decimal import Decimal
from unittest.mock import Mock

import pandas as pd
import pytest

from api import managers
from etl import daily_diff_flow
from ui import dashboard


def test_daily_diff_data_quality_flags_missing_numeric_fields():
    with closing(sqlite3.connect(":memory:")) as conn:
        conn.execute(
            """CREATE TABLE daily_diffs (
                manager_id INTEGER, report_date TEXT, cusip TEXT,
                shares_prev INTEGER, shares_curr INTEGER, value_prev REAL, value_curr REAL
            )"""
        )
        conn.execute(
            "INSERT INTO daily_diffs VALUES (1, '2026-05-09', '012345678', 1, 2, NULL, 10.0)"
        )
        conn.commit()
        result = daily_diff_flow._daily_diff_data_quality(conn, "2026-05-09")

    assert result["confidence"] == "low"
    assert result["missing_fields"] == [
        {"manager_id": 1, "cusip": "012345678", "field": "value_prev"}
    ]


def test_daily_diff_csv_orders_rows():
    with closing(sqlite3.connect(":memory:")) as conn:
        conn.execute(
            """CREATE TABLE daily_diffs (
                cusip TEXT, name_of_issuer TEXT, delta_type TEXT,
                shares_prev INTEGER, shares_curr INTEGER, value_prev REAL, value_curr REAL,
                report_date TEXT
            )"""
        )
        conn.executemany(
            "INSERT INTO daily_diffs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("B", "Beta", "ADD", 0, 1, 0, 1, "2026-05-09"),
                ("A", "Alpha", "EXIT", 1, 0, 5, 0, "2026-05-09"),
            ],
        )
        csv_text = daily_diff_flow._daily_diff_csv(conn, "2026-05-09")

    lines = csv_text.strip().splitlines()
    assert lines[0].startswith("cusip,")
    assert "A,Alpha" in lines[1]
    assert "B,Beta" in lines[2]


def test_large_delta_event_converts_decimal_values():
    row = {
        "manager_id": 3,
        "report_date": "2026-05-09",
        "cusip": "012345678",
        "name_of_issuer": "Issuer",
        "delta_type": "INCREASE",
        "shares_prev": 1,
        "shares_curr": 2,
        "value_prev": Decimal("10.5"),
        "value_curr": Decimal("25.25"),
    }
    event = daily_diff_flow._large_delta_event_from_diff(row)
    assert event.payload["value_prev"] == 10.5
    assert event.payload["value_curr"] == 25.25
    assert event.payload["value_usd"] == pytest.approx(14.75)


def test_resolve_local_timezone_prefers_tz_env(monkeypatch):
    monkeypatch.setenv("TZ", "America/Chicago")
    assert daily_diff_flow._resolve_local_timezone() == "America/Chicago"


def test_ensure_daily_diffs_table_reraises_non_missing_postgres_errors():
    conn = Mock()
    conn.execute.side_effect = RuntimeError("permission denied for table daily_diffs")
    with pytest.raises(RuntimeError, match="permission denied"):
        daily_diff_flow._ensure_daily_diffs_table(conn)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("a;b", ["a", "b"]),
        ("x, y", ["x", "y"]),
        ('["one","two"]', ["one", "two"]),
        (None, []),
    ],
)
def test_manager_json_array_parsing(raw, expected):
    assert managers._json_array(raw) == expected


def test_manager_json_dict_and_object_list_parsing():
    assert managers._json_dict('{"k": "v"}') == {"k": "v"}
    assert managers._json_dict("not-json") == {}
    assert managers._json_object_list('[{"a": 1}, "skip", {"b": 2}]') == [{"a": 1}, {"b": 2}]


def test_manager_escape_like_and_tag_helpers():
    assert managers._escape_like("100%_off") == "100\\%\\_off"
    assert managers._normalize_tags(["  Core ", "core", ""]) == ["Core"]
    merged = managers._merge_tags(["a"], ["b"], ["a"])
    assert merged == ["b"]


def test_to_manager_response_handles_short_legacy_rows():
    row = (5, "Fund", "0000000001", None, "alias", "US", None, None, "2026-01-01", "2026-02-01")
    response = managers._to_manager_response(row)
    assert response.manager_id == 5
    assert response.aliases == ["alias"]


def test_dashboard_helper_styling_and_badges():
    assert "198754" in dashboard._delta_type_color("ADD")
    assert dashboard._delta_type_color("OTHER") == "color: #6C757D"
    assert dashboard._topic_badges("alpha, beta ,gamma") == "`alpha` `beta` `gamma`"
    assert dashboard._topic_badges(None) == ""
    assert dashboard._event_color("threshold_crossing") == "#C1121F"
    assert dashboard._format_event_count(1) == "1 event"
    assert dashboard._format_event_count(3) == "3 events"
