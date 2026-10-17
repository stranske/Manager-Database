"""Focused branch regressions for the ranked #1816 coverage targets."""

from __future__ import annotations

import asyncio
import sqlite3

import pytest

from adapters import asic, base
from api import signals
from etl import activism_detection, conviction_flow


def test_signals_helpers_cover_empty_legacy_and_optional_schema_paths():
    conn = sqlite3.connect(":memory:")
    try:
        assert signals._to_date("2024-01-02T12:00:00Z").isoformat() == "2024-01-02"
        assert signals._parse_manager_ids("") == []
        assert signals._parse_manager_ids('{"not":"a list"}') == []
        assert signals._parse_manager_ids('["1", "bad", null, 2.0]') == [1, 2]
        assert signals._resolve_latest_report_date(conn, "missing") is None
        assert signals._resolve_latest_manager_filing_id(conn, 1) is None
        assert signals._manager_name_lookup(conn) == {}

        conn.execute("CREATE TABLE managers (id INTEGER, name TEXT)")
        conn.executemany(
            "INSERT INTO managers VALUES (?, ?)", [(1, "Alpha"), (None, "Skip"), (2, "")]
        )
        conn.execute("CREATE TABLE contrarian_signals (report_date TEXT)")
        conn.execute("CREATE TABLE crowded_trades (report_date TEXT)")
        conn.commit()
        assert signals._manager_name_lookup(conn) == {1: "Alpha"}
        assert signals.query_contrarian_signals(conn) == []
        assert signals.query_crowded_trades(conn) == []
    finally:
        conn.close()


def test_asic_download_and_resource_filtering_cover_non_csv_payloads():
    assert (
        asic._csv_resource_url({"result": {"results": [{"resources": [None, {"url": "a.pdf"}]}]}})
        is None
    )
    assert asyncio.run(asic.download({"raw": "snapshot"})) == b"snapshot"
    assert asyncio.run(asic.download({})) == b""


def test_base_manager_column_requires_the_requested_table():
    conn = sqlite3.connect(":memory:")
    try:
        assert base.manager_id_column(conn, require_table=True) is None
    finally:
        conn.close()


def _conviction_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE managers (manager_id INTEGER PRIMARY KEY);
        CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, filed_date TEXT);
        CREATE TABLE holdings (
            filing_id INTEGER, cusip TEXT, name_of_issuer TEXT, shares INTEGER, value_usd REAL
        );
        CREATE TABLE daily_diffs (
            manager_id INTEGER, cusip TEXT, name_of_issuer TEXT, delta_type TEXT,
            shares_prev INTEGER, shares_curr INTEGER, value_prev REAL, value_curr REAL, report_date TEXT
        );
        """)
    return conn


def test_conviction_helper_branches_and_empty_scoring_paths(monkeypatch):
    conn = _conviction_conn()
    try:
        assert conviction_flow.compute_conviction_scores.fn(404, conn) == 0
        assert conviction_flow.score_all_latest_filings.fn(conn) == {
            "filings_scored": 0,
            "scores_computed": 0,
            "filings_failed": 0,
        }
        for value, expected in ((None, 3), ("", 3), ("not-a-number", 3), ("0", 1), ("7", 7)):
            if value is None:
                monkeypatch.delenv("CROWDED_TRADE_MIN_MANAGERS", raising=False)
            else:
                monkeypatch.setenv("CROWDED_TRADE_MIN_MANAGERS", value)
            assert conviction_flow._resolve_crowded_trade_min_managers() == expected
        for value, expected in ((None, 0.5), ("", 0.5), ("bad", 0.5), ("0.75", 0.75)):
            if value is None:
                monkeypatch.delenv("SIMILARITY_CROWDING_MIN_SCORE", raising=False)
            else:
                monkeypatch.setenv("SIMILARITY_CROWDING_MIN_SCORE", value)
            assert conviction_flow._resolve_similarity_crowding_min_score() == expected
        assert conviction_flow._similar_manager_ids(conn, [1], 0.5) == []
        assert [
            conviction_flow._compute_delta_value(previous, current)
            for previous, current in ((2.0, 3.0), (None, 3.0), (2.0, None), (None, None))
        ] == [1.0, 3.0, -2.0, None]
        assert [
            conviction_flow._compute_delta_shares(previous, current)
            for previous, current in ((2, 3), (None, 3), (2, None), (None, None))
        ] == [1, 3, -2, None]
    finally:
        conn.close()


def test_conviction_queries_fall_back_for_legacy_filings_schema():
    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript("""
            CREATE TABLE filings (filing_id INTEGER, manager_id INTEGER, filed_date TEXT);
            CREATE TABLE holdings (filing_id INTEGER, cusip TEXT, name_of_issuer TEXT, value_usd REAL);
            INSERT INTO filings VALUES (1, 1, '2024-01-01');
            INSERT INTO holdings VALUES (1, 'CUSIP', 'Issuer', 10.0);
            """)
        assert conviction_flow._fetch_latest_conviction_rows(conn, "2024-02-01") == [
            (1, "CUSIP", "Issuer", 10.0, 100.0)
        ]
    finally:
        conn.close()


def test_activism_serializers_thresholds_and_condition_predicates(monkeypatch):
    assert activism_detection._serialize_json({"b": 2, "a": 1}) == '{"a":1,"b":2}'
    assert activism_detection._deserialize_json_array([1, "two"]) == ["1", "two"]
    assert activism_detection._deserialize_json_array(b'["one", 2]') == ["one", "2"]
    assert activism_detection._deserialize_json_array("{}") == []
    assert activism_detection._deserialize_group_members("one| two | ") == ["one", "two"]
    assert activism_detection._deserialize_group_members('["one", "two"]') == ["one", "two"]
    assert activism_detection._deserialize_group_members(1) == []
    assert activism_detection._normalize_form_type("sc 13g/a") == "SC 13G"
    assert activism_detection._normalize_form_type("other") == "OTHER"
    monkeypatch.setenv("ACTIVISM_THRESHOLDS", " ,5,10,5")
    assert activism_detection._parse_thresholds() == (5.0, 10.0)
    monkeypatch.setenv("ACTIVISM_THRESHOLDS", "0")
    with pytest.raises(ValueError, match="positive threshold"):
        activism_detection._parse_thresholds()

    payload = {"ownership_pct": 8, "delta_pct": -3, "threshold_crossed": 5, "kind": "new"}
    assert activism_detection._condition_matches({}, payload)
    assert activism_detection._condition_matches(
        {"min_ownership_pct": 5, "min_delta_pct": 2, "threshold_crossed": 5, "kind": "new"}, payload
    )
    assert not activism_detection._condition_matches({"min_ownership_pct": 9}, payload)
    assert not activism_detection._condition_matches({"min_delta_pct": 4}, payload)
    assert not activism_detection._condition_matches({"threshold_crossed": 10}, payload)
    assert not activism_detection._condition_matches({"kind": "old"}, payload)


def test_activism_rejects_an_unsupported_connection_type():
    with pytest.raises(TypeError, match="Unsupported database connection type"):
        activism_detection.ensure_activism_events_table(object())
