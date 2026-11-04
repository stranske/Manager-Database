"""Behavior checks for the remaining search, signal, and report boundaries."""

import datetime as dt
import sqlite3
from unittest.mock import Mock

import pytest

from api import search, signals
from ui import daily_report


def test_search_legacy_holdings_return_filing_groups_and_holdings():
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute(
            "CREATE TABLE holdings (cik TEXT, accession TEXT, filed TEXT, nameOfIssuer TEXT, cusip TEXT)"
        )
        conn.executemany(
            "INSERT INTO holdings VALUES (?, ?, ?, ?, ?)",
            [
                ("123", "needle-1", "2024-05-01", "needle Corp", "AAA"),
                ("123", "needle-1", "2024-05-01", "Other", "BBB"),
            ],
        )
        results = search.universal_search("needle", conn)
        assert [
            (r.entity_type, r.entity_id, r.manager_name, r.headline, r.snippet, r.timestamp)
            for r in results
        ] == [
            ("filing", 1, "123", "Filing needle-1", "2 holdings; filed 2024-05-01", "2024-05-01"),
            (
                "holding",
                1,
                "123",
                "needle Corp (AAA)",
                "Accession needle-1 filed 2024-05-01",
                "2024-05-01",
            ),
        ]
    finally:
        conn.close()


def test_search_vector_documents_keep_shared_managers_and_deduplicate(monkeypatch, tmp_path):
    import embeddings

    conn = sqlite3.connect(tmp_path / "documents.db")
    try:
        conn.executescript("""
            CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE documents (doc_id INTEGER PRIMARY KEY, manager_id INTEGER, filename TEXT, text TEXT);
            CREATE TABLE document_managers (doc_id INTEGER, manager_id INTEGER);
            INSERT INTO managers VALUES (1, 'Alpha'), (2, 'Beta');
            INSERT INTO documents VALUES (1, NULL, 'memo', 'needle memo'), (2, 1, NULL, 'vector only result');
            INSERT INTO document_managers VALUES (2, 2), (2, 1), (2, 999);
        """)
        conn.commit()
        vector = Mock(
            return_value=[
                {"content": ""},
                {"content": "absent"},
                {"content": "vector only result", "distance": 0.2},
                {"content": "vector only result", "distance": 0.8},
            ]
        )
        monkeypatch.setattr(embeddings, "search_documents", vector)

        results = search.universal_search("needle", conn, entity_type="document")

        assert [(r.entity_id, r.manager_name, r.headline, r.snippet) for r in results] == [
            (1, None, "memo", "needle memo"),
            (2, "Alpha, Beta", "vector only result", "vector only result"),
        ]
        assert [r.relevance for r in results] == pytest.approx([0.7, 0.62])
        vector.assert_called_once_with("needle", db_path=str(tmp_path / "documents.db"), k=20)
    finally:
        conn.close()


def test_search_postgres_activism_preserves_filing_context(monkeypatch):
    monkeypatch.setattr(search, "table_exists", lambda conn, table: table == "activism_filings")
    conn = Mock()
    conn.execute.return_value.fetchall.return_value = [
        (
            12,
            "Alpha",
            "SC 13D",
            "Target",
            "CUSIP",
            5.5,
            "2024-05-01",
            "https://example.test/12",
            0.5,
        )
    ]

    results = search.universal_search("Target", conn, entity_type="filing")

    assert len(results) == 1
    assert results[0].model_dump() == {
        "entity_type": "filing",
        "entity_id": 12,
        "manager_name": "Alpha",
        "headline": "13D Filing: Alpha -> Target (5.5%)",
        "snippet": "SC 13D | CUSIP: CUSIP | Filed: 2024-05-01",
        "relevance": pytest.approx(0.8666666667),
        "url": "https://example.test/12",
        "timestamp": "2024-05-01",
    }
    sql, params = conn.execute.call_args.args
    assert "FROM activism_filings af" in sql
    assert params == ("Target", "Target", "Target", 20)


def test_conviction_annotation_survives_unavailable_holdings(monkeypatch):
    from etl import insider_flow

    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript("""
            CREATE TABLE conviction_scores (
                manager_id INTEGER, filing_id INTEGER, cusip TEXT, name_of_issuer TEXT,
                value_usd REAL, conviction_pct REAL, portfolio_weight REAL
            );
            CREATE TABLE insider_transactions (ticker TEXT);
            INSERT INTO conviction_scores VALUES (1, 2, 'AAA', 'Issuer', 300, 50, 0.5);
        """)
        annotate = Mock(return_value="BUY")
        monkeypatch.setattr(insider_flow, "insider_net_direction_for_ticker", annotate)

        rows = signals.query_conviction_scores(conn, 1, filing_id=2)

        assert [row.model_dump() for row in rows] == [
            {
                "cusip": "AAA",
                "name_of_issuer": "Issuer",
                "value_usd": 300.0,
                "conviction_pct": 50.0,
                "portfolio_weight": 0.5,
                "insider_net_direction": "BUY",
                "short_interest_pct": None,
                "short_interest_report_date": None,
                "short_interest_source": None,
            }
        ]
        annotate.assert_called_once_with(conn, None, cusip="AAA")
    finally:
        conn.close()


@pytest.mark.parametrize(
    "loader, query",
    [
        ("load_crowded_trades", "query_crowded_trades"),
        ("load_contrarian_signals", "query_contrarian_signals"),
    ],
)
def test_report_signal_query_failure_returns_empty_and_closes(monkeypatch, loader, query):
    conn = Mock()
    failing_query = Mock(side_effect=sqlite3.OperationalError("database unavailable"))
    monkeypatch.setattr(daily_report, "connect_db", lambda: conn)
    monkeypatch.setattr(daily_report, query, failing_query)
    load = getattr(daily_report, loader)
    load.clear()
    try:
        assert load("2024-05-01").empty
        failing_query.assert_called_once()
        assert failing_query.call_args.args == (conn,)
        assert failing_query.call_args.kwargs["report_date"] == dt.date(2024, 5, 1)
        conn.close.assert_called_once_with()
    finally:
        load.clear()
