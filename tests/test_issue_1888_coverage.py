"""Behavior checks for the remaining search, signal, and report boundaries."""

import datetime as dt
import io
import sqlite3
from unittest.mock import MagicMock, Mock

import pandas as pd
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


@pytest.mark.parametrize("topic", ["merger", "unavailable"])
def test_report_page_sorts_value_and_filters_news_exports(monkeypatch, tmp_path, topic):
    from tests.test_daily_report import setup_db

    db_path = setup_db(tmp_path)
    with sqlite3.connect(db_path) as conn:
        # The higher manager count must lose to the larger value when value sorting is selected.
        conn.execute(
            "INSERT INTO crowded_trades VALUES "
            "(21, 'BBB', 'Smaller Position', 4, '[1, 2]', 100, 2, 3, "
            "'2024-05-01', '2024-05-01T10:00:00')"
        )
    monkeypatch.setenv("DB_PATH", db_path)
    caches = [
        daily_report.load_diffs,
        daily_report.load_news,
        daily_report.load_activism_events,
        daily_report.load_crowded_trades,
        daily_report.load_contrarian_signals,
        daily_report.latest_report_date,
        daily_report.nearest_report_date,
    ]
    for cached in caches:
        cached.clear()
    ui = MagicMock()
    ui.date_input.return_value = dt.date(2024, 5, 1)
    ui.tabs.return_value = [MagicMock() for _ in range(3)]
    ui.columns.return_value = [Mock() for _ in range(5)]
    ui.selectbox.side_effect = ["Total value", topic]
    monkeypatch.setattr(daily_report, "st", ui)
    monkeypatch.setattr(daily_report, "require_login", lambda: True)
    try:
        daily_report.main()

        crowded = ui.dataframe.call_args_list[0].args[0]
        assert crowded["CUSIP"].tolist() == ["AAA111111", "BBB"]
        assert crowded["Total Value"].tolist() == [1200000.0, 100.0]
        downloads = {call.args[0]: call.args[1] for call in ui.download_button.call_args_list}
        crowded_csv = pd.read_csv(io.BytesIO(downloads["Download Crowded Trades CSV"]))
        assert crowded_csv["CUSIP"].tolist() == ["AAA111111", "BBB"]
        if topic == "merger":
            assert ui.dataframe.call_count == 2
            assert ui.dataframe.call_args.args[0].data["Headline"].tolist() == ["Headline1"]
            news_csv = pd.read_csv(io.BytesIO(downloads["Download News CSV"]))
            assert news_csv["Headline"].tolist() == ["Headline1"]
            linked = [
                call.args[0] for call in ui.markdown.call_args_list if "[Headline" in call.args[0]
            ]
            assert len(linked) == 1
            assert "[Headline1](<https://example.com/1>)" in linked[0]
        else:
            assert ui.dataframe.call_count == 1
            ui.info.assert_called_once_with("No news matching 'unavailable' on 2024-05-01.")
            assert "Download News CSV" not in downloads
    finally:
        for cached in caches:
            cached.clear()


def test_report_diff_unexpected_error_propagates_and_closes(monkeypatch):
    conn = Mock()
    query = Mock(side_effect=pd.errors.DatabaseError("database is locked"))
    monkeypatch.setattr(daily_report, "connect_db", lambda: conn)
    monkeypatch.setattr(daily_report, "resolve_manager_id_column", lambda conn: "manager_id")
    monkeypatch.setattr(daily_report.pd, "read_sql_query", query)
    daily_report.load_diffs.clear()
    try:
        with pytest.raises(pd.errors.DatabaseError, match="^database is locked$"):
            daily_report.load_diffs("2024-05-01")
        query.assert_called_once()
        conn.close.assert_called_once_with()
    finally:
        daily_report.load_diffs.clear()


def test_report_latest_date_ignores_missing_sources(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE filings (filed_date TEXT);
        INSERT INTO filings VALUES ('2024-04-01'), ('2024-05-01'), (NULL);
    """)
    monkeypatch.setattr(daily_report, "connect_db", lambda: conn)
    daily_report.latest_report_date.clear()
    try:
        assert daily_report.latest_report_date() == dt.date(2024, 5, 1)
        with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
            conn.execute("SELECT 1")
    finally:
        daily_report.latest_report_date.clear()
        conn.close()


@pytest.mark.parametrize(
    "manager_ids, names",
    [('"Alpha"', []), ("true", []), ("7", []), ('[1, "2", "bad", {}, null]', ["Alpha", "Beta"])],
)
def test_crowded_query_preserves_native_dates_and_tolerates_invalid_ids(
    monkeypatch, manager_ids, names
):
    conn = Mock()
    report_date = dt.date(2024, 5, 1)
    conn.execute.return_value.fetchall.return_value = [
        ("AAA", "Issuer", 3, manager_ids, 1200, 25, report_date)
    ]
    monkeypatch.setattr(signals, "table_exists", lambda conn, table: table == "crowded_trades")
    monkeypatch.setattr(signals, "_manager_name_lookup", lambda conn: {1: "Alpha", 2: "Beta"})

    rows = signals.query_crowded_trades(conn, report_date=report_date, limit=5)

    assert [row.model_dump() for row in rows] == [
        {
            "cusip": "AAA",
            "name_of_issuer": "Issuer",
            "manager_count": 3,
            "manager_names": names,
            "total_value_usd": 1200.0,
            "avg_conviction_pct": 25.0,
            "report_date": report_date,
        }
    ]
    assert conn.execute.call_args.args[1] == (report_date, 3, 5)


@pytest.mark.parametrize("failure", ["database_list", "embedding"])
def test_search_sqlite_vector_failure_preserves_text_results(monkeypatch, tmp_path, failure):
    import embeddings

    class Connection(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            if failure == "database_list" and sql == "PRAGMA database_list":
                raise sqlite3.OperationalError("database list unavailable")
            return super().execute(sql, parameters)

    conn = sqlite3.connect(tmp_path / "fallback.db", factory=Connection)
    vector = Mock(side_effect=OSError("vector index unavailable"))
    monkeypatch.setattr(embeddings, "search_documents", vector)
    try:
        conn.executescript("""
            CREATE TABLE documents (doc_id INTEGER PRIMARY KEY, filename TEXT, text TEXT);
            INSERT INTO documents VALUES (1, 'memo', 'needle text');
        """)
        conn.commit()

        results = search.universal_search("needle", conn, entity_type="document")

        assert [r.model_dump() for r in results] == [
            {
                "entity_type": "document",
                "entity_id": 1,
                "manager_name": None,
                "headline": "memo",
                "snippet": "needle text",
                "relevance": pytest.approx(0.7),
                "url": None,
                "timestamp": None,
            }
        ]
        if failure == "database_list":
            vector.assert_not_called()
        else:
            vector.assert_called_once_with("needle", db_path=str(tmp_path / "fallback.db"), k=20)
        assert conn.execute("SELECT COUNT(*) FROM documents").fetchone() == (1,)
    finally:
        conn.close()


def test_search_postgres_ignores_vector_hits_without_document_id(monkeypatch):
    import embeddings
    from tests.test_search import _FakePostgresConn

    conn = _FakePostgresConn()
    monkeypatch.setattr(search, "table_exists", lambda conn, table: table == "documents")
    vector = Mock(return_value=[{"content": "orphan hit"}, {"doc_id": 6, "distance": 1.0}])
    monkeypatch.setattr(embeddings, "search_documents", vector)

    results = search.universal_search("Elliott", conn, entity_type="document")

    assert [(r.entity_id, r.headline, r.snippet, r.relevance) for r in results] == [
        (5, "memo.txt", "Elliott strategy memo", pytest.approx(0.7666666667)),
        (6, "Document", "", 0.5),
    ]
    vector.assert_called_once_with("Elliott", k=20, connection=conn)
