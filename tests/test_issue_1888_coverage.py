"""Behavior checks for the remaining search, signal, and report boundaries."""

import datetime as dt
import io
import sqlite3
from unittest.mock import MagicMock, Mock, call

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


@pytest.mark.parametrize("outcome", ["populated", "empty", "query_error"])
def test_report_postgres_news_caches_results_by_date_and_closes(monkeypatch, outcome):
    columns = [
        "headline",
        "url",
        "published_at",
        "source",
        "topics",
        "confidence",
        "manager_name",
    ]
    expected = pd.DataFrame(columns=columns)
    if outcome == "populated":
        expected = pd.DataFrame(
            [["Market update", None, "2024-05-01T10:00:00", "Wire", None, 0.8, None]],
            columns=columns,
        )
    connections = [Mock(), Mock()]
    connect = Mock(side_effect=connections)
    read_query = Mock(return_value=expected)
    if outcome == "query_error":
        read_query.side_effect = pd.errors.DatabaseError("news temporarily unavailable")
    monkeypatch.setattr(daily_report, "connect_db", connect)
    monkeypatch.setattr(daily_report.pd, "read_sql_query", read_query)
    daily_report.load_news.clear()
    try:
        pd.testing.assert_frame_equal(daily_report.load_news("2024-05-01"), expected)
        pd.testing.assert_frame_equal(daily_report.load_news("2024-05-01"), expected)
        connect.assert_called_once_with()
        read_query.assert_called_once()
        connections[0].close.assert_called_once_with()

        # Empty and failed-query results are cached too; another date must query again.
        pd.testing.assert_frame_equal(daily_report.load_news("2024-05-02"), expected)
        assert connect.call_count == 2
        assert read_query.call_count == 2
        connections[1].close.assert_called_once_with()
        for call, connection, date in zip(
            read_query.call_args_list, connections, ["2024-05-01", "2024-05-02"], strict=True
        ):
            sql, actual_connection = call.args
            assert actual_connection is connection
            assert "LEFT JOIN managers m ON m.manager_id = n.manager_id" in sql
            assert "WHERE n.published_at::date = %s" in sql
            assert sql.endswith("ORDER BY n.published_at DESC LIMIT 50")
            assert call.kwargs == {"params": (date,)}
    finally:
        daily_report.load_news.clear()


@pytest.mark.parametrize("url", [" ", "\t\n"])
def test_report_whitespace_news_url_keeps_escaped_headline(url):
    assert (
        daily_report.headline_markdown(r"  [Desk] \ <x> &  ", url) == r"\[Desk\] \\ &lt;x&gt; &amp;"
    )


@pytest.mark.parametrize("go_to_latest", [False, True])
def test_report_date_navigation_keeps_display_and_export_in_sync(
    monkeypatch, tmp_path, go_to_latest
):
    from tests.test_daily_report import setup_db

    db_path = setup_db(tmp_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO daily_diffs "
            "(manager_id, report_date, cusip, name_of_issuer, delta_type, "
            "shares_prev, shares_curr, value_prev, value_curr) "
            "VALUES ('m1', '2024-04-30', 'OLDER', 'Older Position', 'ADD', 0, 5, 0, 50)"
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
    ui.date_input.return_value = dt.date(2024, 4, 30)
    ui.button.return_value = go_to_latest
    ui.tabs.return_value = [MagicMock() for _ in range(3)]
    ui.columns.return_value = [Mock() for _ in range(5)]
    ui.selectbox.side_effect = ["Manager count", "All topics"]
    monkeypatch.setattr(daily_report, "st", ui)
    monkeypatch.setattr(daily_report, "require_login", lambda: True)
    try:
        daily_report.main()

        ui.date_input.assert_called_once_with("Date", dt.date(2024, 5, 1))
        ui.button.assert_called_once_with("Go to latest report date (2024-05-01)")
        expected_date = "2024-05-01" if go_to_latest else "2024-04-30"
        expected_cusips = ["AAA", "BBB"] if go_to_latest else ["OLDER"]
        diff_download = next(
            call for call in ui.download_button.call_args_list if call.args[0] == "Download CSV"
        )
        exported = pd.read_csv(io.BytesIO(diff_download.args[1]))
        assert exported["cusip"].tolist() == expected_cusips
        assert diff_download.kwargs == {
            "file_name": f"diff_{expected_date}.csv",
            "mime": "text/csv",
        }
        displayed = pd.read_html(io.StringIO(ui.markdown.call_args_list[0].args[0]))[0]
        assert displayed["cusip"].tolist() == expected_cusips
        if go_to_latest:
            news_download = next(
                call
                for call in ui.download_button.call_args_list
                if call.args[0] == "Download News CSV"
            )
            news = pd.read_csv(io.BytesIO(news_download.args[1]))
            assert news["Headline"].tolist() == ["Headline1", "Headline2"]
            assert news_download.kwargs["file_name"] == "news_2024-05-01.csv"
        else:
            assert [call.args[0] for call in ui.download_button.call_args_list] == ["Download CSV"]
            assert [call.args[0] for call in ui.info.call_args_list] == [
                "No data for this date — nearest report is 2024-04-30.",
                "No news for this date.",
            ]
    finally:
        for cached in caches:
            cached.clear()


def test_report_login_stop_prevents_database_access(monkeypatch):
    class LoginRequired(Exception):
        pass

    ui = MagicMock()
    ui.stop.side_effect = LoginRequired
    connect = Mock(side_effect=AssertionError("database accessed before login"))
    monkeypatch.setattr(daily_report, "st", ui)
    monkeypatch.setattr(daily_report, "require_login", lambda: False)
    monkeypatch.setattr(daily_report, "connect_db", connect)
    daily_report.latest_report_date.clear()
    try:
        with pytest.raises(LoginRequired):
            daily_report.main()
        ui.stop.assert_called_once_with()
        ui.date_input.assert_not_called()
        ui.tabs.assert_not_called()
        connect.assert_not_called()
    finally:
        daily_report.latest_report_date.clear()


@pytest.mark.parametrize(
    "dates, expected",
    [
        ([], None),
        ([None, "not-a-date"], None),
        (["2024-04-29", "2024-05-01"], dt.date(2024, 5, 1)),
        (["2024-04-29", "2024-04-30", "2024-05-01"], dt.date(2024, 4, 30)),
    ],
    ids=["empty", "invalid", "tie-prefers-newer", "exact-match"],
)
def test_report_nearest_date_caches_empty_and_populated_results(
    monkeypatch, tmp_path, dates, expected
):
    db_path = tmp_path / "report-dates.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE daily_diffs (report_date TEXT)")
        conn.executemany("INSERT INTO daily_diffs VALUES (?)", [(value,) for value in dates])
    conn.close()
    connection = sqlite3.connect(db_path)
    connect = Mock(return_value=connection)
    monkeypatch.setattr(daily_report, "connect_db", connect)
    daily_report.nearest_report_date.clear()
    try:
        assert daily_report.nearest_report_date("2024-04-30") == expected
        message = (
            "No data for this date."
            if expected is None
            else f"No data for this date — nearest report is {expected.isoformat()}."
        )
        # Repeated rendering must reuse even a cached None, after the connection closes.
        assert daily_report.empty_report_date_message("2024-04-30") == message
        assert daily_report.empty_report_date_message("2024-04-30") == message
        connect.assert_called_once_with()
        with pytest.raises(sqlite3.ProgrammingError, match="Cannot operate on a closed database"):
            connection.execute("SELECT 1")
    finally:
        connection.close()
        daily_report.nearest_report_date.clear()


def test_report_latest_date_query_errors_close_connections_and_are_retried(monkeypatch):
    connections = [Mock(), Mock()]
    connect = Mock(side_effect=connections)
    query = Mock(side_effect=pd.errors.DatabaseError("database is locked"))
    monkeypatch.setattr(daily_report, "connect_db", connect)
    monkeypatch.setattr(daily_report.pd, "read_sql_query", query)
    daily_report.latest_report_date.clear()
    try:
        for connection in connections:
            with pytest.raises(pd.errors.DatabaseError) as exc_info:
                daily_report.latest_report_date()
            assert str(exc_info.value) == "database is locked"
            connection.close.assert_called_once_with()
        assert connect.call_count == 2
        assert query.call_args_list == [
            call("SELECT MAX(report_date) AS report_date FROM daily_diffs", connection)
            for connection in connections
        ]
    finally:
        daily_report.latest_report_date.clear()
