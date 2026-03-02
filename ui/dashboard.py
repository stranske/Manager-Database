"""Holdings delta dashboard with sparkline."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import sqlite3
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st

from adapters.base import connect_db

from . import require_login


def load_delta() -> pd.DataFrame:
    conn = connect_db()
    df = pd.read_sql_query(
        "SELECT filed as date, COUNT(*) AS filings FROM holdings GROUP BY filed ORDER BY filed",
        conn,
    )
    conn.close()
    return df


def load_filing_timeline(manager_id: int) -> pd.DataFrame:
    conn = connect_db()
    placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"
    query = (
        "SELECT filing_id, type, filed_date, period_end, source, raw_key "
        f"FROM filings WHERE manager_id = {placeholder} "
        "ORDER BY filed_date DESC LIMIT 20"
    )
    try:
        df = pd.read_sql_query(query, conn, params=(manager_id,))
    except Exception:
        df = pd.DataFrame(
            columns=["filing_id", "type", "filed_date", "period_end", "source", "raw_key"]
        )
    conn.close()
    return df


def load_latest_holdings_snapshot(manager_id: int) -> pd.DataFrame:
    conn = connect_db()
    placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"
    query = (
        "SELECT h.name_of_issuer, h.cusip, h.shares, h.value_usd "
        "FROM holdings h "
        "JOIN filings f ON f.filing_id = h.filing_id "
        f"WHERE f.manager_id = {placeholder} "
        "ORDER BY f.filed_date DESC, h.value_usd DESC "
        "LIMIT 50"
    )
    try:
        df = pd.read_sql_query(query, conn, params=(manager_id,))
    except Exception:
        df = pd.DataFrame(columns=["name_of_issuer", "cusip", "shares", "value_usd"])
    conn.close()
    return df


def load_top_deltas(manager_id: int) -> pd.DataFrame:
    conn = connect_db()
    placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"
    query = (
        "SELECT cusip, name_of_issuer, delta_type, shares_prev, shares_curr, value_prev, value_curr "
        "FROM daily_diffs "
        f"WHERE manager_id = {placeholder} "
        f"AND report_date = (SELECT MAX(report_date) FROM daily_diffs WHERE manager_id = {placeholder}) "
        "ORDER BY ABS(COALESCE(value_curr,0) - COALESCE(value_prev,0)) DESC "
        "LIMIT 10"
    )
    try:
        df = pd.read_sql_query(query, conn, params=(manager_id, manager_id))
    except Exception:
        df = pd.DataFrame(
            columns=[
                "cusip",
                "name_of_issuer",
                "delta_type",
                "shares_prev",
                "shares_curr",
                "value_prev",
                "value_curr",
            ]
        )
    conn.close()
    return df


def load_news_stream(manager_id: int) -> pd.DataFrame:
    conn = connect_db()
    placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"
    query = (
        "SELECT headline, url, published_at, source, topics, confidence "
        "FROM news_items "
        f"WHERE manager_id = {placeholder} "
        "ORDER BY published_at DESC "
        "LIMIT 15"
    )
    try:
        df = pd.read_sql_query(query, conn, params=(manager_id,))
    except Exception:
        df = pd.DataFrame(
            columns=["headline", "url", "published_at", "source", "topics", "confidence"]
        )
    conn.close()
    return df


def load_qc_flags(manager_id: int) -> dict[str, Any]:
    conn = connect_db()
    placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"
    now_utc = datetime.now(UTC)

    summary: dict[str, Any] = {
        "last_filing_date": None,
        "is_13f_filer": False,
        "latest_holdings_count": 0,
        "news_count_30d": 0,
        "last_etl_run": None,
    }
    try:
        latest_filing_query = (
            "SELECT filed_date, type FROM filings "
            f"WHERE manager_id = {placeholder} "
            "ORDER BY filed_date DESC LIMIT 1"
        )
        latest_filing = pd.read_sql_query(latest_filing_query, conn, params=(manager_id,))
        if not latest_filing.empty:
            filing_date = pd.to_datetime(latest_filing.loc[0, "filed_date"], errors="coerce")
            summary["last_filing_date"] = filing_date

        is_13f_query = (
            "SELECT COUNT(*) AS cnt FROM filings "
            f"WHERE manager_id = {placeholder} AND UPPER(type) LIKE '13F%'"
        )
        is_13f_df = pd.read_sql_query(is_13f_query, conn, params=(manager_id,))
        summary["is_13f_filer"] = int(is_13f_df.loc[0, "cnt"]) > 0

        holdings_count_query = (
            "SELECT COUNT(*) AS holdings_count "
            "FROM holdings h "
            "JOIN filings f ON f.filing_id = h.filing_id "
            f"WHERE f.manager_id = {placeholder} "
            f"AND f.filing_id = (SELECT filing_id FROM filings WHERE manager_id = {placeholder} "
            "ORDER BY filed_date DESC LIMIT 1)"
        )
        holdings_count_df = pd.read_sql_query(
            holdings_count_query,
            conn,
            params=(manager_id, manager_id),
        )
        summary["latest_holdings_count"] = int(holdings_count_df.loc[0, "holdings_count"])

        threshold_30d = now_utc - timedelta(days=30)
        news_count_query = (
            "SELECT COUNT(*) AS news_count_30d "
            "FROM news_items "
            f"WHERE manager_id = {placeholder} AND published_at >= {placeholder}"
        )
        news_count_df = pd.read_sql_query(
            news_count_query,
            conn,
            params=(manager_id, threshold_30d.isoformat(sep=" ")),
        )
        summary["news_count_30d"] = int(news_count_df.loc[0, "news_count_30d"])

        last_etl_df = pd.read_sql_query("SELECT MAX(ts) AS last_etl_run FROM api_usage", conn)
        if not last_etl_df.empty:
            summary["last_etl_run"] = pd.to_datetime(
                last_etl_df.loc[0, "last_etl_run"], errors="coerce"
            )
    except Exception:
        pass
    finally:
        conn.close()
    return summary


@st.cache_data(show_spinner=False)
def load_managers() -> pd.DataFrame:
    conn = connect_db()
    try:
        df = pd.read_sql_query(
            "SELECT manager_id, name FROM managers ORDER BY name",
            conn,
        )
    except Exception:
        df = pd.DataFrame(columns=["manager_id", "name"])
    conn.close()
    return df


def render_manager_selector() -> int | None:
    managers = load_managers()
    options: list[int | str] = ["all"]
    labels: dict[int | str, str] = {"all": "All Managers"}
    for row in managers.itertuples(index=False):
        options.append(row.manager_id)
        labels[row.manager_id] = row.name

    selected_key = "selected_manager_id"
    if selected_key not in st.session_state or st.session_state[selected_key] not in options:
        st.session_state[selected_key] = options[0]

    selected = st.selectbox(
        "Manager",
        options=options,
        index=options.index(st.session_state[selected_key]),
        format_func=lambda manager_id: labels.get(manager_id, str(manager_id)),
        key=selected_key,
    )
    if selected == "all":
        return None
    if isinstance(selected, int):
        return selected
    return None


def render_filing_timeline(selected_manager_id: int | None) -> None:
    st.subheader("Filing Timeline")
    if selected_manager_id is None:
        st.info("Select a manager to view filing timeline details.")
        return

    filings = load_filing_timeline(selected_manager_id)
    if filings.empty:
        st.info("No filings found for the selected manager.")
        return

    filings_for_chart = filings.copy()
    filings_for_chart["filed_date"] = pd.to_datetime(
        filings_for_chart["filed_date"], errors="coerce"
    )
    timeline_chart = (
        alt.Chart(filings_for_chart.dropna(subset=["filed_date"]))
        .mark_circle(size=90)
        .encode(
            x=alt.X("filed_date:T", title="Filed Date"),
            y=alt.Y("type:N", title="Filing Type"),
            color=alt.Color("type:N", title="Type"),
            tooltip=["filing_id", "type", "filed_date", "period_end", "source", "raw_key"],
        )
    )
    st.altair_chart(timeline_chart, use_container_width=True)
    st.dataframe(filings, use_container_width=True)


def render_latest_holdings_snapshot(selected_manager_id: int | None) -> None:
    st.subheader("Latest Holdings Snapshot")
    if selected_manager_id is None:
        st.info("Select a manager to view the latest holdings snapshot.")
        return

    holdings = load_latest_holdings_snapshot(selected_manager_id)
    if holdings.empty:
        st.info("No holdings found for the selected manager.")
        return

    value_series = pd.to_numeric(holdings["value_usd"], errors="coerce").fillna(0.0)
    col_positions, col_aum = st.columns(2)
    col_positions.metric("Total Positions", f"{len(holdings):,}")
    col_aum.metric("Total AUM (USD)", f"${value_series.sum():,.0f}")
    st.dataframe(holdings, use_container_width=True)


def _delta_type_color(delta_type: str) -> str:
    if delta_type in {"ADD", "INCREASE"}:
        return "color: #198754; font-weight: 700"
    if delta_type in {"EXIT", "DECREASE"}:
        return "color: #C1121F; font-weight: 700"
    return "color: #6C757D"


def render_top_deltas(selected_manager_id: int | None) -> None:
    st.subheader("Top Deltas")
    if selected_manager_id is None:
        st.info("Select a manager to view top position changes.")
        return

    deltas = load_top_deltas(selected_manager_id)
    if deltas.empty:
        st.info("No daily deltas found for the selected manager.")
        return

    deltas = deltas.copy()
    deltas["value_prev"] = pd.to_numeric(deltas["value_prev"], errors="coerce").fillna(0.0)
    deltas["value_curr"] = pd.to_numeric(deltas["value_curr"], errors="coerce").fillna(0.0)
    deltas["delta_value"] = deltas["value_curr"] - deltas["value_prev"]
    deltas["abs_delta_value"] = deltas["delta_value"].abs()

    color_scale = alt.Scale(
        domain=["ADD", "INCREASE", "DECREASE", "EXIT"],
        range=["#198754", "#198754", "#C1121F", "#C1121F"],
    )
    delta_chart = (
        alt.Chart(deltas)
        .mark_bar()
        .encode(
            x=alt.X("abs_delta_value:Q", title="Absolute Value Change (USD)"),
            y=alt.Y("name_of_issuer:N", sort="-x", title="Issuer"),
            color=alt.Color("delta_type:N", scale=color_scale, title="Delta Type"),
            tooltip=[
                "name_of_issuer",
                "cusip",
                "delta_type",
                alt.Tooltip("delta_value:Q", title="Value Change"),
                "value_prev",
                "value_curr",
            ],
        )
    )
    st.altair_chart(delta_chart, use_container_width=True)

    table_cols = [
        "name_of_issuer",
        "cusip",
        "delta_type",
        "shares_prev",
        "shares_curr",
        "value_prev",
        "value_curr",
        "delta_value",
    ]
    styled = deltas[table_cols].style.map(_delta_type_color, subset=["delta_type"])
    st.dataframe(styled, use_container_width=True)


def _topic_badges(topics_value: Any) -> str:
    if topics_value is None or pd.isna(topics_value):
        return ""
    topics = [topic.strip() for topic in str(topics_value).split(",") if topic.strip()]
    return " ".join(f"`{topic}`" for topic in topics[:5])


def render_news_stream(selected_manager_id: int | None) -> None:
    st.subheader("News Stream")
    if selected_manager_id is None:
        st.info("Select a manager to view recent news.")
        return

    news_items = load_news_stream(selected_manager_id)
    if news_items.empty:
        st.info("No recent news found for the selected manager.")
        return

    news_items = news_items.copy()
    news_items["published_at"] = pd.to_datetime(news_items["published_at"], errors="coerce")
    for item in news_items.itertuples(index=False):
        headline = str(item.headline) if item.headline else "Untitled"
        url = str(item.url).strip() if item.url else ""
        if url:
            st.markdown(f"- [{headline}]({url})")
        else:
            st.markdown(f"- {headline}")

        meta_parts = []
        if pd.notna(item.published_at):
            meta_parts.append(item.published_at.strftime("%Y-%m-%d %H:%M"))
        if item.source:
            meta_parts.append(str(item.source))
        if pd.notna(item.confidence):
            meta_parts.append(f"confidence {float(item.confidence):.2f}")
        badges = _topic_badges(item.topics)
        meta_line = " | ".join(meta_parts)
        if badges:
            meta_line = f"{meta_line} {badges}" if meta_line else badges
        if meta_line:
            st.caption(meta_line)


def render_qc_flags(selected_manager_id: int | None) -> None:
    st.subheader("QC Flags")
    if selected_manager_id is None:
        st.info("Select a manager to view data quality flags.")
        return

    qc = load_qc_flags(selected_manager_id)
    now_utc = datetime.now(UTC)
    col_filing, col_holdings, col_news, col_freshness = st.columns(4)

    filing_date = qc.get("last_filing_date")
    is_13f_filer = bool(qc.get("is_13f_filer"))
    if filing_date is not None and pd.notna(filing_date):
        filing_dt = pd.to_datetime(filing_date, errors="coerce")
        filing_days = (now_utc - filing_dt.to_pydatetime().replace(tzinfo=UTC)).days
        if is_13f_filer and filing_days > 120:
            filing_delta = f"+{filing_days - 120}d past 120d SLA"
            filing_delta_color = "inverse"
        else:
            filing_delta = f"-{max(120 - filing_days, 0)}d to 120d SLA"
            filing_delta_color = "inverse"
        filing_value = filing_date.strftime("%Y-%m-%d")
    else:
        filing_value = "N/A"
        filing_delta = "+1 missing filing date"
        filing_delta_color = "inverse"
    col_filing.metric(
        "Last Filing Date", filing_value, delta=filing_delta, delta_color=filing_delta_color
    )

    holdings_count = int(qc.get("latest_holdings_count", 0) or 0)
    holdings_delta = "-0 empty filing warnings"
    if holdings_count == 0:
        holdings_delta = "+1 empty filing warning"
    col_holdings.metric(
        "Holdings in Latest Filing",
        f"{holdings_count:,}",
        delta=holdings_delta,
        delta_color="inverse",
    )

    news_count = int(qc.get("news_count_30d", 0) or 0)
    news_delta = "-0 low-coverage warnings"
    if news_count == 0:
        news_delta = "+1 no news in last 30d"
    col_news.metric(
        "News Items (30d)",
        f"{news_count:,}",
        delta=news_delta,
        delta_color="inverse",
    )

    last_etl_run = qc.get("last_etl_run")
    freshness_value = "N/A"
    freshness_delta = "+1 missing ETL telemetry"
    if last_etl_run is not None and pd.notna(last_etl_run):
        etl_dt = pd.to_datetime(last_etl_run, errors="coerce")
        etl_ts = etl_dt.to_pydatetime().replace(tzinfo=UTC)
        hours_old = max((now_utc - etl_ts).total_seconds() / 3600, 0)
        freshness_value = etl_ts.strftime("%Y-%m-%d %H:%M UTC")
        if hours_old > 24:
            freshness_delta = f"+{int(hours_old - 24)}h past 24h target"
        else:
            freshness_delta = f"-{int(24 - hours_old)}h to stale"
    col_freshness.metric(
        "Data Freshness (ETL)",
        freshness_value,
        delta=freshness_delta,
        delta_color="inverse",
    )


def main() -> None:
    if not require_login():
        st.stop()
    st.header("Holdings Delta")
    selected_manager_id = render_manager_selector()
    render_filing_timeline(selected_manager_id)
    render_latest_holdings_snapshot(selected_manager_id)
    render_top_deltas(selected_manager_id)
    render_news_stream(selected_manager_id)
    render_qc_flags(selected_manager_id)
    df = load_delta()
    if df.empty:
        st.info("No data available")
        return
    chart = alt.Chart(df).mark_line().encode(x="date:T", y="filings:Q")
    st.altair_chart(chart, use_container_width=True)
    st.dataframe(df)


if __name__ == "__main__":
    main()
