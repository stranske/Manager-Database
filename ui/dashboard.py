"""Holdings delta dashboard with sparkline."""

from __future__ import annotations

import sqlite3

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


def render_manager_selector() -> int | str | None:
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
    return None if selected == "all" else selected


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


def main() -> None:
    if not require_login():
        st.stop()
    st.header("Holdings Delta")
    selected_manager_id = render_manager_selector()
    render_filing_timeline(selected_manager_id)
    render_latest_holdings_snapshot(selected_manager_id)
    df = load_delta()
    if df.empty:
        st.info("No data available")
        return
    chart = alt.Chart(df).mark_line().encode(x="date:T", y="filings:Q")
    st.altair_chart(chart, use_container_width=True)
    st.dataframe(df)


if __name__ == "__main__":
    main()
