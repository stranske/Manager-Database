"""Holdings delta dashboard with sparkline."""

from __future__ import annotations

import os
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

import altair as alt
import httpx
import pandas as pd
import streamlit as st

from adapters.base import connect_db
from api.activism import (
    query_active_campaigns,
    query_activism_events,
    query_activism_filings,
    query_activism_timeline,
)
from api.signals import query_contrarian_signals, query_conviction_scores, query_crowded_trades

from . import require_login


def _api_base_url() -> str:
    return (
        os.getenv("ALERTS_API_BASE_URL") or os.getenv("API_BASE_URL") or "http://localhost:8000"
    ).rstrip("/")


@st.cache_data(show_spinner=False, ttl=60)
def load_unacknowledged_alert_count() -> int:
    """Load unacknowledged alert count for the sidebar badge."""
    try:
        with httpx.Client(timeout=5.0) as client:
            response = client.get(f"{_api_base_url()}/api/alerts/unacknowledged/count")
        response.raise_for_status()
        payload = response.json()
        return int(payload.get("count", 0))
    except Exception:
        return 0


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


def load_news_stream(manager_id: int | None, limit: int = 10) -> pd.DataFrame:
    conn = connect_db()
    try:
        if isinstance(conn, sqlite3.Connection):
            params: list[Any] = [limit]
            where_clause = ""
            if manager_id is not None:
                where_clause = "WHERE n.manager_id = ? "
                params.insert(0, manager_id)
            query = (
                "SELECT n.headline, n.url, n.published_at, n.source, n.topics, n.confidence, "
                "m.name AS manager_name "
                "FROM news_items n "
                "LEFT JOIN managers m ON m.manager_id = n.manager_id "
                f"{where_clause}"
                "ORDER BY n.published_at DESC "
                "LIMIT ?"
            )
            df = pd.read_sql_query(query, conn, params=tuple(params))
        else:
            params_pg: list[Any] = []
            where_clause = ""
            if manager_id is not None:
                where_clause = "WHERE n.manager_id = %s "
                params_pg.append(manager_id)
            params_pg.append(limit)
            query = (
                "SELECT n.headline, n.url, n.published_at, n.source, n.topics, n.confidence, "
                "m.name AS manager_name "
                "FROM news_items n "
                "LEFT JOIN managers m ON m.manager_id = n.manager_id "
                f"{where_clause}"
                "ORDER BY n.published_at DESC "
                "LIMIT %s"
            )
            df = pd.read_sql_query(query, conn, params=tuple(params_pg))
    except Exception:
        df = pd.DataFrame(
            columns=[
                "headline",
                "url",
                "published_at",
                "source",
                "topics",
                "confidence",
                "manager_name",
            ]
        )
    conn.close()
    return df


def load_manager_activism_filings(manager_id: int, limit: int = 200) -> pd.DataFrame:
    conn = connect_db()
    try:
        rows = query_activism_filings(conn, manager_id=manager_id, limit=limit)
    except Exception:
        rows = []
    finally:
        conn.close()
    return pd.DataFrame([row.model_dump() for row in rows])


def load_manager_activism_events(manager_id: int, limit: int = 200) -> pd.DataFrame:
    conn = connect_db()
    try:
        rows = query_activism_events(conn, manager_id=manager_id, limit=limit)
    except Exception:
        rows = []
    finally:
        conn.close()
    return pd.DataFrame([row.model_dump() for row in rows])


def load_manager_activism_timeline(manager_id: int) -> pd.DataFrame:
    conn = connect_db()
    try:
        rows = query_activism_timeline(conn, manager_id)
    except Exception:
        rows = []
    finally:
        conn.close()
    return pd.DataFrame([row.model_dump() for row in rows])


def load_active_campaigns_summary(min_ownership_pct: float = 5.0, limit: int = 10) -> pd.DataFrame:
    conn = connect_db()
    try:
        rows = query_active_campaigns(
            conn,
            min_ownership_pct=min_ownership_pct,
            limit=limit,
        )
    except Exception:
        rows = []
    finally:
        conn.close()
    return pd.DataFrame([row.model_dump() for row in rows])


def load_manager_conviction_scores(
    manager_id: int,
    *,
    filing_id: int | None = None,
    min_conviction_pct: float = 0.0,
    limit: int = 100,
) -> pd.DataFrame:
    conn = connect_db()
    try:
        rows = query_conviction_scores(
            conn,
            manager_id,
            filing_id=filing_id,
            min_conviction_pct=min_conviction_pct,
            limit=limit,
        )
    except Exception:
        rows = []
    finally:
        conn.close()
    return pd.DataFrame([row.model_dump() for row in rows])


def load_manager_crowded_trades(
    manager_id: int,
    *,
    min_managers: int = 3,
    limit: int = 25,
) -> pd.DataFrame:
    conn = connect_db()
    try:
        rows = query_crowded_trades(
            conn,
            manager_id=manager_id,
            min_managers=min_managers,
            limit=limit,
        )
    except Exception:
        rows = []
    finally:
        conn.close()
    return pd.DataFrame([row.model_dump() for row in rows])


def load_manager_contrarian_signals(manager_id: int, limit: int = 25) -> pd.DataFrame:
    conn = connect_db()
    try:
        rows = query_contrarian_signals(conn, manager_id=manager_id, limit=limit)
    except Exception:
        rows = []
    finally:
        conn.close()
    return pd.DataFrame([row.model_dump() for row in rows])


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


def render_filing_timeline(selected_manager_id: int | None, show_heading: bool = True) -> None:
    if show_heading:
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


def render_latest_holdings_snapshot(
    selected_manager_id: int | None, show_heading: bool = True
) -> None:
    if show_heading:
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


def render_top_deltas(selected_manager_id: int | None, show_heading: bool = True) -> None:
    if show_heading:
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


def render_news_stream(selected_manager_id: int | None, show_heading: bool = True) -> None:
    if show_heading:
        st.subheader("News")

    news_items = load_news_stream(selected_manager_id, limit=10)
    if news_items.empty:
        if selected_manager_id is None:
            st.info("No recent news found.")
        else:
            st.info("No recent news found for the selected manager.")
        return

    news_items = news_items.copy()
    news_items["published_at"] = pd.to_datetime(news_items["published_at"], errors="coerce")
    for item in news_items.itertuples(index=False):
        headline = str(item.headline) if item.headline else "Untitled"
        url = str(item.url).strip() if item.url else ""
        headline_md = f"[{headline}]({url})" if url else headline
        meta_parts: list[str] = []
        if pd.notna(item.published_at):
            meta_parts.append(item.published_at.strftime("%Y-%m-%d %H:%M"))
        if selected_manager_id is None and getattr(item, "manager_name", None):
            meta_parts.append(str(item.manager_name))
        badges = _topic_badges(item.topics)
        prefix = " | ".join(meta_parts)
        compact_line = f"- {prefix} | {headline_md}" if prefix else f"- {headline_md}"
        if badges:
            compact_line = f"{compact_line} {badges}"
        st.markdown(compact_line)


def _event_color(event_type: str) -> str:
    if event_type == "threshold_crossing":
        return "#C1121F"
    if event_type in {"form_upgrade", "group_formation"}:
        return "#F97316"
    if event_type == "initial_stake":
        return "#2563EB"
    return "#475569"


def _format_event_count(events: int) -> str:
    return "1 event" if events == 1 else f"{events} events"


def render_active_campaigns_widget(show_heading: bool = True) -> None:
    if show_heading:
        st.subheader("Active Campaigns")

    campaigns = load_active_campaigns_summary(limit=10)
    if campaigns.empty:
        st.caption("No active activism campaigns.")
        return

    total_campaigns = len(campaigns)
    total_managers = campaigns["manager_name"].fillna("Unknown").nunique()
    st.metric(
        "Active Campaigns",
        f"{total_campaigns}",
        delta=f"{total_managers} managers with positions >= 5.0%",
    )
    st.markdown("Top 3 most recent campaigns")
    for row in campaigns.head(3).itertuples(index=False):
        manager_name = row.manager_name or "Unknown manager"
        ownership_pct = row.current_ownership_pct
        pct_text = f"{ownership_pct:.1f}%" if ownership_pct is not None else "n/a"
        latest_event = row.latest_event_type or "filing_only"
        st.markdown(
            f"- **{manager_name}** -> {row.subject_company} ({pct_text}) | "
            f"{row.latest_filing_date} | {latest_event}"
        )


def render_activism_timeline(selected_manager_id: int, show_heading: bool = True) -> None:
    if show_heading:
        st.subheader("Activism Timeline")

    timeline = load_manager_activism_timeline(selected_manager_id)
    if timeline.empty:
        st.info("No activism filings or events found for the selected manager.")
        return

    timeline = timeline.copy()
    timeline["date"] = pd.to_datetime(timeline["date"], errors="coerce")
    timeline["primary_event_type"] = timeline["event_types"].apply(
        lambda items: items[0] if isinstance(items, list) and items else "filing"
    )
    timeline["color"] = timeline["primary_event_type"].apply(_event_color)
    chart = (
        alt.Chart(timeline.dropna(subset=["date"]))
        .mark_circle(size=110)
        .encode(
            x=alt.X("type:N", title=None),
            y=alt.Y("date:T", title="Date"),
            color=alt.Color(
                "primary_event_type:N",
                scale=alt.Scale(
                    domain=[
                        "filing",
                        "initial_stake",
                        "threshold_crossing",
                        "form_upgrade",
                        "group_formation",
                        "stake_increase",
                        "stake_decrease",
                        "amendment",
                        "form_downgrade",
                    ],
                    range=[
                        "#1D4ED8",
                        "#2563EB",
                        "#C1121F",
                        "#F97316",
                        "#F97316",
                        "#16A34A",
                        "#C1121F",
                        "#64748B",
                        "#A16207",
                    ],
                ),
                title="Timeline Type",
            ),
            tooltip=["date:T", "type:N", "description:N", "ownership_pct:Q"],
        )
        .properties(height=320)
    )
    st.altair_chart(chart, use_container_width=True)

    timeline_display = timeline[["date", "type", "description", "ownership_pct"]].copy()
    timeline_display["date"] = timeline_display["date"].dt.strftime("%Y-%m-%d")
    st.dataframe(timeline_display, use_container_width=True, hide_index=True)


def render_current_activism_stakes(selected_manager_id: int, show_heading: bool = True) -> None:
    if show_heading:
        st.subheader("Current Stakes")

    filings = load_manager_activism_filings(selected_manager_id)
    events = load_manager_activism_events(selected_manager_id)
    if filings.empty:
        st.info("No activism stakes found for the selected manager.")
        return

    filings = filings.copy()
    filings["filed_date"] = pd.to_datetime(filings["filed_date"], errors="coerce")
    filings = filings.sort_values(["filed_date", "filing_id"], ascending=[False, False])
    latest_positions = (
        filings.groupby(["subject_company", "subject_cusip"], as_index=False, dropna=False)
        .first()
        .rename(columns={"ownership_pct": "Ownership %", "filed_date": "Last Filed"})
    )
    if events.empty:
        latest_positions["Events"] = 0
    else:
        event_counts = (
            events.groupby(["subject_company"], dropna=False).size().reset_index(name="Events")
        )
        latest_positions = latest_positions.merge(
            event_counts,
            on="subject_company",
            how="left",
        )
        latest_positions["Events"] = latest_positions["Events"].fillna(0).astype(int)

    latest_positions["Ownership %"] = pd.to_numeric(
        latest_positions["Ownership %"], errors="coerce"
    )
    latest_positions["Last Filed"] = pd.to_datetime(
        latest_positions["Last Filed"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    latest_positions["Events"] = latest_positions["Events"].map(_format_event_count)
    latest_positions = latest_positions.rename(
        columns={
            "subject_company": "Subject Company",
            "subject_cusip": "CUSIP",
            "filing_type": "Filing Type",
        }
    )
    st.dataframe(
        latest_positions[
            ["Subject Company", "CUSIP", "Ownership %", "Filing Type", "Last Filed", "Events"]
        ],
        use_container_width=True,
        hide_index=True,
    )


def render_ownership_chart(selected_manager_id: int, show_heading: bool = True) -> None:
    if show_heading:
        st.subheader("Ownership Chart")

    filings = load_manager_activism_filings(selected_manager_id)
    if filings.empty:
        st.info("No activism ownership history found for the selected manager.")
        return

    filings = filings.copy()
    filings["filed_date"] = pd.to_datetime(filings["filed_date"], errors="coerce")
    filings["ownership_pct"] = pd.to_numeric(filings["ownership_pct"], errors="coerce")
    filings = filings.dropna(subset=["filed_date"])
    if filings.empty:
        st.info("No plottable activism ownership history found.")
        return

    subject_options = sorted(
        {
            str(company)
            for company in filings["subject_company"].dropna().tolist()
            if str(company).strip()
        }
    )
    if not subject_options:
        st.info("No subject companies available for activism charting.")
        return

    selected_subject = st.selectbox(
        "Ownership history subject",
        subject_options,
        key=f"activism_subject_{selected_manager_id}",
    )
    subject_df = filings.loc[filings["subject_company"] == selected_subject].sort_values(
        "filed_date"
    )
    if subject_df.empty:
        st.info("No activism history for the selected subject company.")
        return

    base_chart = alt.Chart(subject_df).encode(
        x=alt.X("filed_date:T", title="Filed Date"),
        y=alt.Y("ownership_pct:Q", title="Ownership %"),
        tooltip=["subject_company", "filing_type", "filed_date:T", "ownership_pct:Q"],
    )
    line_chart = base_chart.mark_line(point=True, color="#1D4ED8")
    threshold_df = pd.DataFrame({"threshold": [5.0, 10.0, 20.0]})
    threshold_rules = (
        alt.Chart(threshold_df)
        .mark_rule(strokeDash=[6, 4], color="#94A3B8")
        .encode(y="threshold:Q")
    )
    st.altair_chart(line_chart + threshold_rules, use_container_width=True)


def render_top_convictions(selected_manager_id: int, show_heading: bool = True) -> None:
    if show_heading:
        st.subheader("Top Convictions")

    convictions = load_manager_conviction_scores(selected_manager_id, limit=10)
    if convictions.empty:
        st.info("No conviction scores found for the selected manager.")
        return

    convictions = convictions.copy()
    convictions["conviction_pct"] = pd.to_numeric(convictions["conviction_pct"], errors="coerce")
    convictions["value_usd"] = pd.to_numeric(convictions["value_usd"], errors="coerce")
    convictions = convictions.dropna(subset=["conviction_pct"])
    if convictions.empty:
        st.info("No conviction scores available for charting.")
        return

    chart = (
        alt.Chart(convictions.sort_values("conviction_pct", ascending=False).head(10))
        .mark_bar(color="#1D4ED8")
        .encode(
            x=alt.X("conviction_pct:Q", title="Conviction %"),
            y=alt.Y("name_of_issuer:N", sort="-x", title="Issuer"),
            tooltip=["cusip", "name_of_issuer", "conviction_pct", "value_usd"],
        )
    )
    st.altair_chart(chart, use_container_width=True)
    st.dataframe(
        convictions[["name_of_issuer", "cusip", "conviction_pct", "portfolio_weight", "value_usd"]],
        use_container_width=True,
        hide_index=True,
    )


def render_manager_crowded_trades(selected_manager_id: int, show_heading: bool = True) -> None:
    if show_heading:
        st.subheader("Crowded Trades")

    crowded = load_manager_crowded_trades(selected_manager_id)
    if crowded.empty:
        st.info("No crowded trades overlap found for the selected manager.")
        return

    crowded = crowded.copy()
    crowded["manager_names"] = crowded["manager_names"].apply(
        lambda names: ", ".join(names) if isinstance(names, list) else ""
    )
    crowded["avg_conviction_pct"] = pd.to_numeric(
        crowded["avg_conviction_pct"], errors="coerce"
    ).round(2)
    crowded["total_value_usd"] = pd.to_numeric(crowded["total_value_usd"], errors="coerce")
    st.dataframe(
        crowded[
            [
                "cusip",
                "name_of_issuer",
                "manager_count",
                "avg_conviction_pct",
                "total_value_usd",
                "manager_names",
            ]
        ].rename(
            columns={
                "name_of_issuer": "Issuer",
                "manager_count": "# Managers",
                "avg_conviction_pct": "Avg Conviction %",
                "total_value_usd": "Total Value USD",
                "manager_names": "Managers",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )


def render_contrarian_alerts(selected_manager_id: int, show_heading: bool = True) -> None:
    if show_heading:
        st.subheader("Contrarian Alerts")

    signals = load_manager_contrarian_signals(selected_manager_id)
    if signals.empty:
        st.info("No contrarian signals found for the selected manager.")
        return

    signals = signals.copy()
    signals["delta_value"] = pd.to_numeric(signals["delta_value"], errors="coerce")
    st.dataframe(
        signals[
            [
                "cusip",
                "name_of_issuer",
                "direction",
                "consensus_direction",
                "consensus_count",
                "delta_value",
                "report_date",
            ]
        ].rename(
            columns={
                "name_of_issuer": "Issuer",
                "direction": "Manager Direction",
                "consensus_direction": "Consensus",
                "consensus_count": "Consensus Count",
                "delta_value": "Delta Value",
                "report_date": "Report Date",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )


def render_conviction_signals_dashboard(selected_manager_id: int) -> None:
    left_col, right_col = st.columns((3, 2), gap="large")
    with left_col:
        render_top_convictions(selected_manager_id, show_heading=False)
        st.divider()
        render_manager_crowded_trades(selected_manager_id, show_heading=False)
    with right_col:
        render_contrarian_alerts(selected_manager_id, show_heading=False)


def render_activism_dashboard(selected_manager_id: int) -> None:
    render_activism_timeline(selected_manager_id, show_heading=False)
    st.divider()
    left_col, right_col = st.columns((3, 2), gap="large")
    with left_col:
        render_current_activism_stakes(selected_manager_id, show_heading=False)
    with right_col:
        render_ownership_chart(selected_manager_id, show_heading=False)


def render_qc_flags(selected_manager_id: int | None, show_heading: bool = True) -> None:
    if show_heading:
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
        else:
            filing_delta = f"-{max(120 - filing_days, 0)}d to 120d SLA"
        filing_value = filing_date.strftime("%Y-%m-%d")
    else:
        filing_value = "N/A"
        filing_delta = "+1 missing filing date"
    col_filing.metric("Last Filing Date", filing_value, delta=filing_delta, delta_color="inverse")

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


def load_all_managers_summary() -> dict[str, Any]:
    conn = connect_db()
    summary: dict[str, Any] = {
        "total_managers": 0,
        "total_filings": 0,
        "total_holdings": 0,
        "total_news_items": 0,
        "recent_activity": pd.DataFrame(
            columns=["activity_date", "filings", "holdings", "news_items"]
        ),
        "stale_managers": pd.DataFrame(
            columns=["manager_id", "name", "last_filing_date", "warning"]
        ),
    }
    try:
        totals_df = pd.read_sql_query(
            (
                "SELECT "
                "(SELECT COUNT(*) FROM managers) AS total_managers, "
                "(SELECT COUNT(*) FROM filings) AS total_filings, "
                "(SELECT COUNT(*) FROM holdings) AS total_holdings, "
                "(SELECT COUNT(*) FROM news_items) AS total_news_items"
            ),
            conn,
        )
        if not totals_df.empty:
            totals = totals_df.iloc[0]
            summary["total_managers"] = int(totals["total_managers"])
            summary["total_filings"] = int(totals["total_filings"])
            summary["total_holdings"] = int(totals["total_holdings"])
            summary["total_news_items"] = int(totals["total_news_items"])

        activity_df = pd.read_sql_query(
            (
                "SELECT activity_date, "
                "SUM(filings_count) AS filings, "
                "SUM(holdings_count) AS holdings, "
                "SUM(news_count) AS news_items "
                "FROM ("
                "SELECT DATE(filed_date) AS activity_date, COUNT(*) AS filings_count, 0 AS holdings_count, 0 AS news_count "
                "FROM filings GROUP BY DATE(filed_date) "
                "UNION ALL "
                "SELECT DATE(f.filed_date) AS activity_date, 0 AS filings_count, COUNT(*) AS holdings_count, 0 AS news_count "
                "FROM holdings h JOIN filings f ON f.filing_id = h.filing_id GROUP BY DATE(f.filed_date) "
                "UNION ALL "
                "SELECT DATE(published_at) AS activity_date, 0 AS filings_count, 0 AS holdings_count, COUNT(*) AS news_count "
                "FROM news_items GROUP BY DATE(published_at)"
                ") daily "
                "GROUP BY activity_date "
                "ORDER BY activity_date DESC LIMIT 30"
            ),
            conn,
        )
        if not activity_df.empty:
            activity_df["activity_date"] = pd.to_datetime(
                activity_df["activity_date"], errors="coerce"
            )
            summary["recent_activity"] = activity_df.sort_values("activity_date")

        managers_df = pd.read_sql_query("SELECT manager_id, name FROM managers", conn)
        filings_df = pd.read_sql_query(
            "SELECT manager_id, filing_id, type, filed_date FROM filings", conn
        )
        holdings_by_filing = pd.read_sql_query(
            "SELECT filing_id, COUNT(*) AS holdings_count FROM holdings GROUP BY filing_id",
            conn,
        )
        now_utc = datetime.now(UTC)
        warnings: list[dict[str, Any]] = []

        if not managers_df.empty:
            for manager in managers_df.itertuples(index=False):
                manager_filings = filings_df[filings_df["manager_id"] == manager.manager_id].copy()
                if manager_filings.empty:
                    warnings.append(
                        {
                            "manager_id": manager.manager_id,
                            "name": manager.name,
                            "last_filing_date": None,
                            "warning": "No filings available",
                        }
                    )
                    continue

                manager_filings["filed_date"] = pd.to_datetime(
                    manager_filings["filed_date"], errors="coerce"
                )
                latest_filing = manager_filings.sort_values("filed_date", ascending=False).iloc[0]
                latest_filing_id = int(latest_filing["filing_id"])
                latest_holdings_count = holdings_by_filing.loc[
                    holdings_by_filing["filing_id"] == latest_filing_id,
                    "holdings_count",
                ].sum()
                latest_filing_ts = pd.to_datetime(latest_filing["filed_date"], errors="coerce")
                days_since_filing = None
                if pd.notna(latest_filing_ts):
                    days_since_filing = (
                        now_utc - latest_filing_ts.to_pydatetime().replace(tzinfo=UTC)
                    ).days

                warning_reasons: list[str] = []
                is_13f_filer = (
                    manager_filings["type"].astype(str).str.upper().str.startswith("13F").any()
                )
                if is_13f_filer and days_since_filing is not None and days_since_filing > 120:
                    warning_reasons.append(f"13F filing stale by {days_since_filing - 120} days")
                if int(latest_holdings_count) == 0:
                    warning_reasons.append("Latest filing has zero holdings")

                if warning_reasons:
                    warnings.append(
                        {
                            "manager_id": manager.manager_id,
                            "name": manager.name,
                            "last_filing_date": latest_filing_ts,
                            "warning": "; ".join(warning_reasons),
                        }
                    )

        if warnings:
            stale_df = pd.DataFrame(warnings).sort_values(
                by=["last_filing_date", "name"], ascending=[True, True], na_position="first"
            )
            summary["stale_managers"] = stale_df
    except Exception:
        pass
    finally:
        conn.close()
    return summary


def render_all_managers_summary(show_heading: bool = True) -> None:
    if show_heading:
        st.subheader("All Managers Summary")
    summary = load_all_managers_summary()

    col_managers, col_filings, col_holdings, col_news = st.columns(4)
    col_managers.metric("Total Managers", f"{summary['total_managers']:,}")
    col_filings.metric("Total Filings", f"{summary['total_filings']:,}")
    col_holdings.metric("Total Holdings", f"{summary['total_holdings']:,}")
    col_news.metric("Total News Items", f"{summary['total_news_items']:,}")
    render_active_campaigns_widget(show_heading=False)

    activity = summary["recent_activity"]
    st.markdown("Recent Activity (30 days)")
    if isinstance(activity, pd.DataFrame) and not activity.empty:
        spark_cols = st.columns(3)
        spark_specs = [
            ("filings", "Filings"),
            ("holdings", "Holdings"),
            ("news_items", "News"),
        ]
        for col, (field, label) in zip(spark_cols, spark_specs, strict=False):
            col.caption(label)
            spark_chart = (
                alt.Chart(activity)
                .mark_line(color="#1D4ED8")
                .encode(
                    x=alt.X("activity_date:T", axis=None),
                    y=alt.Y(f"{field}:Q", axis=None),
                    tooltip=[
                        alt.Tooltip("activity_date:T", title="Date"),
                        alt.Tooltip(f"{field}:Q", title=label),
                    ],
                )
                .properties(height=70)
            )
            col.altair_chart(spark_chart, use_container_width=True)
    else:
        st.caption("No recent activity available.")

    stale_managers = summary["stale_managers"]
    st.markdown("Managers with QC Warnings")
    if isinstance(stale_managers, pd.DataFrame) and not stale_managers.empty:
        stale_display = stale_managers.copy()
        stale_display["last_filing_date"] = pd.to_datetime(
            stale_display["last_filing_date"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        st.dataframe(
            stale_display[["name", "last_filing_date", "warning"]],
            use_container_width=True,
        )
    else:
        st.caption("No stale manager warnings.")


def render_manager_dashboard(selected_manager_id: int) -> None:
    def _render_portfolio() -> None:
        left_col, right_col = st.columns((3, 2), gap="large")
        with left_col:
            with st.expander("Filing Timeline", expanded=True):
                render_filing_timeline(selected_manager_id, show_heading=False)
            with st.expander("Latest Holdings Snapshot", expanded=True):
                render_latest_holdings_snapshot(selected_manager_id, show_heading=False)
            with st.expander("Top Deltas", expanded=True):
                render_top_deltas(selected_manager_id, show_heading=False)

        with right_col:
            with st.expander("News", expanded=True):
                render_news_stream(selected_manager_id, show_heading=False)
            with st.expander("QC Flags", expanded=True):
                render_qc_flags(selected_manager_id, show_heading=False)

    def _render_signals() -> None:
        with st.expander("Top Convictions", expanded=True):
            render_top_convictions(selected_manager_id, show_heading=False)
        with st.expander("Crowded Trades", expanded=True):
            render_manager_crowded_trades(selected_manager_id, show_heading=False)
        with st.expander("Contrarian Alerts", expanded=True):
            render_contrarian_alerts(selected_manager_id, show_heading=False)

    if not hasattr(st, "tabs"):
        _render_portfolio()
        if hasattr(st, "divider"):
            st.divider()
        _render_signals()
        return

    portfolio_tab, activism_tab, signals_tab = st.tabs(
        ["Portfolio", "Activism", "Conviction & Signals"]
    )
    with portfolio_tab:
        _render_portfolio()
    with activism_tab:
        render_activism_dashboard(selected_manager_id)
    with signals_tab:
        render_conviction_signals_dashboard(selected_manager_id)


def render_historical_filing_trend() -> None:
    df = load_delta()
    if df.empty:
        st.info("No data available")
        return
    chart = alt.Chart(df).mark_line().encode(x="date:T", y="filings:Q")
    st.altair_chart(chart, use_container_width=True)
    st.dataframe(df)


def main() -> None:
    if not require_login():
        st.stop()
    alert_count = load_unacknowledged_alert_count()
    st.sidebar.markdown("### Navigation")
    st.sidebar.markdown(
        (
            "<div style='display:flex;align-items:center;gap:8px;'>"
            "<span>Alerts</span>"
            "<span style='background:#d7263d;color:white;border-radius:999px;padding:2px 8px;"
            "font-size:12px;font-weight:700;'>"
            f"{alert_count}</span></div>"
        ),
        unsafe_allow_html=True,
    )
    st.sidebar.metric("Unacknowledged Alerts", alert_count)
    st.header("Holdings Delta")
    selected_manager_id = render_manager_selector()
    if selected_manager_id is None:
        with st.expander("All Managers Summary", expanded=True):
            render_all_managers_summary(show_heading=False)
        with st.expander("News", expanded=True):
            render_news_stream(None, show_heading=False)
    else:
        render_manager_dashboard(selected_manager_id)

    with st.expander("Historical Filing Trend", expanded=False):
        render_historical_filing_trend()


if __name__ == "__main__":
    main()
