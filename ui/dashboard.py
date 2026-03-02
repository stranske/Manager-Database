"""Holdings delta dashboard with sparkline."""

from __future__ import annotations

import os

import altair as alt
import httpx
import pandas as pd
import streamlit as st

from adapters.base import connect_db

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
    df = load_delta()
    if df.empty:
        st.info("No data available")
        return
    chart = alt.Chart(df).mark_line().encode(x="date:T", y="filings:Q")
    st.altair_chart(chart, use_container_width=True)
    st.dataframe(df)


if __name__ == "__main__":
    main()
