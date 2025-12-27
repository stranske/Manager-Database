"""Holdings delta dashboard with sparkline."""

from __future__ import annotations

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


def main() -> None:
    if not require_login():
        st.stop()
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
