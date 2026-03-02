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


def main() -> None:
    if not require_login():
        st.stop()
    st.header("Holdings Delta")
    render_manager_selector()
    df = load_delta()
    if df.empty:
        st.info("No data available")
        return
    chart = alt.Chart(df).mark_line().encode(x="date:T", y="filings:Q")
    st.altair_chart(chart, use_container_width=True)
    st.dataframe(df)


if __name__ == "__main__":
    main()
