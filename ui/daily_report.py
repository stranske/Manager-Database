import datetime as dt
import sqlite3

import pandas as pd
import streamlit as st

from adapters.base import connect_db

from . import require_login


@st.cache_data(show_spinner=False)
def load_diffs(date: str) -> pd.DataFrame:
    conn = connect_db()
    is_sqlite = isinstance(conn, sqlite3.Connection)
    placeholder = "?" if is_sqlite else "%s"
    view_query = f"""SELECT manager_name, cusip, name_of_issuer, delta_type,
         shares_prev, shares_curr, value_prev, value_curr
  FROM mv_daily_report
  WHERE report_date = {placeholder}
  ORDER BY manager_name, delta_type"""
    fallback_query = f"""SELECT cik AS manager_name, cusip, '' AS name_of_issuer, change AS delta_type,
         NULL AS shares_prev, NULL AS shares_curr, NULL AS value_prev, NULL AS value_curr
  FROM daily_diff
  WHERE date = {placeholder}
  ORDER BY manager_name, delta_type"""
    try:
        df = pd.read_sql_query(view_query, conn, params=(date,))
    except Exception as exc:
        if is_sqlite and "no such table" in str(exc).lower() and "mv_daily_report" in str(exc):
            df = pd.read_sql_query(fallback_query, conn, params=(date,))
        else:
            conn.close()
            raise
    conn.close()
    return df


@st.cache_data(show_spinner=False)
def load_news(date: str) -> pd.DataFrame:
    conn = connect_db()
    query = "SELECT headline, source FROM news WHERE substr(published, 1, 10) = ? ORDER BY published DESC LIMIT 20"
    try:
        df = pd.read_sql_query(query, conn, params=(date,))
    except Exception:
        df = pd.DataFrame(columns=["headline", "source"])
    conn.close()
    return df


def main():
    if not require_login():
        st.stop()
    date = st.date_input("Date", dt.date.today() - dt.timedelta(days=1))
    date_str = str(date)
    tab1, tab2 = st.tabs(["Filings & Diffs", "News Pulse"])
    with tab1:
        df = load_diffs(date_str)
        # map change to coloured arrows for on-screen table
        arrow = {
            "ADD": "<span style='color:green'>&uarr;</span>",
            "EXIT": "<span style='color:red'>&darr;</span>",
        }
        df["Δ"] = df["delta_type"].map(arrow)
        html = df[["manager_name", "cusip", "name_of_issuer", "Δ"]].to_html(
            escape=False, index=False
        )
        st.markdown(html, unsafe_allow_html=True)
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", csv, file_name=f"diff_{date_str}.csv", mime="text/csv")
    with tab2:
        news = load_news(date_str)
        st.dataframe(news)


if __name__ == "__main__":
    main()
