import datetime as dt

import pandas as pd
import streamlit as st

from adapters.base import connect_db
from . import require_login


@st.cache_data(show_spinner=False)
def load_diffs(date: str) -> pd.DataFrame:
    conn = connect_db()
    query = "SELECT cik, cusip, change FROM daily_diff WHERE date = ?"
    df = pd.read_sql_query(query, conn, params=(date,))
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
        df["Δ"] = df["change"].map(arrow)
        html = df[["cik", "cusip", "Δ"]].to_html(escape=False, index=False)
        st.markdown(html, unsafe_allow_html=True)
        csv = df[["cik", "cusip", "change"]].to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV", csv, file_name=f"diff_{date_str}.csv", mime="text/csv"
        )
    with tab2:
        news = load_news(date_str)
        st.dataframe(news)


if __name__ == "__main__":
    main()
