"""Simple news search page."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from adapters.base import connect_db
from . import require_login


def search_news(term: str) -> pd.DataFrame:
    conn = connect_db()
    df = pd.read_sql_query(
        "SELECT headline, source FROM news WHERE headline LIKE ? ORDER BY published DESC LIMIT 20",
        conn,
        params=(f"%{term}%",),
    )
    conn.close()
    return df


def main() -> None:
    if not require_login():
        st.stop()
    st.header("News Search")
    q = st.text_input("Keyword")
    if q:
        df = search_news(q)
        st.dataframe(df)


if __name__ == "__main__":
    main()
