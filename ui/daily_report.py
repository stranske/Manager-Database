import datetime as dt
import html
import sqlite3

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
    try:
        if isinstance(conn, sqlite3.Connection):
            query = (
                "SELECT n.headline, n.url, n.published_at, n.source, n.topics, n.confidence, "
                "m.name AS manager_name "
                "FROM news_items n "
                "LEFT JOIN managers m ON m.manager_id = n.manager_id "
                "WHERE date(n.published_at) = ? "
                "ORDER BY n.published_at DESC "
                "LIMIT 50"
            )
        else:
            query = (
                "SELECT n.headline, n.url, n.published_at, n.source, n.topics, n.confidence, "
                "m.name AS manager_name "
                "FROM news_items n "
                "LEFT JOIN managers m ON m.manager_id = n.manager_id "
                "WHERE n.published_at::date = %s "
                "ORDER BY n.published_at DESC "
                "LIMIT 50"
            )
        df = pd.read_sql_query(query, conn, params=(date,))
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
    finally:
        conn.close()
    return df


def parse_topics(value: object) -> list[str]:
    if value is None:
        return []
    raw = str(value).strip()
    if not raw:
        return []
    if raw.startswith("[") and raw.endswith("]"):
        raw = raw[1:-1]
    return [token.strip().strip("\"'") for token in raw.split(",") if token.strip()]


def topic_choices(news: pd.DataFrame) -> list[str]:
    topics = sorted({topic for value in news.get("topics", []) for topic in parse_topics(value)})
    return ["All topics", *topics]


def format_news_table(news: pd.DataFrame) -> pd.DataFrame:
    table = news.copy()
    published = pd.to_datetime(table["published_at"], errors="coerce")
    table["Time"] = published.dt.strftime("%H:%M").fillna("-")
    table["Manager"] = table["manager_name"].fillna("Unknown")
    table["Headline"] = table["headline"].fillna("")
    table["Source"] = table["source"].fillna("")
    table["Topics"] = table["topics"].apply(lambda value: ", ".join(parse_topics(value)))
    return table[["Time", "Manager", "Headline", "Source", "Topics"]]


def topic_badges(value: object) -> str:
    palette = ["#0ea5e9", "#16a34a", "#f97316", "#7c3aed", "#dc2626", "#0891b2"]
    badges = []
    for idx, topic in enumerate(parse_topics(value)):
        color = palette[idx % len(palette)]
        badges.append(
            f"<span style='display:inline-block;padding:2px 8px;margin-right:4px;"
            f"border-radius:12px;background:{color};color:white;font-size:0.75rem;'>{topic}</span>"
        )
    return "".join(badges) if badges else "<span style='color:#666;'>-</span>"


def headline_markdown(headline: object, url: object) -> str:
    text = html.escape(str(headline).strip()) if headline else "Untitled"
    text = text.replace("\\", "\\\\").replace("[", "\\[").replace("]", "\\]")
    if not url:
        return text
    raw_url = str(url).strip()
    if not raw_url:
        return text
    # Wrap the URL in <> so markdown handles parentheses in links correctly.
    return f"[{text}](<{raw_url}>)"


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
        st.download_button("Download CSV", csv, file_name=f"diff_{date_str}.csv", mime="text/csv")
    with tab2:
        news = load_news(date_str)
        if news.empty:
            st.info("No news for this date.")
            return

        selected_topic = st.selectbox(
            "Filter by topic", topic_choices(news), key=f"news_topic_{date_str}"
        )
        if selected_topic != "All topics":
            news = news[
                news["topics"].apply(lambda value: selected_topic in parse_topics(value))
            ].reset_index(drop=True)

        if news.empty:
            st.info(f"No news matching '{selected_topic}' on {date_str}.")
            return

        styled = format_news_table(news).style.set_properties(
            **{"background-color": "#f8fafc", "border-color": "#e2e8f0"}
        )
        st.dataframe(styled, use_container_width=True, hide_index=True)

        st.markdown("#### Linked Headlines")
        for _, row in news.iterrows():
            manager = row.get("manager_name") or "Unknown"
            source = row.get("source") or "Unknown"
            timestamp = pd.to_datetime(row.get("published_at"), errors="coerce")
            time_text = timestamp.strftime("%H:%M") if pd.notna(timestamp) else "-"
            title_md = headline_markdown(row.get("headline"), row.get("url"))
            st.markdown(
                f"**{time_text}** | **{manager}** | {title_md} | *{source}*  \n{topic_badges(row.get('topics'))}",
                unsafe_allow_html=True,
            )

        news_export = format_news_table(news).to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download News CSV",
            news_export,
            file_name=f"news_{date_str}.csv",
            mime="text/csv",
        )


if __name__ == "__main__":
    main()
