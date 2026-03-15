import datetime as dt
import html
import logging
import sqlite3
import time

import pandas as pd
import streamlit as st

from adapters.base import connect_db
from api.signals import query_contrarian_signals, query_crowded_trades

from . import require_login

logger = logging.getLogger(__name__)


@st.cache_data(show_spinner=False)
def load_diffs(date: str) -> pd.DataFrame:
    started = time.perf_counter()
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
    elapsed_ms = (time.perf_counter() - started) * 1000
    logger.info("daily_report.load_diffs completed", extra={"date": date, "elapsed_ms": elapsed_ms})
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


@st.cache_data(show_spinner=False)
def load_activism_events(date: str) -> pd.DataFrame:
    conn = connect_db()
    is_sqlite = isinstance(conn, sqlite3.Connection)
    placeholder = "?" if is_sqlite else "%s"
    try:
        query = (
            "SELECT m.name AS manager_name, ae.event_type, ae.subject_company, "
            "ae.ownership_pct, ae.previous_pct, ae.delta_pct, af.filed_date "
            "FROM activism_events ae "
            "JOIN activism_filings af ON af.filing_id = ae.filing_id "
            "LEFT JOIN managers m ON m.manager_id = ae.manager_id "
            f"WHERE af.filed_date = {placeholder} "
            "ORDER BY af.filed_date DESC, ae.detected_at DESC"
        )
        table = pd.read_sql_query(query, conn, params=(date,))
    except Exception:
        table = pd.DataFrame()
    finally:
        conn.close()
    return table


@st.cache_data(show_spinner=False)
def load_crowded_trades(date: str, min_managers: int = 3, limit: int = 20) -> pd.DataFrame:
    conn = connect_db()
    try:
        rows = query_crowded_trades(
            conn,
            report_date=dt.date.fromisoformat(date),
            min_managers=min_managers,
            limit=limit,
        )
    except Exception:
        rows = []
    finally:
        conn.close()
    return pd.DataFrame([row.model_dump() for row in rows])


@st.cache_data(show_spinner=False)
def load_contrarian_signals(date: str, limit: int = 200) -> pd.DataFrame:
    conn = connect_db()
    try:
        rows = query_contrarian_signals(
            conn,
            report_date=dt.date.fromisoformat(date),
            limit=limit,
        )
    except Exception:
        rows = []
    finally:
        conn.close()
    return pd.DataFrame([row.model_dump() for row in rows])


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


def format_shares_delta(shares_prev: object, shares_curr: object) -> str:
    prev = pd.to_numeric(pd.Series([shares_prev]), errors="coerce").iloc[0]
    curr = pd.to_numeric(pd.Series([shares_curr]), errors="coerce").iloc[0]
    if pd.isna(prev) and pd.isna(curr):
        return "<span style='color:#666;'>-</span>"
    prev_value = 0 if pd.isna(prev) else prev
    curr_value = 0 if pd.isna(curr) else curr
    delta = int(round(curr_value - prev_value))
    if delta > 0:
        return f"<span style='color:green;'>↑ +{delta:,}</span>"
    if delta < 0:
        return f"<span style='color:red;'>↓ {delta:,}</span>"
    return "<span style='color:#666;'>→ 0</span>"


def format_value_delta(value_prev: object, value_curr: object) -> str:
    prev = pd.to_numeric(pd.Series([value_prev]), errors="coerce").iloc[0]
    curr = pd.to_numeric(pd.Series([value_curr]), errors="coerce").iloc[0]
    if pd.isna(prev) and pd.isna(curr):
        return "<span style='color:#666;'>-</span>"
    prev_value = 0.0 if pd.isna(prev) else float(prev)
    curr_value = 0.0 if pd.isna(curr) else float(curr)
    delta = curr_value - prev_value
    if delta > 0:
        return f"<span style='color:green;'>↑ +${delta:,.2f}</span>"
    if delta < 0:
        return f"<span style='color:red;'>↓ -${abs(delta):,.2f}</span>"
    return "<span style='color:#666;'>→ $0.00</span>"


def format_percent_change(value_prev: object, value_curr: object) -> str:
    prev = pd.to_numeric(pd.Series([value_prev]), errors="coerce").iloc[0]
    curr = pd.to_numeric(pd.Series([value_curr]), errors="coerce").iloc[0]
    if pd.isna(prev) and pd.isna(curr):
        return "<span style='color:#666;'>-</span>"
    prev_value = 0.0 if pd.isna(prev) else float(prev)
    curr_value = 0.0 if pd.isna(curr) else float(curr)
    if prev_value == 0:
        return "<span style='color:#666;'>n/a</span>"
    pct = ((curr_value - prev_value) / prev_value) * 100
    if pct > 0:
        return f"<span style='color:green;'>+{pct:.1f}%</span>"
    if pct < 0:
        return f"<span style='color:red;'>{pct:.1f}%</span>"
    return "<span style='color:#666;'>0.0%</span>"


def format_activism_event_type(event_type: object) -> str:
    event_name = str(event_type or "").strip()
    colors = {
        "initial_stake": "#2563eb",
        "threshold_crossing": "#dc2626",
        "form_upgrade": "#f97316",
    }
    color = colors.get(event_name, "#475569")
    return (
        f"<span style='color:{color};font-weight:700;'>"
        f"{html.escape(event_name or '-')}"
        "</span>"
    )


def format_signal_badge(direction: object, consensus_direction: object) -> str:
    manager_direction = html.escape(str(direction or "").strip() or "-")
    consensus = html.escape(str(consensus_direction or "").strip() or "-")
    return (
        "<span style='display:inline-block;padding:2px 8px;border-radius:999px;"
        "background:#fee2e2;color:#991b1b;font-size:0.75rem;font-weight:700;'>"
        f"Contrarian: {manager_direction} vs {consensus}</span>"
    )


def main():
    if not require_login():
        st.stop()
    date = st.date_input("Date", dt.date.today() - dt.timedelta(days=1))
    date_str = str(date)
    tab1, tab2, tab3 = st.tabs(["Filings & Diffs", "Crowded Trades", "News Pulse"])
    with tab1:
        df = load_diffs(date_str)
        contrarian = load_contrarian_signals(date_str)
        contrarian_lookup: dict[tuple[str, str], str] = {}
        if not contrarian.empty:
            # Map exact manager/cusip combinations so only the relevant rows get flagged.
            for row in contrarian.itertuples(index=False):
                manager_name = str(getattr(row, "manager_name", "") or "").strip()
                cusip = str(getattr(row, "cusip", "") or "").strip()
                if manager_name and cusip:
                    contrarian_lookup[(manager_name, cusip)] = format_signal_badge(
                        getattr(row, "direction", None),
                        getattr(row, "consensus_direction", None),
                    )
        total_managers = df.get("manager_name", pd.Series(dtype=object)).nunique()
        total_rows = len(df)
        adds = int((df.get("delta_type") == "ADD").sum()) if "delta_type" in df.columns else 0
        exits = int((df.get("delta_type") == "EXIT").sum()) if "delta_type" in df.columns else 0
        increases = (
            int((df.get("delta_type") == "INCREASE").sum()) if "delta_type" in df.columns else 0
        )
        decreases = (
            int((df.get("delta_type") == "DECREASE").sum()) if "delta_type" in df.columns else 0
        )
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Managers w/ changes", f"{total_managers:,}", delta=f"{total_rows:,} positions")
        col2.metric(
            "Added",
            f"{adds:,}",
            delta=f"{(adds / total_rows * 100):.1f}%" if total_rows else "0.0%",
        )
        col3.metric(
            "Exited",
            f"{exits:,}",
            delta=f"{(exits / total_rows * 100):.1f}%" if total_rows else "0.0%",
        )
        col4.metric(
            "Increased",
            f"{increases:,}",
            delta=f"{(increases / total_rows * 100):.1f}%" if total_rows else "0.0%",
        )
        col5.metric(
            "Decreased",
            f"{decreases:,}",
            delta=f"{(decreases / total_rows * 100):.1f}%" if total_rows else "0.0%",
        )
        df["Shares Δ"] = df.apply(
            lambda row: format_shares_delta(row.get("shares_prev"), row.get("shares_curr")), axis=1
        )
        df["Value Δ"] = df.apply(
            lambda row: format_value_delta(row.get("value_prev"), row.get("value_curr")), axis=1
        )
        df["% Δ"] = df.apply(
            lambda row: format_percent_change(row.get("value_prev"), row.get("value_curr")), axis=1
        )
        df["Signals"] = df.apply(
            lambda row: contrarian_lookup.get(
                (
                    str(row.get("manager_name") or "").strip(),
                    str(row.get("cusip") or "").strip(),
                ),
                "<span style='color:#666;'>-</span>",
            ),
            axis=1,
        )
        html = df[
            ["manager_name", "cusip", "name_of_issuer", "Shares Δ", "Value Δ", "% Δ", "Signals"]
        ].to_html(escape=False, index=False)
        st.markdown(html, unsafe_allow_html=True)
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", csv, file_name=f"diff_{date_str}.csv", mime="text/csv")

        st.markdown("### Activism Alerts")
        activism_events = load_activism_events(date_str)
        if activism_events.empty:
            st.caption("No activism events detected for this report date.")
        else:
            activism_events = activism_events.copy()
            activism_events["Event Type"] = activism_events["event_type"].apply(
                format_activism_event_type
            )
            activism_events["Manager"] = activism_events["manager_name"].fillna("Unknown")
            activism_events["Subject Company"] = activism_events["subject_company"].fillna("")
            activism_events["Ownership %"] = pd.to_numeric(
                activism_events["ownership_pct"], errors="coerce"
            ).round(2)
            activism_events["Change"] = activism_events.apply(
                lambda row: format_percent_change(
                    row.get("previous_pct"),
                    row.get("ownership_pct"),
                ),
                axis=1,
            )
            activism_events["Filed Date"] = pd.to_datetime(
                activism_events["filed_date"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")
            activism_html = activism_events[
                [
                    "Manager",
                    "Event Type",
                    "Subject Company",
                    "Ownership %",
                    "Change",
                    "Filed Date",
                ]
            ].to_html(escape=False, index=False)
            st.markdown(activism_html, unsafe_allow_html=True)
    with tab2:
        crowded = load_crowded_trades(date_str)
        if crowded.empty:
            st.info("No crowded trades detected for this report date.")
        else:
            crowded = crowded.copy()
            crowded["manager_names"] = crowded["manager_names"].apply(
                lambda names: ", ".join(names) if isinstance(names, list) else ""
            )
            crowded["total_value_usd"] = pd.to_numeric(crowded["total_value_usd"], errors="coerce")
            crowded["avg_conviction_pct"] = pd.to_numeric(
                crowded["avg_conviction_pct"], errors="coerce"
            ).round(2)
            sort_choice = st.selectbox(
                "Sort crowded trades by",
                ["Manager count", "Total value"],
                key=f"crowded_sort_{date_str}",
            )
            if sort_choice == "Total value":
                crowded = crowded.sort_values(
                    ["total_value_usd", "manager_count"],
                    ascending=[False, False],
                )
            else:
                crowded = crowded.sort_values(
                    ["manager_count", "total_value_usd"],
                    ascending=[False, False],
                )
            crowded_display = crowded.rename(
                columns={
                    "cusip": "CUSIP",
                    "name_of_issuer": "Issuer",
                    "manager_count": "# Managers",
                    "total_value_usd": "Total Value",
                    "avg_conviction_pct": "Avg Conviction %",
                    "manager_names": "Managers",
                }
            )
            st.dataframe(
                crowded_display[
                    [
                        "CUSIP",
                        "Issuer",
                        "# Managers",
                        "Total Value",
                        "Avg Conviction %",
                        "Managers",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )
            crowded_csv = crowded_display.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download Crowded Trades CSV",
                crowded_csv,
                file_name=f"crowded_trades_{date_str}.csv",
                mime="text/csv",
            )
    with tab3:
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
