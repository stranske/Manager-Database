"""Streamlit multipage shell for analyst workflows."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import streamlit as st

ROOT = str(Path(__file__).resolve().parents[1])
if sys.path[0] != ROOT:
    sys.path.insert(0, ROOT)

from ui import (  # noqa: E402
    ALERTS_URL_PATH,
    alerts,
    daily_report,
    dashboard,
    research,
    search,
    upload,
)


def _build_pages() -> list[Any]:
    """Define sidebar navigation and URL paths for all UI pages."""
    return [
        st.Page(dashboard.main, title="Dashboard", icon="📈", url_path="", default=True),
        st.Page(alerts.main, title="Alerts", icon="🔔", url_path=ALERTS_URL_PATH),
        st.Page(daily_report.main, title="Daily Report", icon="🗞️", url_path="daily-report"),
        st.Page(search.main, title="Search", icon="🔎", url_path="search"),
        st.Page(upload.main, title="Upload", icon="📝", url_path="upload"),
        st.Page(research.main, title="🔬 Research", icon="🔬", url_path="research"),
    ]


def main() -> None:
    navigation = st.navigation(_build_pages(), position="sidebar")
    navigation.run()


if __name__ == "__main__":
    main()
