import sqlite3
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ui.dashboard import load_delta, load_managers, render_manager_selector


def setup_db(tmp_path: Path) -> str:
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute(
        "CREATE TABLE holdings (cik TEXT, accession TEXT, filed DATE, nameOfIssuer TEXT, cusip TEXT, value INTEGER, sshPrnamt INTEGER)"
    )
    manager_rows = [
        (2, "Zulu Capital"),
        (1, "Alpha Partners"),
    ]
    rows = [
        ("0", "a", "2024-01-01", "CorpA", "AAA", 1, 1),
        ("0", "b", "2024-01-02", "CorpB", "BBB", 1, 1),
        ("0", "c", "2024-01-02", "CorpC", "CCC", 1, 1),
    ]
    conn.executemany("INSERT INTO managers VALUES (?,?)", manager_rows)
    conn.executemany("INSERT INTO holdings VALUES (?,?,?,?,?,?,?)", rows)
    conn.commit()
    conn.close()
    return str(db_path)


def test_load_delta_counts(tmp_path: Path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    df = load_delta()
    assert list(df["date"]) == ["2024-01-01", "2024-01-02"]
    assert list(df["filings"]) == [1, 2]


def test_load_managers_sorted(tmp_path: Path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    st.cache_data.clear()
    df = load_managers()
    assert list(df["name"]) == ["Alpha Partners", "Zulu Capital"]
    assert list(df["manager_id"]) == [1, 2]


class FakeStreamlit:
    def __init__(self):
        self.session_state = {}

    def selectbox(self, _label, options, index, format_func, key):
        self.session_state.setdefault(key, options[index])
        return self.session_state[key]


def test_render_manager_selector_default_and_persist(monkeypatch):
    fake_st = FakeStreamlit()
    monkeypatch.setattr("ui.dashboard.st", fake_st)
    monkeypatch.setattr(
        "ui.dashboard.load_managers",
        lambda: pd.DataFrame(
            [
                {"manager_id": 1, "name": "Alpha Partners"},
                {"manager_id": 2, "name": "Zulu Capital"},
            ]
        ),
    )

    selected = render_manager_selector()
    assert selected is None
    assert fake_st.session_state["selected_manager_id"] == "all"

    fake_st.session_state["selected_manager_id"] = 2
    selected = render_manager_selector()
    assert selected == 2
