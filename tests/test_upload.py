import sqlite3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ui.search import search_notes
from ui.upload import save_note


def setup_db(tmp_path: Path) -> str:
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY AUTOINCREMENT, filename TEXT, content TEXT)"
    )
    conn.commit()
    conn.close()
    return str(db_path)


def test_save_and_search(tmp_path: Path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    save_note("hello world", "note.txt")
    df = search_notes("hello")
    assert list(df["filename"]) == ["note.txt"]
