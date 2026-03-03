import sqlite3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ui.upload import save_note
from utils.extract import extract_text


def setup_db(tmp_path: Path) -> str:
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    conn.commit()
    conn.close()
    return str(db_path)


def test_save_note_stores_in_documents(tmp_path: Path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    doc_id = save_note("hello world", "note.txt")
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT doc_id, filename, kind, text FROM documents WHERE doc_id = ?", (doc_id,)
    ).fetchone()
    conn.close()
    assert row == (doc_id, "note.txt", "note", "hello world")


def test_extract_text_txt_and_md():
    assert extract_text(b"alpha", "a.txt") == "alpha"
    assert extract_text(b"# heading", "a.md") == "# heading"
