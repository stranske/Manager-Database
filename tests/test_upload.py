import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ui import upload
from ui.upload import _file_exceeds_limit, _get_max_upload_bytes, _store_uploaded_text
from utils.extract import extract_text

FIXTURE_PDF = Path(__file__).resolve().parent / "fixtures" / "sample.pdf"


def setup_db(tmp_path: Path) -> str:
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    conn.commit()
    conn.close()
    return str(db_path)


def test_upload_text_and_markdown_store_in_documents(tmp_path: Path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)

    txt_content = extract_text(b"hello world", "note.txt")
    txt_id = _store_uploaded_text(txt_content, "note.txt")
    md_content = extract_text(b"# heading", "memo.md")
    md_id = _store_uploaded_text(md_content, "memo.md")

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT doc_id, filename, kind, text FROM documents WHERE doc_id IN (?, ?) ORDER BY doc_id",
        (txt_id, md_id),
    ).fetchall()
    conn.close()

    assert rows == [
        (txt_id, "note.txt", "note", "hello world"),
        (md_id, "memo.md", "memo", "# heading"),
    ]


def test_extract_text_txt_and_md():
    assert extract_text(b"alpha", "a.txt") == "alpha"
    assert extract_text(b"# heading", "a.md") == "# heading"


def test_extract_text_pdf_fixture():
    pdf_text = extract_text(FIXTURE_PDF.read_bytes(), "sample.pdf")
    assert "Sample PDF fixture text for upload tests." in pdf_text


def test_pdf_upload_flow_stores_in_documents_and_not_notes(tmp_path: Path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)

    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE notes (filename TEXT, content TEXT)")
    conn.commit()
    conn.close()

    pdf_text = extract_text(FIXTURE_PDF.read_bytes(), "sample.pdf")
    doc_id = _store_uploaded_text(pdf_text, "sample.pdf")

    conn = sqlite3.connect(db_path)
    doc_row = conn.execute(
        "SELECT doc_id, filename, kind FROM documents WHERE doc_id = ?",
        (doc_id,),
    ).fetchone()
    notes_count = conn.execute("SELECT COUNT(*) FROM notes").fetchone()[0]
    conn.close()

    assert doc_row == (doc_id, "sample.pdf", "pdf")
    assert notes_count == 0


@pytest.mark.parametrize(
    ("filename", "raw_bytes", "expected_kind"),
    [
        ("note.txt", b"plain note body", "note"),
        ("memo.md", b"# memo heading", "memo"),
        ("sample.pdf", FIXTURE_PDF.read_bytes(), "pdf"),
    ],
)
def test_uploads_never_write_legacy_notes_table(
    tmp_path: Path, monkeypatch, filename: str, raw_bytes: bytes, expected_kind: str
):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)

    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE notes (filename TEXT, content TEXT)")
    conn.commit()
    conn.close()

    text = extract_text(raw_bytes, filename)
    doc_id = _store_uploaded_text(text, filename)

    conn = sqlite3.connect(db_path)
    doc_row = conn.execute(
        "SELECT doc_id, filename, kind FROM documents WHERE doc_id = ?",
        (doc_id,),
    ).fetchone()
    notes_count = conn.execute("SELECT COUNT(*) FROM notes").fetchone()[0]
    conn.close()

    assert doc_row == (doc_id, filename, expected_kind)
    assert notes_count == 0


def test_file_size_validation_rejects_oversized_file(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("MAX_UPLOAD_BYTES", "100")
    limit = _get_max_upload_bytes()
    assert limit == 100

    oversized = b"x" * 101
    assert _file_exceeds_limit(oversized)

    normal = b"x" * 100
    assert not _file_exceeds_limit(normal)


def test_file_size_validation_env_default(monkeypatch):
    monkeypatch.delenv("MAX_UPLOAD_BYTES", raising=False)
    limit = _get_max_upload_bytes()
    assert limit == 10 * 1024 * 1024


def test_file_size_validation_invalid_env_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("MAX_UPLOAD_BYTES", "not_a_number")
    limit = _get_max_upload_bytes()
    assert limit == 10 * 1024 * 1024


def test_corrupted_pdf_raises_exception():
    corrupted_bytes = b"%PDF-1.4 this is not a real pdf \x00\x01\x02"
    with pytest.raises(ValueError, match="Failed to extract PDF text"):
        extract_text(corrupted_bytes, "bad.pdf")


def test_extract_text_corrupted_pdf_propagates_exception():
    corrupted_bytes = b"%PDF-1.4 corrupted content \x00\x01\x02"
    with pytest.raises(ValueError, match="Failed to extract PDF text"):
        extract_text(corrupted_bytes, "bad.pdf")


def test_upload_history_lists_managers_and_recent_documents(tmp_path: Path, monkeypatch):
    """Keep the upload page's database-backed choices and history observable."""
    db_path = tmp_path / "upload-history.db"
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT NOT NULL);
        CREATE TABLE documents (
            doc_id INTEGER PRIMARY KEY,
            filename TEXT NOT NULL,
            kind TEXT NOT NULL,
            created_at TEXT NOT NULL,
            manager_id INTEGER
        );
        INSERT INTO managers (manager_id, name) VALUES (2, 'Zulu'), (1, 'Alpha');
        INSERT INTO documents (doc_id, filename, kind, created_at, manager_id)
        VALUES
            (10, 'older.md', 'memo', '2026-01-01T00:00:00Z', 2),
            (11, 'newer.txt', 'note', '2026-01-02T00:00:00Z', 1);
        """)
    conn.commit()
    conn.close()

    monkeypatch.setattr(upload, "connect_db", lambda: sqlite3.connect(db_path))

    assert upload._load_managers() == [(1, "Alpha"), (2, "Zulu")]
    recent = upload._recent_uploads(limit=1)
    assert recent.to_dict("records") == [
        {
            "doc_id": 11,
            "filename": "newer.txt",
            "kind": "note",
            "created_at": "2026-01-02T00:00:00Z",
            "manager_name": "Alpha",
        }
    ]


def test_load_managers_returns_empty_when_schema_is_not_initialized(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "empty-upload.db"
    sqlite3.connect(db_path).close()
    monkeypatch.setattr(upload, "connect_db", lambda: sqlite3.connect(db_path))

    assert upload._load_managers() == []
