from __future__ import annotations

import logging
import os
import sqlite3

import pandas as pd
import streamlit as st

from adapters.base import connect_db
from embeddings import store_document
from utils.extract import extract_text

from . import require_login

logger = logging.getLogger(__name__)


def save_note(content: str, filename: str) -> int:
    """Backward-compatible helper for tests and legacy callers."""
    return store_document(content, kind=_kind_for_filename(filename), filename=filename)


def _get_max_upload_bytes() -> int:
    raw = os.getenv("MAX_UPLOAD_BYTES", str(10 * 1024 * 1024)).strip()
    try:
        value = int(raw)
    except ValueError:
        return 10 * 1024 * 1024
    return value if value > 0 else 10 * 1024 * 1024


def _load_managers() -> list[tuple[int, str]]:
    conn = connect_db()
    try:
        if isinstance(conn, sqlite3.Connection):
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='managers'"
            ).fetchall()
            if not tables:
                return []
        rows = conn.execute("SELECT id, name FROM managers ORDER BY name ASC").fetchall()
        return [(int(row[0]), str(row[1])) for row in rows]
    except Exception:
        return []
    finally:
        conn.close()


def _kind_for_filename(filename: str) -> str:
    ext = filename.rsplit(".", 1)[-1].lower()
    if ext == "md":
        return "memo"
    if ext == "pdf":
        return "pdf"
    return "note"


def _recent_uploads(limit: int = 10) -> pd.DataFrame:
    conn = connect_db()
    try:
        query = (
            "SELECT d.doc_id, d.filename, d.kind, d.created_at, m.name AS manager_name "
            "FROM documents d "
            "LEFT JOIN managers m ON m.id = d.manager_id "
            "ORDER BY d.created_at DESC "
            "LIMIT ?"
        )
        if isinstance(conn, sqlite3.Connection):
            return pd.read_sql_query(query, conn, params=(limit,))
        query = query.replace("?", "%s")
        return pd.read_sql_query(query, conn, params=(limit,))
    except Exception:
        return pd.DataFrame(columns=["doc_id", "filename", "kind", "created_at", "manager_name"])
    finally:
        conn.close()


def main() -> None:
    if not require_login():
        st.stop()
    st.header("Upload Document")
    managers = _load_managers()
    manager_labels = ["None"] + [f"{name} (id={mid})" for mid, name in managers]
    selected_label = st.selectbox("Optional manager link", options=manager_labels)
    selected_manager = None
    if selected_label != "None":
        selected_index = manager_labels.index(selected_label) - 1
        selected_manager = managers[selected_index][0]

    uploaded = st.file_uploader("Upload", type=["txt", "md", "pdf"])
    max_upload_bytes = _get_max_upload_bytes()

    if uploaded:
        file_bytes = uploaded.getvalue()
        if len(file_bytes) > max_upload_bytes:
            st.error(f"File exceeds maximum size of {max_upload_bytes} bytes.")
        else:
            try:
                text = extract_text(file_bytes, uploaded.name)
            except Exception as exc:
                logger.exception("Failed to extract uploaded file %s", uploaded.name, exc_info=exc)
                st.error(f"Could not extract text from {uploaded.name}.")
            else:
                preview = text[:500]
                st.subheader("Extraction Preview")
                st.text_area("First 500 chars", preview, height=200)
                if st.button("Store Document"):
                    kind = _kind_for_filename(uploaded.name)
                    doc_id = store_document(
                        text,
                        manager_id=selected_manager,
                        kind=kind,
                        filename=uploaded.name,
                    )
                    st.success(f"Uploaded document ID: {doc_id}")

    history = _recent_uploads()
    if not history.empty:
        st.subheader("Recent Uploads")
        st.dataframe(history, use_container_width=True)


if __name__ == "__main__":
    main()
