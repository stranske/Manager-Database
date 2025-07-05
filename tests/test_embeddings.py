import sqlite3
import os
from pathlib import Path

import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from embeddings import store_document, search_documents


def test_store_and_search(tmp_path):
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    conn.close()
    os.environ["USE_SIMPLE_EMBED"] = "1"
    store_document("hello world", str(db_path))
    store_document("goodbye", str(db_path))
    results = search_documents("hello", str(db_path))
    assert results[0]["content"] == "hello world"
