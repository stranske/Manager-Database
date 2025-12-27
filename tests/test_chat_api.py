import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.chat import chat
from embeddings import store_document


def test_chat_endpoint(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    store_document("hello world", str(db_path))
    # Call the handler directly to avoid ASGI threadpool issues in tests.
    payload = chat(q="hello")
    assert "hello world" in payload["answer"]
