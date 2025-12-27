import sys
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.chat import app
from embeddings import store_document


def test_chat_endpoint(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    store_document("hello world", str(db_path))
    client = TestClient(app)
    resp = client.get("/chat", params={"q": "hello"})
    assert resp.status_code == 200
    assert "hello world" in resp.json()["answer"]
