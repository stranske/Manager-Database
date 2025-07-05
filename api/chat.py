"""Minimal FastAPI app providing a /chat endpoint."""

from __future__ import annotations

from fastapi import FastAPI, Query

from embeddings import search_documents

app = FastAPI()


@app.get("/chat")
def chat(q: str = Query(..., description="User question")):
    """Return a naive answer built from stored documents."""
    hits = search_documents(q)
    if not hits:
        answer = "No documents found."
    else:
        answer = "Context: " + " ".join(h["content"] for h in hits)
    return {"answer": answer}
