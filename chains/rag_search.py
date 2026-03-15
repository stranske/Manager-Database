"""Dual-retrieval RAG chain for research questions."""

from __future__ import annotations

import datetime as dt
import re
from typing import Any

from pydantic import BaseModel, Field

from adapters.base import connect_db
from embeddings import search_documents
from llm.injection import guard_input
from tools.llm_provider import (
    build_langsmith_metadata,
    derive_langsmith_trace_url,
    extract_trace_id,
)

RAG_SYSTEM_PROMPT = """You are a research assistant for an investment manager monitoring platform.
Answer the user's question using ONLY the provided context.

Context includes:
- Document excerpts (from uploaded research notes, memos, PDFs)
- Structured data (from the manager database: holdings, filings, news)

IMPORTANT:
- Cite your sources: reference document IDs, filing dates, or news headlines.
- If the context does not contain enough information, say so explicitly.
- Do not fabricate data or speculate beyond what is provided.
- Be concise but thorough.
"""

_CUSIP_RE = re.compile(r"\b[0-9A-Z]{8,9}\b")
_DATE_RE = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")


class RAGSearchResult(BaseModel):
    """Structured output for the RAG search chain."""

    answer: str
    sources: list[dict[str, Any]] = Field(default_factory=list)
    confidence: str = Field(default="low")
    trace_url: str | None = None


class RAGSearchChain:
    """Combine vector search with structured database lookups for research answers."""

    def __init__(self, llm: Any | None = None, db_conn: Any | None = None) -> None:
        self.llm = llm
        self.db = db_conn

    def _acquire_connection(self):
        if self.db is not None:
            return self.db, False
        return connect_db(), True

    def _manager_catalog(self) -> list[dict[str, Any]]:
        conn, should_close = self._acquire_connection()
        try:
            cursor = conn.execute("SELECT manager_id, name, cik FROM managers")
            rows = cursor.fetchall()
            return [
                {
                    "manager_id": int(row[0]),
                    "name": str(row[1]),
                    "cik": str(row[2]) if row[2] is not None else None,
                }
                for row in rows
            ]
        except Exception:
            return []
        finally:
            if should_close:
                conn.close()

    def _vector_search(
        self, query: str, k: int = 5, manager_id: int | None = None
    ) -> list[dict[str, Any]]:
        """Fetch the most relevant indexed documents for the question."""
        return search_documents(query, k=k, manager_id=manager_id)

    def _entity_extraction(self, query: str) -> dict[str, Any]:
        """Extract manager, CUSIP, date, and keyword hints without another LLM call."""
        lowered = query.lower()
        manager_ids: list[int] = []
        manager_names: list[str] = []
        for manager in self._manager_catalog():
            name = manager["name"]
            cik = manager.get("cik")
            if name.lower() in lowered or (cik and cik in query):
                manager_ids.append(int(manager["manager_id"]))
                manager_names.append(name)

        dates = _DATE_RE.findall(query)
        date_range: tuple[str, str] | None = None
        if dates:
            date_range = (dates[0], dates[-1])
        elif "yesterday" in lowered:
            yesterday = dt.date.today() - dt.timedelta(days=1)
            date_range = (yesterday.isoformat(), yesterday.isoformat())

        keywords = [
            token for token in re.findall(r"[A-Za-z][A-Za-z0-9_-]+", query) if len(token) >= 4
        ]
        return {
            "manager_ids": manager_ids,
            "manager_names": manager_names,
            "cusips": sorted({match.upper() for match in _CUSIP_RE.findall(query)}),
            "date_range": date_range,
            "keywords": keywords,
        }

    def _structured_search(self, entities: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
        """Build compact structured context from holdings, filings, news, and activism tables."""
        conn, should_close = self._acquire_connection()
        context_sections: list[str] = []
        sources: list[dict[str, Any]] = []
        manager_ids = [int(manager_id) for manager_id in entities.get("manager_ids", [])]
        cusips = [str(cusip) for cusip in entities.get("cusips", [])]
        try:
            if manager_ids:
                placeholders = ",".join("?" for _ in manager_ids)
                manager_rows = conn.execute(
                    f"SELECT manager_id, name, cik FROM managers WHERE manager_id IN ({placeholders})",
                    manager_ids,
                ).fetchall()
                if manager_rows:
                    lines = [
                        f"Manager {row[1]} (manager_id={row[0]}, cik={row[2] or 'n/a'})"
                        for row in manager_rows
                    ]
                    context_sections.append("Managers:\n" + "\n".join(lines))

                filing_rows = conn.execute(
                    f"SELECT filing_id, type, filed_date, url FROM filings WHERE manager_id IN ({placeholders}) "
                    "ORDER BY filed_date DESC LIMIT 5",
                    manager_ids,
                ).fetchall()
                if filing_rows:
                    lines = [f"Filing {row[0]}: {row[1]} filed {row[2]}" for row in filing_rows]
                    context_sections.append("Recent filings:\n" + "\n".join(lines))
                    for filing_id, filing_type, filed_date, url in filing_rows:
                        sources.append(
                            {
                                "type": "filing",
                                "filing_id": int(filing_id),
                                "description": f"{filing_type} filed {filed_date}",
                                "filing_url": url,
                            }
                        )

                holding_rows = conn.execute(
                    f"SELECT h.cusip, h.name_of_issuer, h.shares, h.value_usd, f.manager_id "
                    "FROM holdings h JOIN filings f ON f.filing_id = h.filing_id "
                    f"WHERE f.manager_id IN ({placeholders}) ORDER BY f.filed_date DESC LIMIT 8",
                    manager_ids,
                ).fetchall()
                if holding_rows:
                    lines = [
                        f"Holding {row[1]} ({row[0] or 'n/a'}): shares={row[2]}, value_usd={row[3]}"
                        for row in holding_rows
                    ]
                    context_sections.append("Latest holdings:\n" + "\n".join(lines))

                news_rows = conn.execute(
                    f"SELECT headline, published_at, url FROM news_items WHERE manager_id IN ({placeholders}) "
                    "ORDER BY published_at DESC LIMIT 5",
                    manager_ids,
                ).fetchall()
                if news_rows:
                    lines = [f"{row[1]}: {row[0]}" for row in news_rows]
                    context_sections.append("Recent news:\n" + "\n".join(lines))
                    for headline, published_at, url in news_rows:
                        sources.append(
                            {
                                "type": "news",
                                "news_reference": headline,
                                "description": f"Published {published_at}",
                                "url": url,
                            }
                        )

                activism_rows = conn.execute(
                    f"SELECT subject_company, filing_type, filed_date, url FROM activism_filings "
                    f"WHERE manager_id IN ({placeholders}) ORDER BY filed_date DESC LIMIT 5",
                    manager_ids,
                ).fetchall()
                if activism_rows:
                    lines = [f"{row[1]} on {row[0]} filed {row[2]}" for row in activism_rows]
                    context_sections.append("Activism filings:\n" + "\n".join(lines))
                    for subject_company, filing_type, filed_date, url in activism_rows:
                        sources.append(
                            {
                                "type": "activism_filing",
                                "description": f"{filing_type} for {subject_company} filed {filed_date}",
                                "filing_url": url,
                            }
                        )

            if cusips:
                placeholders = ",".join("?" for _ in cusips)
                cusip_rows = conn.execute(
                    f"SELECT cusip, name_of_issuer, manager_count, total_value_usd, report_date "
                    f"FROM crowded_trades WHERE cusip IN ({placeholders}) ORDER BY report_date DESC LIMIT 5",
                    cusips,
                ).fetchall()
                if cusip_rows:
                    lines = [
                        f"Crowded trade {row[1]} ({row[0]}): managers={row[2]}, total_value_usd={row[3]}, report_date={row[4]}"
                        for row in cusip_rows
                    ]
                    context_sections.append("Crowded trades:\n" + "\n".join(lines))

            if not context_sections:
                context_sections.append("Structured data: no directly matching rows found.")
            return "\n\n".join(context_sections), sources
        finally:
            if should_close:
                conn.close()

    def _document_context(
        self, documents: list[dict[str, Any]]
    ) -> tuple[str, list[dict[str, Any]]]:
        lines: list[str] = []
        sources: list[dict[str, Any]] = []
        for document in documents:
            snippet = str(document.get("content", "")).strip().replace("\n", " ")[:400]
            lines.append(
                f"doc {document.get('doc_id')}: {document.get('filename') or document.get('kind') or 'document'} | {snippet}"
            )
            sources.append(
                {
                    "type": "document",
                    "document_id": document.get("doc_id"),
                    "description": document.get("filename") or document.get("kind") or "document",
                }
            )
        return "\n".join(lines) if lines else "No relevant document excerpts found.", sources

    def _confidence(
        self, documents: list[dict[str, Any]], structured_sources: list[dict[str, Any]]
    ) -> str:
        if documents and structured_sources:
            return "high"
        if documents or structured_sources:
            return "medium"
        return "low"

    def _invoke_llm(self, prompt: str) -> tuple[str, str | None]:
        if self.llm is None:
            return (
                "I do not have enough configured LLM context to answer beyond the retrieved sources.",
                None,
            )

        config = build_langsmith_metadata(operation="rag-search")
        response = None
        if hasattr(self.llm, "invoke"):
            try:
                response = self.llm.invoke(prompt, config=config)
            except TypeError:
                response = self.llm.invoke(prompt)
        elif callable(self.llm):
            response = self.llm(prompt)
        else:
            raise TypeError("Configured LLM does not support invoke()")

        trace_url = derive_langsmith_trace_url(extract_trace_id(response))
        if isinstance(response, str):
            return response, trace_url
        if hasattr(response, "content"):
            return str(response.content), trace_url
        return str(response), trace_url

    def run(self, question: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
        """Run vector retrieval and structured retrieval, then answer with source attribution."""
        del context  # Reserved for future API-provided filters.
        guard_input(question)
        entities = self._entity_extraction(question)
        manager_filter = entities["manager_ids"][0] if len(entities["manager_ids"]) == 1 else None
        documents = self._vector_search(question, k=5, manager_id=manager_filter)
        structured_context, structured_sources = self._structured_search(entities)
        document_context, document_sources = self._document_context(documents)
        all_sources = document_sources + structured_sources
        confidence = self._confidence(documents, structured_sources)

        if not documents and not structured_sources:
            return RAGSearchResult(
                answer="I do not have enough context in the indexed documents or database records to answer that question.",
                sources=[],
                confidence="low",
                trace_url=None,
            ).model_dump()

        prompt = (
            f"{RAG_SYSTEM_PROMPT}\nQuestion: {question}\n\n"
            f"--- Document excerpts (vector search results) ---\n{document_context}\n\n"
            f"--- Structured data (database query results) ---\n{structured_context}\n"
        )
        answer, trace_url = self._invoke_llm(prompt)
        result = RAGSearchResult(
            answer=answer,
            sources=all_sources,
            confidence=confidence,
            trace_url=trace_url,
        )
        return result.model_dump()
