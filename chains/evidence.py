"""Evidence objects for per-fact RAG source attribution."""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class Evidence(BaseModel):
    """Structured source evidence for a retrieved or database-backed fact."""

    model_config = ConfigDict(extra="allow")

    schema_version: Literal["evidence-object/v1"] = "evidence-object/v1"
    evidence_id: str = Field(default="", min_length=1)
    fact_ref: str = Field(default="", min_length=1)
    source_id: str
    source_type: str
    locator: dict[str, Any] = Field(default_factory=dict)
    excerpt: str
    method: Literal[
        "rule", "parser", "table", "text", "ocr", "llm", "computed", "fallback", "manual"
    ]
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    description: str = ""
    type: str | None = None
    document_id: str | int | None = None
    campaign_id: int | None = None
    campaign_status: str | None = None
    filing_id: int | None = None
    filing_url: str | None = None
    news_reference: str | None = None
    url: str | None = None

    @field_validator("method", mode="before")
    @classmethod
    def _canonical_method(cls, value: Any) -> Any:
        """Accept legacy retrieval labels while emitting contract extraction methods."""
        aliases = {"db_lookup": "table", "vector_search": "text", "vector": "text"}
        return aliases.get(value, value) if isinstance(value, str) else value

    @field_validator("confidence")
    @classmethod
    def _validate_confidence(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("confidence must be finite")
        return value

    @model_validator(mode="after")
    def _populate_legacy_fields(self) -> Evidence:
        """Populate legacy source keys from locator when not explicitly provided."""
        if self.document_id is None and "doc_id" in self.locator:
            self.document_id = self.locator["doc_id"]
        if self.filing_id is None and "filing_id" in self.locator:
            try:
                self.filing_id = int(self.locator["filing_id"])
            except (TypeError, ValueError):
                self.filing_id = None
        if self.filing_url is None and isinstance(self.locator.get("filing_url"), str):
            self.filing_url = self.locator["filing_url"]
        if self.url is None and isinstance(self.locator.get("url"), str):
            self.url = self.locator["url"]
        # Without a field-specific reference, this evidence supports the retrieved
        # source excerpt. Confidence and presentation changes do not change identity.
        if not self.fact_ref:
            self.fact_ref = f"{self.source_id}:excerpt"
        if not self.evidence_id:
            identity = self.model_dump(
                mode="json",
                include={"source_id", "source_type", "locator", "excerpt", "method", "fact_ref"},
            )
            canonical = json.dumps(
                identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            )
            self.evidence_id = "evidence:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        return self

    def get(self, key: str, default: Any = None) -> Any:
        """Dictionary-style compatibility for existing source consumers."""
        return self.model_dump().get(key, default)
