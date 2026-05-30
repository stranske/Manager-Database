"""Evidence objects for per-fact RAG source attribution."""

from __future__ import annotations

import math
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class Evidence(BaseModel):
    """Structured source evidence for a retrieved or database-backed fact."""

    model_config = ConfigDict(extra="allow")

    source_id: str
    source_type: str
    locator: dict[str, Any] = Field(default_factory=dict)
    excerpt: str
    method: str
    confidence: float = Field(ge=0.0, le=1.0)
    description: str = ""
    type: str | None = None
    document_id: str | int | None = None
    filing_id: int | None = None
    filing_url: str | None = None
    news_reference: str | None = None
    url: str | None = None

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
        return self

    def get(self, key: str, default: Any = None) -> Any:
        """Dictionary-style compatibility for existing source consumers."""
        return self.model_dump().get(key, default)
