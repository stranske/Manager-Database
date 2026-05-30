"""Evidence objects for per-fact RAG source attribution."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


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

    def get(self, key: str, default: Any = None) -> Any:
        """Dictionary-style compatibility for existing source consumers."""
        return self.model_dump().get(key, default)
