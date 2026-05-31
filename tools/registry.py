"""Machine-readable tool permissions and data-zone registry."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

LlmBoundary = Literal["none", "external_authorized"]


class ToolDescriptor(BaseModel):
    """Declare the data and external-LLM boundary for one tool or chain."""

    name: str
    data_zone: str
    external_sources: list[str] = Field(default_factory=list)
    db_writes: list[str] = Field(default_factory=list)
    llm_boundary: LlmBoundary


TOOL_REGISTRY: dict[str, ToolDescriptor] = {
    "diff_holdings": ToolDescriptor(
        name="diff_holdings",
        data_zone="public_filings",
        external_sources=["EDGAR"],
        db_writes=[],
        llm_boundary="none",
    ),
    "daily_diff_flow": ToolDescriptor(
        name="daily_diff_flow",
        data_zone="public_filings",
        external_sources=["EDGAR"],
        db_writes=["daily_diffs"],
        llm_boundary="none",
    ),
    "RAGSearchChain": ToolDescriptor(
        name="RAGSearchChain",
        data_zone="internal_notes",
        external_sources=["uploaded_documents", "vector_index"],
        db_writes=[],
        llm_boundary="external_authorized",
    ),
    "FilingSummaryChain": ToolDescriptor(
        name="FilingSummaryChain",
        data_zone="public_filings",
        external_sources=["EDGAR"],
        db_writes=["api_usage"],
        llm_boundary="external_authorized",
    ),
    "NLQueryChain": ToolDescriptor(
        name="NLQueryChain",
        data_zone="internal_notes",
        external_sources=["database_schema"],
        db_writes=[],
        llm_boundary="external_authorized",
    ),
}


def descriptor_for(name: str) -> ToolDescriptor:
    """Return the registered descriptor for ``name``."""
    return TOOL_REGISTRY[name]


def run_contract_fields(name: str) -> dict[str, str]:
    """Return descriptor fields that belong in a ``RunResult`` envelope."""
    descriptor = descriptor_for(name)
    return {
        "data_zone": descriptor.data_zone,
        "llm_boundary": descriptor.llm_boundary,
    }
