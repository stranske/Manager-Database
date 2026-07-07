"""Utilities for extracting text from uploaded files."""

from __future__ import annotations

from stranske_pdf_extract.providers.text_baseline import TextBaselineProvider


def extract_text(file_bytes: bytes, filename: str) -> str:
    """Extract text from uploaded file based on file type.

    Supports: .txt, .md, .pdf
    Returns extracted text content.
    """
    ext = filename.rsplit(".", 1)[-1].lower()
    if ext == "pdf":
        return _extract_pdf(file_bytes)
    return file_bytes.decode("utf-8", errors="replace")


def _extract_pdf(file_bytes: bytes) -> str:
    """Extract text from PDF using the shared baseline provider."""

    try:
        output = TextBaselineProvider().extract_modalities("upload", file_bytes)
    except Exception as exc:
        raise ValueError("Failed to extract PDF text") from exc
    text = "\n\n".join(block.text for block in output.text_blocks if block.text)
    if not text or text.lstrip().startswith("%PDF-"):
        raise ValueError("Failed to extract PDF text")
    return text
