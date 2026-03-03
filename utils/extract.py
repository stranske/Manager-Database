"""Utilities for extracting text from uploaded files."""

from __future__ import annotations

import io


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
    """Extract text from PDF using pdfplumber."""
    import pdfplumber

    text_parts: list[str] = []
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
    return "\n\n".join(text_parts)
