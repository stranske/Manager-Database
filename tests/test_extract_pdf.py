from pathlib import Path

import pytest

from utils.extract import extract_text

FIXTURE_PDF = Path(__file__).resolve().parent / "fixtures" / "sample.pdf"


def test_extract_pdf_returns_text_from_native_pdf():
    text = extract_text(FIXTURE_PDF.read_bytes(), "sample.pdf")

    assert "Sample PDF fixture text for upload tests." in text


def test_extract_pdf_raises_on_empty_provider_result(monkeypatch):
    class EmptyProvider:
        def extract_modalities(self, _document_id, _file_bytes):
            class EmptyOutput:
                text_blocks = []

            return EmptyOutput()

    monkeypatch.setattr("utils.extract.TextBaselineProvider", EmptyProvider)

    with pytest.raises(ValueError, match="Failed to extract PDF text"):
        extract_text(FIXTURE_PDF.read_bytes(), "sample.pdf")


def test_extract_pdf_raises_on_whitespace_provider_result(monkeypatch):
    class WhitespaceProvider:
        def extract_modalities(self, _document_id, _file_bytes):
            class Block:
                text = "   \n\t"

            class WhitespaceOutput:
                text_blocks = [Block()]

            return WhitespaceOutput()

    monkeypatch.setattr("utils.extract.TextBaselineProvider", WhitespaceProvider)

    with pytest.raises(ValueError, match="Failed to extract PDF text"):
        extract_text(FIXTURE_PDF.read_bytes(), "sample.pdf")


def test_extract_pdf_wraps_malformed_provider_output(monkeypatch):
    class MalformedProvider:
        def extract_modalities(self, _document_id, _file_bytes):
            class MalformedOutput:
                pass

            return MalformedOutput()

    monkeypatch.setattr("utils.extract.TextBaselineProvider", MalformedProvider)

    with pytest.raises(ValueError, match="Failed to extract PDF text"):
        extract_text(FIXTURE_PDF.read_bytes(), "sample.pdf")
