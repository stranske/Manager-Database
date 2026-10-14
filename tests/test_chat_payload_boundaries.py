"""Chain responses preserve answer and citation metadata at the API boundary."""

from types import SimpleNamespace

import pytest

from api import chat


def test_chain_string_answer_keeps_empty_metadata():
    result = chat._extract_chain_payload("plain answer")
    assert result == ("plain answer", [], None, None)


def test_chain_source_list_preserves_dicts_and_describes_non_dicts():
    source = {"document_id": "filing-17", "page": 2}
    result = chat._normalize_sources([source, "plain citation", 17, None])
    assert result == [
        source,
        {"description": "plain citation"},
        {"description": "17"},
        {"description": "None"},
    ]
    assert result[0] is source


@pytest.mark.parametrize(
    "sources", [None, "citation", 17, ("citation",)], ids=["none", "text", "number", "tuple"]
)
def test_chain_unsupported_source_container_stays_empty(sources):
    result = chat._normalize_sources(sources)
    assert result == []


def test_chain_object_response_fallback_preserves_all_metadata():
    source = {"document_id": "filing-17"}
    payload = SimpleNamespace(
        answer="",
        response="fallback answer",
        sources=[source, "note"],
        sql="SELECT 1",
        trace_url="https://trace.invalid/run/17",
    )
    result = chat._extract_chain_payload(payload)
    assert result == (
        "fallback answer",
        [source, {"description": "note"}],
        "SELECT 1",
        "https://trace.invalid/run/17",
    )


def test_chain_object_without_optional_attributes_has_empty_metadata():
    result = chat._extract_chain_payload(SimpleNamespace())
    assert result == ("", [], None, None)
