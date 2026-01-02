import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from langchain_analysis import (
    GITHUB_MODELS_ENDPOINT_ENV,
    GITHUB_MODELS_TOKEN_ENV,
    OPENAI_API_KEY_ENV,
    OPENAI_BASE_URL_ENV,
    PREFERRED_LLM_PROVIDER_ENV,
    detect_llm_provider,
    resolve_llm_provider,
)


@pytest.mark.parametrize(
    ("identifier", "expected"),
    [
        ("gpt-4o", "OpenAI"),
        ("azure-openai:gpt-4", "Azure OpenAI"),
        ("claude-3-opus", "Anthropic"),
        ("command-r", "Cohere"),
        ("gemini-1.5-pro", "Google"),
        ("mixtral-8x7b", "Mistral"),
        ("hf/tiiuae/falcon-7b", "Hugging Face"),
        ("mystery-model", "Unknown"),
    ],
)
def test_detect_llm_provider(identifier, expected):
    # Exercise common LangChain model identifiers across providers.
    assert detect_llm_provider(identifier) == expected


def test_resolve_llm_provider_prefers_env(monkeypatch):
    # Explicit preference should override identifier-based detection.
    monkeypatch.setenv(PREFERRED_LLM_PROVIDER_ENV, "openai")
    assert resolve_llm_provider("claude-3-opus") == "OpenAI"


def test_resolve_llm_provider_falls_back_to_detection(monkeypatch):
    # Without a preference, resolve uses the identifier heuristic.
    monkeypatch.delenv(PREFERRED_LLM_PROVIDER_ENV, raising=False)
    assert resolve_llm_provider("claude-3-opus") == "Anthropic"


def test_resolve_llm_provider_prefers_github_models(monkeypatch):
    # GitHub Models configuration should win when no explicit preference exists.
    monkeypatch.delenv(PREFERRED_LLM_PROVIDER_ENV, raising=False)
    monkeypatch.setenv(GITHUB_MODELS_ENDPOINT_ENV, "https://models.github.example")
    assert resolve_llm_provider("gpt-4o") == "GitHub Models"


def test_resolve_llm_provider_falls_back_to_openai(monkeypatch):
    # OpenAI is used when GitHub Models is not configured.
    monkeypatch.delenv(PREFERRED_LLM_PROVIDER_ENV, raising=False)
    monkeypatch.delenv(GITHUB_MODELS_ENDPOINT_ENV, raising=False)
    monkeypatch.delenv(GITHUB_MODELS_TOKEN_ENV, raising=False)
    monkeypatch.setenv(OPENAI_API_KEY_ENV, "test-key")
    assert resolve_llm_provider("gpt-4o") == "OpenAI"
