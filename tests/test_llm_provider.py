from __future__ import annotations

import sys
import types

import pytest

from llm.provider import LLMProviderConfig, create_llm


class _Recorder:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


def test_create_llm_openai_with_valid_credentials(monkeypatch):
    fake_module = types.SimpleNamespace(ChatOpenAI=_Recorder, AzureChatOpenAI=_Recorder)
    monkeypatch.setitem(sys.modules, "langchain_openai", fake_module)

    client = create_llm(
        LLMProviderConfig(provider_name="openai", credentials={"api_key": "sk-test"})
    )

    assert isinstance(client, _Recorder)
    assert client.kwargs["model"] == "gpt-4o-mini"
    assert client.kwargs["api_key"].get_secret_value() == "sk-test"


def test_create_llm_anthropic_with_valid_credentials(monkeypatch):
    fake_module = types.SimpleNamespace(ChatAnthropic=_Recorder)
    monkeypatch.setitem(sys.modules, "langchain_anthropic", fake_module)

    client = create_llm(
        LLMProviderConfig(provider_name="anthropic", credentials={"api_key": "anthropic-test"})
    )

    assert isinstance(client, _Recorder)
    assert client.kwargs["model_name"] == "claude-sonnet-4-20250514"
    assert client.kwargs["api_key"].get_secret_value() == "anthropic-test"


def test_create_llm_missing_credentials_raises_value_error():
    with pytest.raises(ValueError, match="Missing credentials"):
        create_llm(LLMProviderConfig(provider_name="openai", credentials={}))


def test_create_llm_unsupported_provider_raises_value_error():
    with pytest.raises(ValueError, match="Unsupported provider"):
        create_llm(LLMProviderConfig(provider_name="bogus", credentials={"api_key": "x"}))


def test_create_llm_lazy_import_failure_is_explicit(monkeypatch):
    monkeypatch.delitem(sys.modules, "langchain_openai", raising=False)

    real_import = __import__

    def _fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "langchain_openai":
            raise ImportError("missing")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr("builtins.__import__", _fake_import)

    with pytest.raises(ImportError, match="langchain_openai"):
        create_llm(LLMProviderConfig(provider_name="openai", credentials={"api_key": "sk-test"}))
