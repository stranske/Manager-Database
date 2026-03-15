from __future__ import annotations

import json

from llm import client as llm_client


class _FakeClient:
    pass


def test_build_chat_client_returns_openai_when_key_available(monkeypatch):
    monkeypatch.setenv("MANAGER_DB_OPENAI_API_KEY", "openai-key")
    monkeypatch.delenv("MANAGER_DB_ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setattr(llm_client, "create_llm", lambda config: _FakeClient())

    client_info = llm_client.build_chat_client()

    assert client_info is not None
    assert client_info.provider == "openai"
    assert client_info.model == "gpt-4o-mini"


def test_build_chat_client_falls_back_to_anthropic(monkeypatch):
    monkeypatch.delenv("MANAGER_DB_OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("MANAGER_DB_ANTHROPIC_API_KEY", "anthropic-key")
    monkeypatch.setattr(llm_client, "create_llm", lambda config: _FakeClient())

    client_info = llm_client.build_chat_client()

    assert client_info is not None
    assert client_info.provider == "anthropic"
    assert client_info.model == "claude-sonnet-4-20250514"


def test_build_chat_client_returns_none_when_no_keys(monkeypatch):
    monkeypatch.delenv("MANAGER_DB_OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("MANAGER_DB_ANTHROPIC_API_KEY", raising=False)

    assert llm_client.build_chat_client() is None


def test_build_chat_client_honors_env_overrides(monkeypatch):
    monkeypatch.setenv("MANAGER_DB_OPENAI_API_KEY", "openai-key")
    monkeypatch.setenv("LANGCHAIN_PROVIDER", "openai")
    monkeypatch.setenv("LANGCHAIN_MODEL", "o3-mini")
    captured = {}

    def _fake_create_llm(config):
        captured["config"] = config
        return _FakeClient()

    monkeypatch.setattr(llm_client, "create_llm", _fake_create_llm)

    client_info = llm_client.build_chat_client()

    assert client_info is not None
    assert client_info.model == "o3-mini"
    assert captured["config"].client_kwargs["max_retries"] == llm_client.DEFAULT_MAX_RETRIES
    assert "temperature" not in captured["config"].client_kwargs


def test_slot_config_loading_from_json_file(monkeypatch, tmp_path):
    config_path = tmp_path / "llm_slots.json"
    config_path.write_text(
        json.dumps(
            {
                "slots": [
                    {"name": "slot1", "provider": "anthropic", "model": "claude-test"},
                    {"name": "slot2", "provider": "openai", "model": "gpt-test"},
                ]
            }
        )
    )
    monkeypatch.setenv("LANGCHAIN_SLOT_CONFIG", str(config_path))
    monkeypatch.setenv("MANAGER_DB_ANTHROPIC_API_KEY", "anthropic-key")
    monkeypatch.delenv("MANAGER_DB_OPENAI_API_KEY", raising=False)
    monkeypatch.setattr(llm_client, "create_llm", lambda config: _FakeClient())

    client_info = llm_client.build_chat_client()

    assert client_info is not None
    assert client_info.provider == "anthropic"
    assert client_info.model == "claude-test"
