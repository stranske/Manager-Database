from __future__ import annotations

import json

from llm import client as llm_client

# Tests that exercise the blocked-model guard must pin their own model registry.
# The guard filters a model only when the registry marks it ``blocked``; it does
# not maintain a hardcoded denylist of legacy model ids. Relying on the ambient
# config/model_registry.json is fragile — a catalog refresh that drops a legacy
# id (as the 2026-07-24 refresh dropped gpt-4o-mini) silently turns a "blocked"
# fixture into an "unknown, therefore allowed" one. Pin a hermetic registry so
# the guard is tested against a model that is unambiguously blocked.
ENV_MODEL_REGISTRY_CONFIG = "LANGCHAIN_MODEL_REGISTRY_CONFIG"


def _write_blocked_model_registry(tmp_path) -> str:
    registry_path = tmp_path / "model_registry.json"
    registry_path.write_text(
        json.dumps(
            {
                "models": [
                    {
                        "model_id": "gpt-4o-mini",
                        "provider": "openai",
                        "lifecycle": "deprecated",
                        "blocked": True,
                    },
                    {
                        "model_id": "gpt-5.4",
                        "provider": "openai",
                        "lifecycle": "current",
                    },
                ],
                "selections": [],
            }
        ),
        encoding="utf-8",
    )
    return str(registry_path)


def _write_lifecycle_registry(tmp_path) -> str:
    registry_path = tmp_path / "model_registry.json"
    registry_path.write_text(
        json.dumps(
            {
                "models": [
                    {
                        "model_id": "gpt-5.5",
                        "provider": "openai",
                        "lifecycle": "compatibility",
                    },
                    {
                        "model_id": "gpt-5.4",
                        "provider": "openai",
                        "lifecycle": "current",
                    },
                ],
                "selections": [],
            }
        ),
        encoding="utf-8",
    )
    return str(registry_path)


def _reviewed_model(provider: str) -> str:
    """Return the registry's reviewed ``verifier-balanced`` selection for a provider.

    Tests that exercise the *ambient* config must assert the resolver serves the
    reviewed selection, not a hardcoded model id. Pinning literals here meant a
    synced model-registry refresh (which advances the reviewed selection) turned
    into a CI failure with no underlying defect. Hermetic tests below that write
    their own registry/slot fixtures keep using explicit ids on purpose.
    """
    from tools.llm_registry import select_model_for_profile

    model = select_model_for_profile(provider=provider)
    assert model, f"no reviewed selection for {provider}; registry is misconfigured"
    return model


class _FakeClient:
    pass


def test_build_chat_client_returns_openai_when_key_available(monkeypatch):
    monkeypatch.setenv("MANAGER_DB_OPENAI_API_KEY", "openai-key")
    monkeypatch.delenv("MANAGER_DB_ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setattr(llm_client, "create_llm", lambda config: _FakeClient())

    client_info = llm_client.build_chat_client()

    assert client_info is not None
    assert client_info.provider == "openai"
    assert client_info.model == _reviewed_model("openai")


def test_build_chat_client_falls_back_to_anthropic(monkeypatch):
    monkeypatch.delenv("MANAGER_DB_OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("MANAGER_DB_ANTHROPIC_API_KEY", "anthropic-key")
    monkeypatch.setattr(llm_client, "create_llm", lambda config: _FakeClient())

    client_info = llm_client.build_chat_client()

    assert client_info is not None
    assert client_info.provider == "anthropic"
    assert client_info.model == _reviewed_model("anthropic")


def test_build_chat_client_returns_none_when_no_keys(monkeypatch):
    monkeypatch.delenv("MANAGER_DB_OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("MANAGER_DB_ANTHROPIC_API_KEY", raising=False)

    assert llm_client.build_chat_client() is None


def test_build_chat_client_honors_env_overrides(monkeypatch):
    monkeypatch.setenv("MANAGER_DB_OPENAI_API_KEY", "openai-key")
    monkeypatch.setenv("LANGCHAIN_PROVIDER", "openai")
    monkeypatch.setenv("LANGCHAIN_MODEL", "gpt-5.6-sol")
    captured = {}

    def _fake_create_llm(config):
        captured["config"] = config
        return _FakeClient()

    monkeypatch.setattr(llm_client, "create_llm", _fake_create_llm)

    client_info = llm_client.build_chat_client()

    assert client_info is not None
    assert client_info.model == "gpt-5.6-sol"
    assert captured["config"].client_kwargs["max_retries"] == llm_client.DEFAULT_MAX_RETRIES
    assert captured["config"].client_kwargs["temperature"] == 0.1


def test_explicit_provider_uses_its_matching_default_model(monkeypatch):
    registry = object()
    monkeypatch.setattr(
        llm_client,
        "_default_slots",
        lambda: [
            llm_client.SlotDefinition("openai", "openai", "gpt-5.4"),
            llm_client.SlotDefinition("anthropic", "anthropic", "claude-opus-4-6"),
        ],
    )
    monkeypatch.setattr(llm_client, "load_model_registry", lambda: registry)
    monkeypatch.setenv("MANAGER_DB_ANTHROPIC_API_KEY", "anthropic-key")
    monkeypatch.delenv("MANAGER_DB_OPENAI_API_KEY", raising=False)
    captured = {}

    def _eligible(provider, model, *, registry=None):
        captured["registry"] = registry
        return provider == "anthropic" and model == "claude-opus-4-6"

    monkeypatch.setattr(llm_client, "_is_model_eligible", _eligible)
    monkeypatch.setattr(llm_client, "create_llm", lambda config: _FakeClient())

    client_info = llm_client.build_chat_client(provider="anthropic")

    assert client_info is not None
    assert client_info.provider == "anthropic"
    assert client_info.model == "claude-opus-4-6"
    assert captured["registry"] is registry


def test_non_current_lifecycle_model_is_not_served(monkeypatch, tmp_path):
    config_path = tmp_path / "llm_slots.json"
    config_path.write_text(
        json.dumps(
            {
                "slots": [
                    {"name": "slot1", "provider": "openai", "model": "gpt-5.5"},
                ]
            }
        )
    )
    monkeypatch.setenv("LANGCHAIN_SLOT_CONFIG", str(config_path))
    monkeypatch.setenv(ENV_MODEL_REGISTRY_CONFIG, _write_lifecycle_registry(tmp_path))
    monkeypatch.setenv("MANAGER_DB_OPENAI_API_KEY", "openai-key")

    def _fake_create_llm(config):
        if config.model_name == "gpt-5.5":
            raise AssertionError("non-current model reached create_llm")
        return _FakeClient()

    monkeypatch.setattr(llm_client, "create_llm", _fake_create_llm)

    client_info = llm_client.build_chat_client()

    assert client_info is None


def test_blocked_slot_model_is_not_selected(monkeypatch, tmp_path):
    config_path = tmp_path / "llm_slots.json"
    config_path.write_text(
        json.dumps(
            {
                "slots": [
                    {"name": "blocked", "provider": "openai", "model": "gpt-4o-mini"},
                    {"name": "approved", "provider": "openai", "model": "gpt-5.4"},
                ]
            }
        )
    )
    monkeypatch.setenv("LANGCHAIN_SLOT_CONFIG", str(config_path))
    monkeypatch.setenv(ENV_MODEL_REGISTRY_CONFIG, _write_blocked_model_registry(tmp_path))
    monkeypatch.setenv("MANAGER_DB_OPENAI_API_KEY", "openai-key")
    monkeypatch.delenv("MANAGER_DB_ANTHROPIC_API_KEY", raising=False)
    captured = []

    def _fake_create_llm(config):
        captured.append(config.model_name)
        if config.model_name == "gpt-4o-mini":
            raise AssertionError("blocked model reached create_llm")
        return _FakeClient()

    monkeypatch.setattr(llm_client, "create_llm", _fake_create_llm)

    client_info = llm_client.build_chat_client()

    assert client_info is not None
    assert client_info.provider == "openai"
    assert client_info.model == "gpt-5.4"
    assert captured == ["gpt-5.4"]


def test_blocked_slot_env_model_falls_back_to_slot_model(monkeypatch, tmp_path):
    config_path = tmp_path / "llm_slots.json"
    config_path.write_text(
        json.dumps(
            {
                "slots": [
                    {"name": "approved", "provider": "openai", "model": "gpt-5.4"},
                ]
            }
        )
    )
    monkeypatch.setenv("LANGCHAIN_SLOT_CONFIG", str(config_path))
    monkeypatch.setenv("LANGCHAIN_SLOT1_MODEL", "gpt-4o-mini")
    monkeypatch.setenv(ENV_MODEL_REGISTRY_CONFIG, _write_blocked_model_registry(tmp_path))
    monkeypatch.setenv("MANAGER_DB_OPENAI_API_KEY", "openai-key")
    monkeypatch.delenv("MANAGER_DB_ANTHROPIC_API_KEY", raising=False)
    captured = []

    def _fake_create_llm(config):
        captured.append(config.model_name)
        if config.model_name == "gpt-4o-mini":
            raise AssertionError("blocked override reached create_llm")
        return _FakeClient()

    monkeypatch.setattr(llm_client, "create_llm", _fake_create_llm)

    client_info = llm_client.build_chat_client()

    assert client_info is not None
    assert client_info.provider == "openai"
    assert client_info.model == "gpt-5.4"
    assert captured == ["gpt-5.4"]


def test_invalid_slot_config_slots_shape_fails_closed(monkeypatch, tmp_path):
    config_path = tmp_path / "llm_slots.json"
    config_path.write_text(json.dumps({"slots": {"name": "slot1"}}))
    monkeypatch.setenv("LANGCHAIN_SLOT_CONFIG", str(config_path))
    monkeypatch.setenv("MANAGER_DB_OPENAI_API_KEY", "openai-key")
    monkeypatch.delenv("MANAGER_DB_ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setattr(llm_client, "create_llm", lambda config: _FakeClient())

    client_info = llm_client.build_chat_client()

    assert client_info is None


def test_invalid_slot_config_entry_fails_closed(monkeypatch, tmp_path):
    config_path = tmp_path / "llm_slots.json"
    config_path.write_text(json.dumps({"slots": [None]}))
    monkeypatch.setenv("LANGCHAIN_SLOT_CONFIG", str(config_path))
    monkeypatch.setenv("MANAGER_DB_OPENAI_API_KEY", "openai-key")
    monkeypatch.delenv("MANAGER_DB_ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setattr(llm_client, "create_llm", lambda config: _FakeClient())

    client_info = llm_client.build_chat_client()

    assert client_info is None


def test_blank_slot_env_model_falls_back_to_slot_model(monkeypatch, tmp_path):
    config_path = tmp_path / "llm_slots.json"
    config_path.write_text(
        json.dumps(
            {
                "slots": [
                    {"name": "approved", "provider": "openai", "model": "gpt-5.4"},
                ]
            }
        )
    )
    monkeypatch.setenv("LANGCHAIN_SLOT_CONFIG", str(config_path))
    monkeypatch.setenv("LANGCHAIN_SLOT1_MODEL", "   ")
    monkeypatch.setenv("MANAGER_DB_OPENAI_API_KEY", "openai-key")
    monkeypatch.delenv("MANAGER_DB_ANTHROPIC_API_KEY", raising=False)
    captured = []

    def _fake_create_llm(config):
        captured.append(config.model_name)
        return _FakeClient()

    monkeypatch.setattr(llm_client, "create_llm", _fake_create_llm)

    client_info = llm_client.build_chat_client()

    assert client_info is not None
    assert client_info.provider == "openai"
    assert client_info.model == "gpt-5.4"
    assert captured == ["gpt-5.4"]


def test_profile_slot_resolves_reviewed_selection(monkeypatch, tmp_path):
    """A profile-driven slot file must resolve via the reviewed selection.

    Workflows now ships config/llm_slots.json in profile form (no model pins).
    Before profile support existed here, such a file resolved to zero slots and
    the chat path served no model at all.
    """
    registry_path = tmp_path / "model_registry.json"
    registry_path.write_text(
        json.dumps(
            {
                "models": [
                    {
                        "model_id": "gpt-reviewed",
                        "provider": "openai",
                        "lifecycle": "current",
                    }
                ],
                "selections": [
                    {
                        "profile": "verifier-balanced",
                        "provider": "openai",
                        "model_id": "gpt-reviewed",
                        "status": "provisional",
                        "evidence_ids": ["catalog-review"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "llm_slots.json"
    config_path.write_text(
        json.dumps(
            {"slots": [{"name": "slot1", "provider": "openai", "profile": "verifier-balanced"}]}
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("LANGCHAIN_SLOT_CONFIG", str(config_path))
    monkeypatch.setenv(ENV_MODEL_REGISTRY_CONFIG, str(registry_path))
    monkeypatch.setenv("MANAGER_DB_OPENAI_API_KEY", "openai-key")
    monkeypatch.delenv("MANAGER_DB_ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setattr(llm_client, "create_llm", lambda config: _FakeClient())

    client_info = llm_client.build_chat_client()

    assert client_info is not None
    assert client_info.provider == "openai"
    assert client_info.model == "gpt-reviewed"


def test_profile_slot_with_unresolvable_profile_fails_closed(monkeypatch, tmp_path):
    """A profile with no reviewed selection must not fall back to a default model."""
    registry_path = tmp_path / "model_registry.json"
    registry_path.write_text(
        json.dumps(
            {
                "models": [{"model_id": "gpt-5.4", "provider": "openai", "lifecycle": "current"}],
                "selections": [],
            }
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "llm_slots.json"
    config_path.write_text(
        json.dumps(
            {"slots": [{"name": "slot1", "provider": "openai", "profile": "verifier-balanced"}]}
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("LANGCHAIN_SLOT_CONFIG", str(config_path))
    monkeypatch.setenv(ENV_MODEL_REGISTRY_CONFIG, str(registry_path))
    monkeypatch.setenv("MANAGER_DB_OPENAI_API_KEY", "openai-key")
    monkeypatch.delenv("MANAGER_DB_ANTHROPIC_API_KEY", raising=False)

    def _fail(config):
        raise AssertionError(f"unresolved profile slot reached create_llm: {config.model_name}")

    monkeypatch.setattr(llm_client, "create_llm", _fail)

    assert llm_client.build_chat_client() is None
