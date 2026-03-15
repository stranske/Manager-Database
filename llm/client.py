"""Slot-based chat client builder built on top of the provider factory."""

from __future__ import annotations

import contextlib
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path

from llm.provider import _DEFAULT_MODEL_NAMES, LLMProviderConfig, create_llm

logger = logging.getLogger(__name__)

ENV_PROVIDER = "LANGCHAIN_PROVIDER"
ENV_MODEL = "LANGCHAIN_MODEL"
ENV_TIMEOUT = "LANGCHAIN_TIMEOUT"
ENV_MAX_RETRIES = "LANGCHAIN_MAX_RETRIES"
ENV_SLOT_CONFIG = "LANGCHAIN_SLOT_CONFIG"
ENV_SLOT_PREFIX = "LANGCHAIN_SLOT"

DEFAULT_TIMEOUT = 60
DEFAULT_MAX_RETRIES = 2
DEFAULT_SLOT_CONFIG_PATH = Path(__file__).resolve().parent / "config" / "llm_slots.json"
LEGACY_SLOT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "llm_slots.json"

_PROVIDER_ALIASES = {
    "openai": "openai",
    "anthropic": "anthropic",
    "claude": "anthropic",
    "azure_openai": "azure_openai",
    "azure-openai": "azure_openai",
}


@dataclass(frozen=True)
class ClientInfo:
    client: object
    provider: str
    model: str

    @property
    def provider_label(self) -> str:
        return f"{self.provider}/{self.model}"


@dataclass(frozen=True)
class SlotDefinition:
    name: str
    provider: str
    model: str


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning("Invalid %s value %r; using default %s", name, value, default)
        return default


def _normalize_provider(value: str | None) -> str | None:
    if not value:
        return None
    return _PROVIDER_ALIASES.get(value.strip().lower())


def _default_slots() -> list[SlotDefinition]:
    return [
        SlotDefinition(name="slot1", provider="openai", model="gpt-4o-mini"),
        SlotDefinition(name="slot2", provider="anthropic", model="claude-sonnet-4-20250514"),
    ]


def _slot_config_path() -> Path:
    configured = os.environ.get(ENV_SLOT_CONFIG)
    if configured:
        return Path(configured)
    if DEFAULT_SLOT_CONFIG_PATH.is_file():
        return DEFAULT_SLOT_CONFIG_PATH
    return LEGACY_SLOT_CONFIG_PATH


def _load_slot_config() -> list[SlotDefinition]:
    path = _slot_config_path()
    if not path.is_file():
        return _default_slots()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _default_slots()

    slots: list[SlotDefinition] = []
    for index, entry in enumerate(payload.get("slots", []), start=1):
        provider = _normalize_provider(str(entry.get("provider", "")))
        model = str(entry.get("model", "")).strip()
        if not provider or not model:
            continue
        name = str(entry.get("name") or f"slot{index}").strip() or f"slot{index}"
        slots.append(SlotDefinition(name=name, provider=provider, model=model))
    return slots or _default_slots()


def _apply_slot_env_overrides(slots: list[SlotDefinition]) -> list[SlotDefinition]:
    updated: list[SlotDefinition] = []
    for index, slot in enumerate(slots, start=1):
        provider_override = _normalize_provider(
            os.environ.get(f"{ENV_SLOT_PREFIX}{index}_PROVIDER")
        )
        model_override = os.environ.get(f"{ENV_SLOT_PREFIX}{index}_MODEL")
        updated.append(
            SlotDefinition(
                name=slot.name,
                provider=provider_override or slot.provider,
                model=(model_override or slot.model).strip(),
            )
        )
    return updated


def _resolve_slots() -> list[SlotDefinition]:
    return _apply_slot_env_overrides(_load_slot_config())


def _default_model_for_provider(provider: str) -> str:
    for slot in _resolve_slots():
        if slot.provider == provider:
            return slot.model
    return _DEFAULT_MODEL_NAMES[provider]


def _resolve_timeout(timeout: int | None) -> int:
    return _env_int(ENV_TIMEOUT, DEFAULT_TIMEOUT) if timeout is None else timeout


def _resolve_max_retries(max_retries: int | None) -> int:
    return _env_int(ENV_MAX_RETRIES, DEFAULT_MAX_RETRIES) if max_retries is None else max_retries


def _is_reasoning_model(model: str) -> bool:
    lowered = model.lower().strip()
    return lowered.startswith("o") and len(lowered) > 1 and lowered[1].isdigit()


def _client_kwargs(model: str, timeout: int, max_retries: int) -> dict[str, object]:
    kwargs: dict[str, object] = {"timeout": timeout, "max_retries": max_retries}
    if not _is_reasoning_model(model):
        kwargs["temperature"] = 0.1
    return kwargs


def _credentials_for(provider: str) -> dict[str, str] | None:
    if provider == "openai":
        api_key = os.environ.get("MANAGER_DB_OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY")
        return {"api_key": api_key} if api_key else None
    if provider == "anthropic":
        api_key = (
            os.environ.get("MANAGER_DB_ANTHROPIC_API_KEY")
            or os.environ.get("ANTHROPIC_API_KEY")
            or os.environ.get("CLAUDE_API_STRANSKE")
        )
        return {"api_key": api_key} if api_key else None
    if provider == "azure_openai":
        api_key = os.environ.get("MANAGER_DB_AZURE_OPENAI_API_KEY") or os.environ.get(
            "AZURE_OPENAI_API_KEY"
        )
        endpoint = os.environ.get("MANAGER_DB_AZURE_OPENAI_ENDPOINT") or os.environ.get(
            "AZURE_OPENAI_ENDPOINT"
        )
        api_version = os.environ.get("MANAGER_DB_AZURE_OPENAI_API_VERSION") or os.environ.get(
            "AZURE_OPENAI_API_VERSION"
        )
        if api_key and endpoint and api_version:
            return {
                "api_key": api_key,
                "azure_endpoint": endpoint,
                "api_version": api_version,
            }
    return None


def _build_for(provider: str, model: str, timeout: int, max_retries: int) -> ClientInfo | None:
    credentials = _credentials_for(provider)
    if credentials is None:
        return None
    config = LLMProviderConfig(
        provider_name=provider,
        credentials=credentials,
        model_name=model,
        client_kwargs=_client_kwargs(model, timeout, max_retries),
    )
    with contextlib.suppress(Exception):
        client = create_llm(config)
        return ClientInfo(client=client, provider=provider, model=model)
    return None


def build_chat_client(
    *,
    model: str | None = None,
    provider: str | None = None,
    timeout: int | None = None,
    max_retries: int | None = None,
) -> ClientInfo | None:
    selected_timeout = _resolve_timeout(timeout)
    selected_retries = _resolve_max_retries(max_retries)
    selected_provider = _normalize_provider(provider or os.environ.get(ENV_PROVIDER))

    if (provider or os.environ.get(ENV_PROVIDER)) and selected_provider is None:
        return None

    if selected_provider is not None:
        selected_model = (model or os.environ.get(ENV_MODEL) or "").strip()
        if not selected_model:
            selected_model = _default_model_for_provider(selected_provider)
        return _build_for(selected_provider, selected_model, selected_timeout, selected_retries)

    slots = _resolve_slots()
    for index, slot in enumerate(slots, start=1):
        slot_model = slot.model
        if index == 1 and (model or os.environ.get(ENV_MODEL)):
            slot_model = (model or os.environ.get(ENV_MODEL) or slot.model).strip()
        client_info = _build_for(slot.provider, slot_model, selected_timeout, selected_retries)
        if client_info is not None:
            return client_info
    return None
