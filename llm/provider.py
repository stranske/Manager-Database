"""Lazy LLM provider factory for LangChain chat models."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from pydantic import SecretStr

_REQUIRED_CREDENTIAL_KEYS: dict[str, tuple[str, ...]] = {
    "openai": ("api_key",),
    "anthropic": ("api_key",),
    "azure_openai": ("api_key", "azure_endpoint", "api_version"),
}

_DEFAULT_MODEL_NAMES: dict[str, str] = {
    "openai": "gpt-4o-mini",
    "anthropic": "claude-sonnet-4-20250514",
    "azure_openai": "gpt-4o-mini",
}


@dataclass(frozen=True)
class LLMProviderConfig:
    provider_name: str
    credentials: Mapping[str, str]
    model_name: str | None = None
    client_kwargs: Mapping[str, Any] = field(default_factory=dict)


def _validate_credentials(config: LLMProviderConfig) -> None:
    provider_name = config.provider_name.strip().lower()
    required = _REQUIRED_CREDENTIAL_KEYS.get(provider_name)
    if required is None:
        raise ValueError(f"Unsupported provider: {config.provider_name}")

    missing = [key for key in required if not str(config.credentials.get(key, "")).strip()]
    if missing:
        missing_keys = ", ".join(missing)
        raise ValueError(f"Missing credentials for {provider_name}: {missing_keys}")


def create_llm(config: LLMProviderConfig) -> Any:
    """Create a LangChain chat client from provider config."""
    provider_name = config.provider_name.strip().lower()
    _validate_credentials(config)

    model_name = config.model_name or _DEFAULT_MODEL_NAMES[provider_name]
    api_key = SecretStr(str(config.credentials["api_key"]))
    client_kwargs = dict(config.client_kwargs)

    if provider_name == "openai":
        try:
            from langchain_openai import ChatOpenAI
        except ImportError as exc:  # pragma: no cover - lazy import guard
            raise ImportError("langchain_openai is required for openai provider") from exc
        return ChatOpenAI(model=model_name, api_key=api_key, **client_kwargs)

    if provider_name == "azure_openai":
        try:
            from langchain_openai import AzureChatOpenAI
        except ImportError as exc:  # pragma: no cover - lazy import guard
            raise ImportError("langchain_openai is required for azure_openai provider") from exc
        return AzureChatOpenAI(
            model=model_name,
            api_key=api_key,
            azure_endpoint=str(config.credentials["azure_endpoint"]),
            api_version=str(config.credentials["api_version"]),
            **client_kwargs,
        )

    try:
        from langchain_anthropic import ChatAnthropic
    except ImportError as exc:  # pragma: no cover - lazy import guard
        raise ImportError("langchain_anthropic is required for anthropic provider") from exc
    return ChatAnthropic(model_name=model_name, api_key=api_key, **client_kwargs)
