"""
Embedding provider registry and deterministic fallback implementation.

This module provides a lightweight embedding interface that can operate without
external credentials. The fallback provider supplies deterministic embeddings so
tests and local workflows remain stable when no external providers are
configured.
"""

from __future__ import annotations

import hashlib
import math
import re
from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True)
class EmbeddingResponse:
    vectors: list[list[float]]
    model: str
    dimensions: int


@dataclass(frozen=True)
class EmbeddingSelectionCriteria:
    model: str | None = None
    preferred_provider: str | None = None
    provider_allowlist: set[str] | None = None
    provider_denylist: set[str] | None = None
    prefer_low_cost: bool = False
    prefer_low_latency: bool = False


@dataclass(frozen=True)
class EmbeddingProviderSelection:
    provider: EmbeddingProvider
    model: str


class EmbeddingProvider(ABC):
    provider_id = "base"
    default_model = "text-embedding-3-small"
    cost_score = 1.0
    latency_score = 1.0

    @abstractmethod
    def is_available(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def embed(self, texts: list[str], *, model: str | None = None) -> EmbeddingResponse:
        raise NotImplementedError

    def is_fallback(self) -> bool:
        return False


class DeterministicEmbeddingProvider(EmbeddingProvider):
    provider_id = "deterministic"
    default_model = "deterministic-embedding-v1"
    cost_score = 0.0
    latency_score = 0.0
    _dimensions = 16

    def is_available(self) -> bool:
        return True

    def is_fallback(self) -> bool:
        return True

    def embed(self, texts: list[str], *, model: str | None = None) -> EmbeddingResponse:
        vectors = [self._embed_text(text or "") for text in texts]
        return EmbeddingResponse(
            vectors=vectors,
            model=model or self.default_model,
            dimensions=self._dimensions,
        )

    def _embed_text(self, text: str) -> list[float]:
        tokens = re.findall(r"[a-z0-9]+", text.lower())
        if not tokens:
            tokens = [text.lower()]
        vector = [0.0] * self._dimensions
        for token in tokens:
            digest = hashlib.sha256(token.encode("utf-8")).digest()
            for idx in range(self._dimensions):
                vector[idx] += (digest[idx] / 255.0) - 0.5
        norm = math.sqrt(sum(value * value for value in vector))
        if norm <= 0:
            return vector
        return [value / norm for value in vector]


class EmbeddingProviderRegistry:
    def __init__(self, providers: Iterable[EmbeddingProvider] | None = None) -> None:
        self._providers: list[EmbeddingProvider] = list(providers or [])

    def register(self, provider: EmbeddingProvider) -> None:
        self._providers.append(provider)

    @property
    def providers(self) -> tuple[EmbeddingProvider, ...]:
        return tuple(self._providers)

    def select(self, criteria: EmbeddingSelectionCriteria) -> EmbeddingProviderSelection | None:
        providers = [provider for provider in self._providers if provider.is_available()]
        providers = self._apply_allow_deny(providers, criteria)
        if not providers:
            return None

        preferred = _normalize_provider_id(criteria.preferred_provider)
        if preferred:
            for provider in providers:
                if _normalize_provider_id(provider.provider_id) == preferred:
                    return EmbeddingProviderSelection(
                        provider=provider,
                        model=criteria.model or provider.default_model,
                    )

        if criteria.prefer_low_cost or criteria.prefer_low_latency:
            providers = sorted(providers, key=lambda item: _provider_score(item, criteria))

        chosen = providers[0]
        return EmbeddingProviderSelection(
            provider=chosen,
            model=criteria.model or chosen.default_model,
        )

    @staticmethod
    def _apply_allow_deny(
        providers: list[EmbeddingProvider],
        criteria: EmbeddingSelectionCriteria,
    ) -> list[EmbeddingProvider]:
        allowlist = {
            _normalize_provider_id(item) for item in (criteria.provider_allowlist or set())
        }
        denylist = {_normalize_provider_id(item) for item in (criteria.provider_denylist or set())}
        filtered: list[EmbeddingProvider] = []
        for provider in providers:
            provider_id = _normalize_provider_id(provider.provider_id)
            if allowlist and provider_id not in allowlist:
                continue
            if provider_id in denylist:
                continue
            filtered.append(provider)
        return filtered


def _normalize_provider_id(value: str | None) -> str | None:
    if not value:
        return None
    normalized = str(value).strip().lower()
    return normalized or None


def _provider_score(
    provider: EmbeddingProvider, criteria: EmbeddingSelectionCriteria
) -> tuple[float, float]:
    return (
        provider.cost_score if criteria.prefer_low_cost else 0.0,
        provider.latency_score if criteria.prefer_low_latency else 0.0,
    )


def bootstrap_registry() -> EmbeddingProviderRegistry:
    registry = EmbeddingProviderRegistry()
    registry.register(DeterministicEmbeddingProvider())
    return registry


__all__ = [
    "EmbeddingProvider",
    "EmbeddingProviderRegistry",
    "EmbeddingProviderSelection",
    "EmbeddingResponse",
    "EmbeddingSelectionCriteria",
    "bootstrap_registry",
]
