from __future__ import annotations

import ast
from pathlib import Path

import pytest

from api import cache as cache_module
from config import (
    DEFAULT_CACHE_MAX_ITEMS,
    DEFAULT_CACHE_TTL_SECONDS,
    DEFAULT_DB_PATH,
    load_runtime_config,
)
from etl import edgar_flow

ROOT = Path(__file__).resolve().parents[1]


def _defined_functions(path: str) -> set[str]:
    tree = ast.parse((ROOT / path).read_text(encoding="utf-8"))
    return {node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}


def test_runtime_config_centralizes_db_and_cache_defaults(monkeypatch):
    for name in ("DB_URL", "DB_PATH", "CACHE_TTL_SECONDS", "CACHE_MAX_ITEMS", "REDIS_URL"):
        monkeypatch.delenv(name, raising=False)

    config = load_runtime_config()

    assert config.db_url is None
    assert config.db_path == DEFAULT_DB_PATH
    assert config.cache_ttl_seconds == DEFAULT_CACHE_TTL_SECONDS
    assert config.cache_max_items == DEFAULT_CACHE_MAX_ITEMS
    assert config.redis_url is None


def test_runtime_config_uses_positive_cache_env_values(monkeypatch):
    monkeypatch.setenv("CACHE_TTL_SECONDS", "7")
    monkeypatch.setenv("CACHE_MAX_ITEMS", "11")

    config = load_runtime_config()

    assert config.cache_ttl_seconds == 7
    assert config.cache_max_items == 11


def test_runtime_config_falls_back_for_bad_cache_env_values(monkeypatch):
    monkeypatch.setenv("CACHE_TTL_SECONDS", "not-int")
    monkeypatch.setenv("CACHE_MAX_ITEMS", "-1")

    config = load_runtime_config()

    assert config.cache_ttl_seconds == DEFAULT_CACHE_TTL_SECONDS
    assert config.cache_max_items == DEFAULT_CACHE_MAX_ITEMS


def test_redis_backend_does_not_swallow_unexpected_errors(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    cache_module.reset_cache_backend()

    def unexpected_failure(_url: str):
        raise RuntimeError("programmer error")

    monkeypatch.setattr(cache_module, "_build_redis_client", unexpected_failure)

    with pytest.raises(RuntimeError, match="programmer error"):
        cache_module._get_backend()


def test_store_document_does_not_swallow_unexpected_import_errors(monkeypatch):
    def unexpected_import_error(_name: str):
        raise RuntimeError("broken module side effect")

    monkeypatch.setattr(edgar_flow.importlib, "import_module", unexpected_import_error)

    with pytest.raises(RuntimeError, match="broken module side effect"):
        edgar_flow.store_document("raw text")


def test_removed_dead_code_does_not_reappear():
    assert "_deserialize_json_object" not in _defined_functions("etl/activism_detection.py")
    assert "evaluate_filing_summary_completeness" not in _defined_functions("llm/evaluation.py")
    assert "search_notes" not in _defined_functions("ui/search.py")


def test_kept_issue_listed_helpers_still_have_callers():
    assert "search_news" in _defined_functions("ui/search.py")
    assert "detect_events_batch" in _defined_functions("etl/activism_detection.py")
