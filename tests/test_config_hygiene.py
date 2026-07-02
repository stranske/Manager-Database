from __future__ import annotations

import ast
import sys
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


def _has_external_callers(name: str, definition_path: str) -> bool:
    definition_file = (ROOT / definition_path).resolve()
    for path in ROOT.rglob("*.py"):
        if path.resolve() == definition_file:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (OSError, SyntaxError, UnicodeDecodeError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                if (isinstance(func, ast.Name) and func.id == name) or (
                    isinstance(func, ast.Attribute) and func.attr == name
                ):
                    return True
    return False


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


def test_redis_fallback_warning_does_not_log_secret_url(monkeypatch, caplog):
    redis_url = "redis://:super-secret@example.invalid:6379/0"
    monkeypatch.setenv("REDIS_URL", redis_url)
    cache_module.reset_cache_backend()

    def optional_backend_unavailable(_url: str):
        raise ValueError(f"cannot connect to {redis_url}")

    monkeypatch.setattr(cache_module, "_build_redis_client", optional_backend_unavailable)

    with caplog.at_level("WARNING", logger=cache_module.logger.name):
        cache_module._get_backend()

    assert "ValueError" in caplog.text
    assert redis_url not in caplog.text
    assert "super-secret" not in caplog.text


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
    assert _has_external_callers("search_news", "ui/search.py")
    assert _has_external_callers("detect_events_batch", "etl/activism_detection.py")


def test_external_caller_scan_skips_bad_files_and_finds_attribute_calls(tmp_path, monkeypatch):
    (tmp_path / "helpers.py").write_text("def search_news():\n    pass\n", encoding="utf-8")
    (tmp_path / "caller.py").write_text(
        "import helpers\nhelpers.search_news()\n",
        encoding="utf-8",
    )
    (tmp_path / "bad_syntax.py").write_text("def broken(:\n", encoding="utf-8")
    monkeypatch.setattr(sys.modules[__name__], "ROOT", tmp_path)

    assert _has_external_callers("search_news", "helpers.py")
