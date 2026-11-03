#!/usr/bin/env python3
"""Run break-and-revert proof for tests/test_chat_coverage_round_1840.py."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
CHAT = REPO / "api" / "chat.py"
OUT = Path(__file__).resolve().parent / "mutation-run"
PYTHON = "/opt/anaconda3/bin/python3"
TEST_FILE = "tests/test_chat_coverage_round_1840.py"

# (test_name, old, new, mutation_label)
MUTATIONS: list[tuple[str, str, str, str]] = [
    (
        "test_resolve_chain_name_auto_filing_requires_context_filing_id",
        'if classified_chain == "filing_summary" and not (context or {}).get("filing_id"):\n        return "rag_search"',
        'if classified_chain == "filing_summary" and not (context or {}).get("filing_id"):\n        return "nl_query"',
        "auto filing without id returns nl_query instead of rag_search",
    ),
    (
        "test_context_filter_requested_detects_empty_and_non_empty_values",
        "        if isinstance(value, (list, tuple)):\n            if value:",
        "        if isinstance(value, (list, tuple)):\n            if False and value:",
        "non-empty list filter ignored",
    ),
    (
        "test_normalize_sources_and_extract_chain_payload_object_paths",
        "    return []\n\n\ndef _extract_chain_payload",
        "    return [{\"broken\": True}]\n\n\ndef _extract_chain_payload",
        "normalize_sources default returns spurious entry",
    ),
    (
        "test_get_health_executor_recreates_after_shutdown",
        "if _HEALTH_EXECUTOR._shutdown:",
        "if False and _HEALTH_EXECUTOR._shutdown:",
        "shutdown executor never recreated",
    ),
    (
        "test_record_chat_fleet_safe_swallows_observability_failures",
        "    except Exception:  # pragma: no cover - never let observability break the API\n        logger.debug(\"LangSmith fleet emission failed\", exc_info=True)",
        "    except Exception:\n        raise",
        "fleet observability errors propagate",
    ),
    (
        "test_run_chain_rag_campaign_snapshot_unavailable",
        "No materialized campaign snapshot is available for the selected scope.",
        "Campaign snapshot ready.",
        "wrong unavailable message",
    ),
    (
        "test_run_chain_rejects_invalid_holdings_as_of",
        'detail="as_of must be a valid ISO date or timestamp"',
        'detail="as_of accepted"',
        "invalid as_of accepted",
    ),
    (
        "test_run_chain_requires_filing_id_for_filing_summary",
        'detail="filing_summary requires context.filing_id"',
        'detail="ok"',
        "missing filing_id allowed",
    ),
    (
        "test_run_chain_awaits_coroutine_results",
        "    if asyncio.iscoroutine(result):\n        result = await result",
        "    if False and asyncio.iscoroutine(result):\n        result = await result",
        "coroutine chain results not awaited",
    ),
    (
        "test_build_chain_uses_fallback_after_generic_import_error",
        "    except Exception as exc:\n        if _chat_chain_fallback_enabled():",
        "    except Exception as exc:\n        if False and _chat_chain_fallback_enabled():",
        "generic import error does not use fallback",
    ),
    (
        "test_chat_chain_health_when_llm_zone_disabled",
        'return {"healthy": True, "available": True, "fallback_enabled": fallback_enabled}, None',
        'return {"healthy": False, "available": True, "fallback_enabled": fallback_enabled}, None',
        "disabled zone reported unhealthy",
    ),
    (
        "test_enforce_chat_rate_limit_raises_when_budget_exceeded",
        "            if len(history) >= self._max_requests:\n                return False",
        "            if len(history) >= self._max_requests:\n                return True",
        "rate limiter never blocks",
    ),
    (
        "test_direct_chat_wrappers_delegate_to_chat_api",
        '            context={"filing_id": filing_id},',
        '            context={"filing_id": filing_id + 1},',
        "filing_summary wrapper wrong filing id",
    ),
    (
        "test_resolve_chain_name_falls_back_when_classifier_returns_unknown",
        "    if classified_chain in DIRECT_CHAIN_PATHS:\n        return classified_chain\n    return \"rag_search\"",
        "    if classified_chain in DIRECT_CHAIN_PATHS:\n        return classified_chain\n    return classified_chain",
        "unknown classifier chain not coerced to rag_search",
    ),
    (
        "test_enforce_chat_rate_limit_session_budget_after_ip_passes",
        "    if not CHAT_RATE_LIMITER.check_and_record(session_id):\n        raise HTTPException(status_code=429, detail=\"Rate limit exceeded\")",
        "    if not CHAT_RATE_LIMITER.check_and_record(session_id):\n        raise HTTPException(status_code=429, detail=\"Rate limit ok\")",
        "session rate limit wrong detail",
    ),
    (
        "test_run_chain_holdings_analysis_formats_payload_with_as_of",
        'thesis = str(payload.get("thesis") or "Holdings analysis complete.")',
        'thesis = "broken thesis"',
        "holdings thesis text wrong",
    ),
    (
        "test_run_chain_rejects_non_string_holdings_as_of",
        "            if not isinstance(raw_cutoff, str):\n                raise ValueError(\"as_of must be ISO text\")",
        "            if False and not isinstance(raw_cutoff, str):\n                raise ValueError(\"as_of must be ISO text\")",
        "non-string as_of accepted",
    ),
    (
        "test_run_chain_uses_invoke_when_run_missing",
        'elif hasattr(chain, "invoke"):',
        'elif False and hasattr(chain, "invoke"):',
        "invoke path disabled",
    ),
    (
        "test_run_chain_raises_when_chain_lacks_run_and_invoke",
        'raise RuntimeError(f"Chain \'{chain_name}\' does not expose run/invoke")',
        "pass",
        "broken chain allowed",
    ),
    (
        "test_run_chain_filing_summary_formats_structured_payload",
        'manager_name = str(payload.get("manager_name") or "Unknown manager")',
        'manager_name = "Wrong manager"',
        "filing summary manager name wrong",
    ),
    (
        "test_chat_api_disabled_zone_returns_notice",
        'chain_used="disabled"',
        'chain_used="enabled"',
        "disabled zone wrong chain_used",
    ),
    (
        "test_chat_api_requires_llm_client",
        "if not client_info:",
        "if False and not client_info:",
        "missing LLM client ignored",
    ),
    (
        "test_chat_api_success_path_records_fleet",
        'response_id = _response_id_from_trace_url(trace_url) or str(uuid.uuid4())',
        "response_id = \"wrong-id\"",
        "success path wrong response_id",
    ),
    (
        "test_chat_api_maps_chain_unavailable_to_503",
        "raise HTTPException(status_code=503, detail=str(exc)) from exc",
        "raise HTTPException(status_code=502, detail=str(exc)) from exc",
        "chain unavailable mapped to 502",
    ),
    (
        "test_chat_api_maps_unexpected_errors_to_500",
        "            status_code=500,\n            detail=\"Research assistant error. Check server logs.\",",
        "            status_code=502,\n            detail=\"Research assistant error. Check server logs.\",",
        "unexpected error mapped to 502",
    ),
    (
        "test_submit_feedback_persists_and_forwards",
        '    return {"ok": True, "feedback_id": feedback_id}',
        '    return {"ok": True, "feedback_id": feedback_id + 1}',
        "feedback id wrong",
    ),
    (
        "test_build_chain_instantiates_imported_classes",
        "        if chain_name == \"filing_summary\":\n            return chain_cls(client_info=client_info, db_conn=db_conn)",
        "        if chain_name == \"filing_summary\":\n            return chain_cls(llm=client_info.client if client_info else None, db_conn=db_conn)",
        "filing chain constructed with nl_query kwargs",
    ),
    (
        "test_rate_limiter_clear_resets_window",
        "            self._requests.clear()",
        "            pass",
        "clear() no-op",
    ),
    (
        "test_parse_date_range_and_empty_manager_name_edges",
        "    elif isinstance(value, (list, tuple)) and len(value) == 2:",
        "    elif isinstance(value, (list, tuple)) and len(value) == 3:",
        "list date range arity wrong",
    ),
    (
        "test_fallback_filing_summary_chain_optional_filing_id",
        'return {"answer": f"Filing summary{detail}", "sources": []}',
        'return {"answer": "broken", "sources": []}',
        "fallback filing summary wrong answer",
    ),
    (
        "test_build_chain_unavailable_without_fallback_and_ctor_closes_db",
        "        raise ChainUnavailableError(f\"Chat chain '{chain_name}' is unavailable\") from exc",
        "        return FALLBACK_CHAINS[chain_name]()",
        "unavailable chain returns fallback stub",
    ),
    (
        "test_build_chat_client_info_calls_callable_builder",
        "            return build_fn()",
        "            return None",
        "callable builder ignored",
    ),
    (
        "test_manager_id_column_falls_back_when_helper_returns_none",
        'return manager_id_column(conn, require_table=True) or "id"',
        'return manager_id_column(conn, require_table=True) or "broken"',
        "manager id column fallback wrong",
    ),
    (
        "test_chat_chain_health_payload_import_success_and_failure",
        '    return {"healthy": True, "available": True, "fallback_enabled": False}, None\n\n\nasync def _run_dependency_check',
        '    return {"healthy": False, "available": True, "fallback_enabled": False}, None\n\n\nasync def _run_dependency_check',
        "import success reported unhealthy",
    ),
    (
        "test_run_dependency_check_disabled_and_success_paths",
        "    if not enabled:\n        payload = {\"healthy\": True, \"latency_ms\": 0}",
        "    if not enabled:\n        payload = {\"healthy\": False, \"latency_ms\": 0}",
        "disabled dependency check unhealthy",
    ),
    (
        "test_run_health_summary_checks_success_and_timeout",
        "            results[name] = task.result()",
        '            payload, reason = task.result()\n            payload = {**payload, "healthy": False}\n            results[name] = (payload, reason)',
        "completed health checks forced unhealthy",
    ),
    (
        "test_chat_api_prompt_injection_joins_list_reasons",
        'detail=f"Input rejected: {reason_text}"',
        'detail="Input rejected: broken"',
        "prompt injection detail wrong",
    ),
    (
        "test_attach_langsmith_feedback_success_path",
        "        return True\n    except Exception:\n        logger.warning(\n            \"Failed to attach feedback to LangSmith\"",
        "        return False\n    except Exception:\n        logger.warning(\n            \"Failed to attach feedback to LangSmith\"",
        "langsmith feedback reported failure",
    ),
    (
        "test_liveness_aliases_and_shutdown_hooks",
        "    return _health_payload()\n\n\n@app.get(\"/healthz\")",
        '    return {"healthy": False}\n\n\n@app.get("/healthz")',
        "health_live wrong payload",
    ),
    (
        "test_configure_default_executor_and_memory_profiler_shutdown",
        "    asyncio.get_running_loop().set_default_executor(_APP_EXECUTOR)",
        "    asyncio.get_running_loop().set_default_executor(_HEALTH_EXECUTOR)",
        "default executor not APP_EXECUTOR",
    ),
    (
        "test_health_app_marks_unhealthy_dependencies",
        "        status_code = 200 if healthy else 503",
        "        status_code = 200",
        "unhealthy dependencies return 200",
    ),
]


def run_pytest(node: str) -> tuple[int, str]:
    cmd = [
        PYTHON,
        "-m",
        "pytest",
        f"{TEST_FILE}::{node}",
        "-q",
        "--tb=short",
        "--disable-warnings",
    ]
    proc = subprocess.run(
        cmd,
        cwd=REPO,
        env={**os.environ, "PYTHONPATH": str(REPO)},
        capture_output=True,
        text=True,
    )
    return proc.returncode, proc.stdout + proc.stderr


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    original = CHAT.read_text(encoding="utf-8")
    failures: list[str] = []

    for node, old, new, label in MUTATIONS:
        if old not in original:
            failures.append(f"{node}: old snippet missing ({label})")
            continue
        mutated = original.replace(old, new, 1)
        CHAT.write_text(mutated, encoding="utf-8")
        red_code, red_out = run_pytest(node)
        (OUT / f"{node}-red.txt").write_text(red_out, encoding="utf-8")
        CHAT.write_text(original, encoding="utf-8")
        green_code, green_out = run_pytest(node)
        (OUT / f"{node}-green.txt").write_text(green_out, encoding="utf-8")
        if red_code == 0:
            failures.append(f"{node}: expected failure after mutation ({label})")
        if green_code != 0:
            failures.append(f"{node}: expected pass after restore ({label})")

    summary = "\n".join(failures) if failures else f"all {len(MUTATIONS)} mutations ok"
    (OUT / "summary.txt").write_text(summary + "\n", encoding="utf-8")
    print(summary)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
