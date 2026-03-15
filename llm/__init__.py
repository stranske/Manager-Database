"""LLM utilities and client factory exports."""

from llm.client import ClientInfo, SlotDefinition, build_chat_client
from llm.evaluation import ManagerDBEvaluator
from llm.injection import (
    DEFAULT_BLOCK_SUMMARY,
    INJECTION_PATTERNS,
    PromptInjectionError,
    check_and_block,
    detect_prompt_injection,
    guard_chain_input,
    guard_input,
)
from llm.provider import LLMProviderConfig, create_llm
from llm.tracing import (
    langsmith_tracing_context,
    maybe_enable_langsmith_tracing,
    resolve_trace_url,
)

__all__ = [
    "ClientInfo",
    "DEFAULT_BLOCK_SUMMARY",
    "INJECTION_PATTERNS",
    "LLMProviderConfig",
    "ManagerDBEvaluator",
    "PromptInjectionError",
    "SlotDefinition",
    "build_chat_client",
    "check_and_block",
    "create_llm",
    "detect_prompt_injection",
    "guard_chain_input",
    "guard_input",
    "langsmith_tracing_context",
    "maybe_enable_langsmith_tracing",
    "resolve_trace_url",
]
