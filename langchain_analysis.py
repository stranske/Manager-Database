import os

from utils import format_provider_name


PREFERRED_LLM_PROVIDER_ENV = "PREFERRED_LLM_PROVIDER"
GITHUB_MODELS_ENDPOINT_ENV = "GITHUB_MODELS_ENDPOINT"
GITHUB_MODELS_TOKEN_ENV = "GITHUB_MODELS_TOKEN"
OPENAI_API_KEY_ENV = "OPENAI_API_KEY"
OPENAI_BASE_URL_ENV = "OPENAI_BASE_URL"


def detect_llm_provider(model_identifier: str) -> str:
    """Infer the LLM provider based on a LangChain model identifier."""
    if not model_identifier:
        return format_provider_name("")

    identifier = model_identifier.strip().lower()
    # Order matters to avoid Azure OpenAI being classified as generic OpenAI.
    if "azure" in identifier:
        return format_provider_name("azure openai")
    if any(token in identifier for token in ("openai", "gpt-", "text-", "o1-", "o3-")):
        return format_provider_name("openai")
    if "anthropic" in identifier or "claude" in identifier:
        return format_provider_name("anthropic")
    if "cohere" in identifier or "command" in identifier:
        return format_provider_name("cohere")
    if "mistral" in identifier or "mixtral" in identifier:
        return format_provider_name("mistral")
    if "gemini" in identifier or "palm" in identifier or "google" in identifier:
        return format_provider_name("google")
    if "bedrock" in identifier or "titan" in identifier or "aws" in identifier:
        return format_provider_name("bedrock")
    if "huggingface" in identifier or identifier.startswith("hf/"):
        return format_provider_name("huggingface")

    return format_provider_name("unknown")


def resolve_llm_provider(model_identifier: str) -> str:
    """Resolve the provider, honoring an explicit preference when configured."""
    preferred = os.getenv(PREFERRED_LLM_PROVIDER_ENV, "").strip()
    if preferred:
        # Allow explicit preferences (ex: "openai") to override detection.
        return format_provider_name(preferred)
    if os.getenv(GITHUB_MODELS_ENDPOINT_ENV) or os.getenv(GITHUB_MODELS_TOKEN_ENV):
        # Prefer GitHub Models when its endpoint or token is configured.
        return format_provider_name("github models")
    if os.getenv(OPENAI_API_KEY_ENV) or os.getenv(OPENAI_BASE_URL_ENV):
        # Use OpenAI when a direct OpenAI configuration is present.
        return format_provider_name("openai")
    return detect_llm_provider(model_identifier)
