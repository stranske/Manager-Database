from utils import format_provider_name


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
