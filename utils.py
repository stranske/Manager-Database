def format_provider_name(provider: str) -> str:
    """Format a provider identifier into a user-facing name."""
    if not provider or not provider.strip():
        return "Unknown"

    normalized = provider.strip().lower().replace("_", " ").replace("-", " ")
    normalized = " ".join(normalized.split())
    overrides = {
        "openai": "OpenAI",
        "azure openai": "Azure OpenAI",
        "anthropic": "Anthropic",
        "cohere": "Cohere",
        "google": "Google",
        "mistral": "Mistral",
        "bedrock": "Bedrock",
        "huggingface": "Hugging Face",
    }
    # Preserve canonical branding for known providers.
    if normalized in overrides:
        return overrides[normalized]

    return normalized.title()
