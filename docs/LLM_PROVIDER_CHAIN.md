# LLM provider chain

This repository resolves the LLM provider in a simple, ordered chain:

1. GitHub Models
   - Selected when `GITHUB_MODELS_ENDPOINT` or `GITHUB_MODELS_TOKEN` is set.
   - This is the first choice because it can proxy multiple model backends.
2. OpenAI
   - Selected when `OPENAI_API_KEY` or `OPENAI_BASE_URL` is set and GitHub
     Models is not configured.
3. Regex fallback
   - If neither provider is configured, the provider is inferred by regex
     matching on the model identifier (see `langchain_analysis.detect_llm_provider`).
   - This is a best-effort heuristic used for logs and UI labels.

## Preferred provider override

Set `PREFERRED_LLM_PROVIDER` to force a specific provider name regardless of
chain position. This is helpful when the model identifier is ambiguous or when
running in a mixed environment.

Examples:

- `PREFERRED_LLM_PROVIDER=openai`
- `PREFERRED_LLM_PROVIDER=github models`
- `PREFERRED_LLM_PROVIDER=azure openai`
