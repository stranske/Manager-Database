# Documentation index

<!-- Keep provider details aligned with langchain_analysis.py and docs/LLM_PROVIDER_CHAIN.md. -->

## Supported LLM providers

The LLM analysis layer recognizes the providers below. These names are the
canonical labels surfaced in logs and UI.

- GitHub Models
- OpenAI
- Azure OpenAI
- Anthropic
- Cohere
- Mistral
- Google (Gemini/PaLM)
- Amazon Bedrock
- Hugging Face
- Unknown (fallback when no match is detected)

## How the provider is resolved

1. `PREFERRED_LLM_PROVIDER` overrides everything when set.
2. GitHub Models is selected if `GITHUB_MODELS_ENDPOINT` or
   `GITHUB_MODELS_TOKEN` is configured.
3. OpenAI is selected if `OPENAI_API_KEY` or `OPENAI_BASE_URL` is configured.
4. If no configuration is present, the provider is inferred by regex matching
   on the model identifier (see `docs/LLM_PROVIDER_CHAIN.md`).

<!-- Commit-message checklist:
- [ ] docs: add docs/README.md for supported LLM providers
- [ ] docs: keep provider resolution notes aligned with code
-->
