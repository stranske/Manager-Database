import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from langchain_analysis import detect_llm_provider


@pytest.mark.parametrize(
    ("identifier", "expected"),
    [
        ("gpt-4o", "OpenAI"),
        ("azure-openai:gpt-4", "Azure OpenAI"),
        ("claude-3-opus", "Anthropic"),
        ("command-r", "Cohere"),
        ("gemini-1.5-pro", "Google"),
        ("mixtral-8x7b", "Mistral"),
        ("hf/tiiuae/falcon-7b", "Hugging Face"),
        ("mystery-model", "Unknown"),
    ],
)
def test_detect_llm_provider(identifier, expected):
    # Exercise common LangChain model identifiers across providers.
    assert detect_llm_provider(identifier) == expected
