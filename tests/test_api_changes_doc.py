from pathlib import Path


def test_api_changes_document_default_limit_confirmation():
    api_changes = Path(__file__).resolve().parents[1] / "docs" / "api_changes.md"
    content = api_changes.read_text(encoding="utf-8")
    assert "default pagination limit is now always set to 25" in content
    assert "Confirmation:" in content
    assert "docs/api_design_guidelines.md" in content
