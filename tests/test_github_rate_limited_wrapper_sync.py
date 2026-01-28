from __future__ import annotations

from pathlib import Path


def test_github_rate_limited_wrapper_matches_template() -> None:
    # Guard against accidental edits outside the workflow template source.
    repo_root = Path(__file__).resolve().parents[1]
    template_path = (
        repo_root / ".workflows-lib" / ".github" / "scripts" / "github-rate-limited-wrapper.js"
    )
    wrapper_path = repo_root / ".github" / "scripts" / "github-rate-limited-wrapper.js"

    assert template_path.exists(), "Expected workflow template file to exist"
    assert wrapper_path.exists(), "Expected wrapper file to exist"

    template_contents = template_path.read_text(encoding="utf-8")
    wrapper_contents = wrapper_path.read_text(encoding="utf-8")

    assert wrapper_contents == template_contents


# Commit-message checklist:
# - [ ] type is accurate (test)
# - [ ] scope is clear (github-scripts)
# - [ ] summary is concise and imperative
