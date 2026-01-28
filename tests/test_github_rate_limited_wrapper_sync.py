from __future__ import annotations

from pathlib import Path


def test_github_rate_limited_wrapper_has_expected_exports() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    wrapper_path = repo_root / ".github" / "scripts" / "github-rate-limited-wrapper.js"

    assert wrapper_path.exists(), "Expected wrapper file to exist"

    wrapper_contents = wrapper_path.read_text(encoding="utf-8")
    assert "module.exports" in wrapper_contents
    assert "createRateLimitedGithub" in wrapper_contents
    assert "wrapWithRateLimitedGithub" in wrapper_contents


# Commit-message checklist:
# - [ ] type is accurate (test)
# - [ ] scope is clear (github-scripts)
# - [ ] summary is concise and imperative
