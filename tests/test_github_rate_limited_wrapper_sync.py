from __future__ import annotations

from pathlib import Path


def test_github_rate_limited_wrapper_has_expected_exports() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    wrapper_path = repo_root / ".github" / "scripts" / "github-rate-limited-wrapper.js"
    fixture_path = repo_root / "tests" / "fixtures" / "github-rate-limited-wrapper.js"

    assert wrapper_path.exists(), "Expected wrapper file to exist"
    assert fixture_path.exists(), "Expected wrapper fixture file to exist"

    wrapper_contents = wrapper_path.read_text(encoding="utf-8")
    fixture_contents = fixture_path.read_text(encoding="utf-8")
    assert "module.exports" in wrapper_contents
    assert "createRateLimitedGithub" in wrapper_contents
    assert "wrapWithRateLimitedGithub" in wrapper_contents
    assert (
        wrapper_contents == fixture_contents
    ), "Wrapper file should stay in sync with the approved fixture"


def test_github_rate_limited_wrapper_is_single_source_of_truth() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    wrapper_path = repo_root / ".github" / "scripts" / "github-rate-limited-wrapper.js"
    fixture_path = repo_root / "tests" / "fixtures" / "github-rate-limited-wrapper.js"

    expected_paths = {wrapper_path.resolve(), fixture_path.resolve()}
    found_paths = {
        path.resolve()
        for path in repo_root.rglob("github-rate-limited-wrapper.js")
        if ".git" not in path.parts and ".workflows-lib" not in path.parts
    }

    assert (
        found_paths == expected_paths
    ), "Only the wrapper and approved fixture should exist in the repo"


# Commit-message checklist:
# - [ ] type is accurate (test)
# - [ ] scope is clear (github-scripts)
# - [ ] summary is concise and imperative
