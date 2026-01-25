import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.langchain import issue_formatter


def _section_lines(formatted: str, header: str) -> list[str]:
    lines = formatted.splitlines()
    try:
        start = next(i for i, line in enumerate(lines) if line.strip() == header)
    except StopIteration:
        return []
    end = next(
        (
            i
            for i in range(start + 1, len(lines))
            if lines[i].startswith("## ") and lines[i].strip() != header
        ),
        len(lines),
    )
    return [line.strip() for line in lines[start + 1 : end] if line.strip()]


def test_format_issue_fallback_ignores_structural_lines_in_checklists():
    raw = """## Tasks
- [ ] Fix crash when database is unreachable
---
```
not a task
```
</details>

## Acceptance Criteria
- [ ] Returns 503 Service Unavailable on DB failure
```
also not a task
```
---
</details>
"""

    formatted = issue_formatter._format_issue_fallback(raw)
    tasks_lines = _section_lines(formatted, "## Tasks")
    acceptance_lines = _section_lines(formatted, "## Acceptance Criteria")

    assert "- [ ] Fix crash when database is unreachable" in tasks_lines
    assert "- [ ] Returns 503 Service Unavailable on DB failure" in acceptance_lines
    for line in tasks_lines + acceptance_lines:
        assert not line.startswith("```")
        assert line not in {"---", "<details>", "</details>"}


# Commit-message checklist:
# - [ ] type is accurate (test)
# - [ ] scope is clear (issue_formatter)
# - [ ] summary is concise and imperative
