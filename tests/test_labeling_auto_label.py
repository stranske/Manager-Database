import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.langchain import integration_layer


def test_auto_label_marks_crash_reports_as_bug(monkeypatch):
    # Force keyword-only matching so the test stays deterministic.
    monkeypatch.setattr(
        integration_layer.label_matcher,
        "build_label_vector_store",
        lambda _labels, **_kwargs: None,
    )
    labels = [
        {"name": "bug", "description": "Something is broken or crashes"},
        {"name": "feature", "description": "New functionality requests"},
        {"name": "docs", "description": "Documentation updates"},
    ]
    issue = integration_layer.IssueData(
        title="Crash when database is unreachable",
        body="The service crashes on startup when Postgres is down.",
    )

    selected = integration_layer.label_issue(issue, labels, max_labels=1)

    assert selected == ["bug"]
    assert issue.labels == ["bug"]


# Commit-message checklist:
# - [ ] type is accurate (test, fix, feat)
# - [ ] scope is clear (labeling)
# - [ ] summary is concise and imperative
