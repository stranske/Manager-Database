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


def test_bug_keyword_beats_semantic_match(monkeypatch):
    class FakeDoc:
        def __init__(self, metadata):
            self.metadata = metadata
            self.page_content = metadata.get("name")

    class FakeStore:
        def similarity_search_with_score(self, _query, k=1):
            return [(FakeDoc({"name": "feature"}), 0.0)]

    labels = [
        {"name": "bug", "description": "Something is broken or crashes"},
        {"name": "feature", "description": "New functionality requests"},
    ]
    label_store = integration_layer.label_matcher.LabelVectorStore(
        store=FakeStore(),
        provider="fake",
        model="fake",
        labels=[integration_layer.label_matcher.LabelRecord(**label) for label in labels],
    )

    matches = integration_layer.label_matcher.find_similar_labels(
        label_store,
        "Crash when database is unreachable",
        threshold=0.2,
        k=1,
    )

    assert matches[0].label.name == "bug"


# Commit-message checklist:
# - [ ] type is accurate (test, fix, feat)
# - [ ] scope is clear (labeling)
# - [ ] summary is concise and imperative
