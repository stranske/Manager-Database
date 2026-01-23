import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.langchain.issue_dedup import (
    IssueMatch,
    IssueRecord,
    IssueVectorStore,
    find_similar_issues,
    format_similar_issues_comment,
)


@dataclass
class _FakeDoc:
    """Simple stand-in for langchain docs in duplicate detection tests."""

    page_content: str
    metadata: dict


class _FakeStore:
    def __init__(self, results):
        # Store canned similarity results so tests stay deterministic.
        self._results = results

    def similarity_search_with_relevance_scores(self, _query, k=5):
        return self._results[:k]


def test_unrelated_issue_is_not_flagged_as_duplicate():
    unrelated_doc = _FakeDoc(
        page_content="Unrelated performance dashboard request",
        metadata={"title": "Add dashboard charts", "number": 12, "url": "http://example"},
    )
    store = IssueVectorStore(
        store=_FakeStore([(unrelated_doc, 0.2)]),
        provider="fake",
        model="fake",
        issues=[IssueRecord(number=12, title="Add dashboard charts")],
    )

    matches = find_similar_issues(store, "Investigate SEC filing parser")
    assert matches == []
    assert format_similar_issues_comment(matches) is None


def test_duplicate_comment_includes_details_block_and_separator():
    # Build a synthetic match to validate the comment structure is preserved.
    match = IssueMatch(
        issue=IssueRecord(number=7, title="Cache outage", url="http://example"),
        score=0.92,
        raw_score=0.92,
        score_type="relevance",
    )

    comment = format_similar_issues_comment([match])
    assert comment is not None
    assert "</details>" in comment
    assert "---" in comment


# Commit-message checklist:
# - [ ] type is accurate (test, fix, feat)
# - [ ] scope is clear (issue_dedup)
# - [ ] summary is concise and imperative
