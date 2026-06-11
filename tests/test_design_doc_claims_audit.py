from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
REPORT = REPO_ROOT / "docs/reports/design-doc-behavioral-claims-audit.md"


def test_design_doc_claims_audit_covers_current_behavioral_claims() -> None:
    content = REPORT.read_text(encoding="utf-8")

    required_claim_refs = [
        "Manager-Intel-Platform.md:7",
        "Manager-Intel-Platform.md:9",
        "Manager-Intel-Platform.md:110",
        "Manager-Intel-Platform.md:112",
        "README_bootstrap.md:69",
        "README_bootstrap.md:241",
        "docs/api_design_guidelines.md:11",
        "docs/api_rate_limiting.md:43",
    ]
    for claim_ref in required_claim_refs:
        assert claim_ref in content

    assert "implemented-and-verified" in content
    assert "unimplemented" in content
    assert "contradicted" in content
    assert "implemented-partial" not in content
    assert "#1150" in content
    assert "#1151" in content


def test_design_doc_claims_audit_records_known_resolved_drift() -> None:
    content = REPORT.read_text(encoding="utf-8")

    assert "Resolved by #1142" in content
    assert "Resolved by #1145" in content
    assert (
        "| `Manager-Intel-Platform.md:7` | Nightly ETL job hits official APIs,"
        " downloads new filings, parses metadata, extracts tables, and stores"
        " PDF/text. | unimplemented |"
    ) in content
    assert (
        "| `docs/api_design_guidelines.md:11` | Original drift: the guideline"
        " claimed all API endpoints were rate limited even though only chat"
        " write paths used the limiter. | contradicted |"
    ) in content
