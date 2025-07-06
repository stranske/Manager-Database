from scripts.sync_milestones import parse_milestones


def test_parse_milestones(tmp_path):
    md = (
        "| Milestone ID | Goal | Due Date |\n"
        "|--------------|------|----------|\n"
        "| **M1** | First milestone | 2025-01-01 |\n"
        "| **M2** | Second milestone | 2025-02-02 |\n"
    )
    f = tmp_path / "plan.md"
    f.write_text(md)
    ms = parse_milestones(str(f))
    assert ms == [
        ("M1", "First milestone", "2025-01-01"),
        ("M2", "Second milestone", "2025-02-02"),
    ]
