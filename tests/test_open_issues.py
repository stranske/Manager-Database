import textwrap

from scripts.open_issues import parse_tasks


def test_parse_tasks(tmp_path):
    md = textwrap.dedent("""
        ### 4.1 Stage 0 — Bootstrap
        1. Create docker-compose
        2. Create schema
        ### 4.2 Stage 1 — Proof
        * Implement adapter
        """)
    file = tmp_path / "a.md"
    file.write_text(md)
    tasks = parse_tasks(str(file))
    assert tasks == [
        ("Stage0", "Create docker-compose"),
        ("Stage0", "Create schema"),
        ("Stage1", "Implement adapter"),
    ]
