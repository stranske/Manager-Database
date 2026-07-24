import sqlite3

from etl.manager_similarity_flow import compute_manager_similarity


def test_similarity_uses_union_denominator_and_latest_filings():
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, period_end TEXT);
    CREATE TABLE holdings (filing_id INTEGER, cusip TEXT, resolved_ticker TEXT);
    INSERT INTO filings VALUES (1, 1, '2025-03-31'), (2, 2, '2025-03-31'), (3, 3, '2025-03-31');
    INSERT INTO holdings VALUES (1,'A',NULL),(1,'B',NULL),(1,'C',NULL),(1,'D',NULL),
      (2,'C',NULL),(2,'D',NULL),(2,'E',NULL),(2,'F',NULL),(3,'Z',NULL);"""
    )
    assert compute_manager_similarity(conn) == 3
    assert conn.execute(
        "SELECT jaccard, overlap_count, union_count FROM manager_similarity WHERE manager_id_a=1 AND manager_id_b=2"
    ).fetchone() == (2 / 6, 2, 6)
    assert conn.execute(
        "SELECT jaccard FROM manager_similarity WHERE manager_id_a=1 AND manager_id_b=3"
    ).fetchone() == (0.0,)
