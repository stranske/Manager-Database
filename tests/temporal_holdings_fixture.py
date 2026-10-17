"""A correction retains stored history while removing obsolete current positions."""

import sqlite3


def correction_db():
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE managers(manager_id INTEGER PRIMARY KEY, name TEXT, cik TEXT);
        CREATE TABLE filings(filing_id INTEGER PRIMARY KEY, manager_id INTEGER,
            type TEXT, period_end TEXT, filed_date TEXT, url TEXT);
        CREATE TABLE holdings(holding_id INTEGER PRIMARY KEY, filing_id INTEGER,
            cusip TEXT, name_of_issuer TEXT, shares INTEGER, value_usd REAL,
            knowledge_time TEXT, superseded_at TEXT);
        INSERT INTO managers VALUES (1, 'TestFund', '0000000000');
        INSERT INTO filings VALUES (10, 1, '13F-HR', '2025-12-31', '2026-01-15', 'prior');
        INSERT INTO filings VALUES (20, 1, '13F-HR', '2026-03-31', '2026-04-15', 'current');
        INSERT INTO holdings VALUES
            (1,10,'KEEP','Retained',500,5000,'2026-01-15T00:00:00Z',NULL),
            (2,10,'REMOVE','Removed',300,3000,'2026-01-15T00:00:00Z',NULL),
            (3,20,'KEEP','Retained',1000,10000,'2026-04-15T00:00:00Z','2026-05-01T00:00:00Z'),
            (4,20,'REMOVE','Removed',300,3000,'2026-04-15T00:00:00Z','2026-05-01T00:00:00Z'),
            (5,20,'KEEP','Retained',2000,20000,'2026-05-01T00:00:00Z',NULL);
    """)
    return conn
