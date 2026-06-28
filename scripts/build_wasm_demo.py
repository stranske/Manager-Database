"""Build the synthetic SQLite bundle used by the offline stlite demo."""

from __future__ import annotations

import os
import shutil
import sqlite3
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.seed_managers import SEED_MANAGERS
from scripts.seed_readiness_data import READINESS_DOC_FILENAME, READINESS_DOC_TEXT

DEFAULT_OUTPUT_DIR = Path("web")
DEFAULT_DB_NAME = "manager_demo.sqlite"
STATIC_ROUTE_PATHS = ("daily-report", "search", "upload")
STATIC_SHELL_FILES = (
    Path("web/index.html"),
    Path("web/wasm_app.py"),
)
TEXT_BUNDLE_FILES = [
    Path("embeddings.py"),
    Path("ui/__init__.py"),
    Path("ui/dashboard.py"),
    Path("ui/daily_report.py"),
    Path("ui/search.py"),
    Path("ui/upload.py"),
    Path("api/__init__.py"),
    Path("api/_compat.py"),
    Path("api/activism.py"),
    Path("api/search.py"),
    Path("api/signals.py"),
    Path("adapters/__init__.py"),
    Path("adapters/base.py"),
    Path("utils/__init__.py"),
    Path("utils/extract.py"),
]


def _copy_static_shell(output_dir: Path) -> None:
    """Copy the static shell when building into a separate output directory."""
    for source in STATIC_SHELL_FILES:
        target = output_dir / source.name
        if source.resolve() == target.resolve():
            continue
        shutil.copy2(source, target)


def _route_shell_html(index_html: str) -> str:
    if "<base " in index_html:
        return index_html
    return index_html.replace("<head>", '<head>\n    <base href="../" />', 1)


def _write_static_route_entrypoints(output_dir: Path) -> None:
    index_path = output_dir / "index.html"
    if not index_path.exists():
        return
    route_html = _route_shell_html(index_path.read_text(encoding="utf-8"))
    for route_path in STATIC_ROUTE_PATHS:
        route_index = output_dir / route_path / "index.html"
        route_index.parent.mkdir(parents=True, exist_ok=True)
        route_index.write_text(route_html, encoding="utf-8")


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE managers (
            manager_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            cik TEXT UNIQUE,
            aliases TEXT,
            jurisdictions TEXT,
            tags TEXT
        );
        CREATE TABLE filings (
            filing_id INTEGER PRIMARY KEY,
            manager_id INTEGER,
            type TEXT,
            filed_date TEXT,
            period_end TEXT,
            source TEXT,
            raw_key TEXT
        );
        CREATE TABLE holdings (
            holding_id INTEGER PRIMARY KEY,
            filing_id INTEGER,
            manager_id INTEGER,
            filed TEXT,
            name_of_issuer TEXT,
            cusip TEXT,
            shares REAL,
            value_usd REAL
        );
        CREATE TABLE daily_diffs (
            manager_id INTEGER,
            report_date TEXT,
            cusip TEXT,
            name_of_issuer TEXT,
            delta_type TEXT,
            shares_prev REAL,
            shares_curr REAL,
            value_prev REAL,
            value_curr REAL
        );
        CREATE TABLE news_items (
            news_id INTEGER PRIMARY KEY,
            manager_id INTEGER,
            headline TEXT,
            url TEXT,
            published_at TEXT,
            source TEXT,
            topics TEXT,
            confidence REAL,
            body_snippet TEXT
        );
        CREATE TABLE documents (
            doc_id INTEGER PRIMARY KEY,
            manager_id INTEGER,
            kind TEXT NOT NULL DEFAULT 'note',
            filename TEXT,
            sha256 TEXT,
            text TEXT,
            embedding TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE api_usage (
            id INTEGER PRIMARY KEY,
            ts TEXT DEFAULT CURRENT_TIMESTAMP,
            source TEXT,
            endpoint TEXT,
            status INT,
            bytes INT,
            latency_ms INT,
            cost_usd REAL
        );
        CREATE TABLE crowded_trades (
            crowd_id INTEGER PRIMARY KEY,
            cusip TEXT,
            name_of_issuer TEXT,
            manager_count INTEGER,
            manager_ids TEXT,
            total_value_usd REAL,
            avg_conviction_pct REAL,
            max_conviction_pct REAL,
            report_date TEXT,
            computed_at TEXT
        );
        CREATE TABLE contrarian_signals (
            signal_id INTEGER PRIMARY KEY,
            manager_id INTEGER,
            cusip TEXT,
            name_of_issuer TEXT,
            direction TEXT,
            consensus_direction TEXT,
            manager_delta_shares INTEGER,
            manager_delta_value REAL,
            consensus_count INTEGER,
            report_date TEXT,
            detected_at TEXT
        );
        """)


def _seed_sqlite(conn: sqlite3.Connection) -> None:
    for idx, manager in enumerate(SEED_MANAGERS, start=1):
        conn.execute(
            """
            INSERT INTO managers(manager_id, name, cik, aliases, jurisdictions, tags)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                idx,
                manager["name"],
                manager["cik"],
                ",".join(manager["aliases"]),
                ",".join(manager["jurisdictions"]),
                ",".join(manager["tags"]),
            ),
        )

    conn.execute("""
        INSERT INTO filings(filing_id, manager_id, type, filed_date, period_end, source, raw_key)
        VALUES (1, 1, '13F-HR', '2026-03-15', '2025-12-31', 'synthetic', 'seed/elliott/13f')
        """)
    conn.executemany(
        """
        INSERT INTO holdings(
            holding_id, filing_id, manager_id, filed, name_of_issuer, cusip, shares, value_usd
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (1, 1, 1, "2026-03-15", "Synthetic Energy Holdings", "111111111", 1200, 4150000),
            (2, 1, 1, "2026-03-15", "Example Cloud Systems", "222222222", 840, 2250000),
        ],
    )
    conn.executemany(
        """
        INSERT INTO daily_diffs(
            manager_id, report_date, cusip, name_of_issuer, delta_type,
            shares_prev, shares_curr, value_prev, value_curr
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                1,
                "2026-03-15",
                "111111111",
                "Synthetic Energy Holdings",
                "INCREASE",
                900,
                1200,
                3000000,
                4150000,
            ),
            (1, "2026-03-15", "222222222", "Example Cloud Systems", "ADD", 0, 840, 0, 2250000),
        ],
    )
    conn.execute("""
        INSERT INTO news_items(
            news_id, manager_id, headline, url, published_at, source, topics, confidence, body_snippet
        )
        VALUES (
            1, 1, 'Elliott synthetic portfolio briefing ready',
            'https://example.invalid/synthetic-briefing',
            '2026-03-15T10:00:00', 'synthetic-fixture', 'readiness,demo', 0.98,
            'Readiness smoke deterministic fact: manager universe bootstrap is healthy.'
        )
        """)
    conn.execute("""
        INSERT INTO api_usage(source, endpoint, status, bytes, latency_ms, cost_usd)
        VALUES ('seed', 'offline-demo', 200, 0, 1, 0.0)
        """)


def build_wasm_demo(output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    """Create the static demo directory and return the SQLite asset path."""
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = output_dir / DEFAULT_DB_NAME
    if db_path.exists():
        db_path.unlink()

    old_db_path = os.environ.get("DB_PATH")
    old_simple = os.environ.get("USE_SIMPLE_EMBED")
    os.environ["DB_PATH"] = str(db_path)
    os.environ["USE_SIMPLE_EMBED"] = "1"
    try:
        with sqlite3.connect(db_path) as conn:
            _create_schema(conn)
            _seed_sqlite(conn)
            conn.commit()

        from embeddings import store_document

        store_document(
            READINESS_DOC_TEXT,
            db_path=str(db_path),
            kind="note",
            filename=READINESS_DOC_FILENAME,
        )
    finally:
        if old_db_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = old_db_path
        if old_simple is None:
            os.environ.pop("USE_SIMPLE_EMBED", None)
        else:
            os.environ["USE_SIMPLE_EMBED"] = old_simple

    for package_dir in ("ui", "api", "adapters", "utils"):
        target = output_dir / package_dir
        if target.exists():
            shutil.rmtree(target)

    for source in TEXT_BUNDLE_FILES:
        target = output_dir / source
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    _copy_static_shell(output_dir)
    _write_static_route_entrypoints(output_dir)
    return db_path


def main(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    output_dir = Path(argv[0]) if argv else DEFAULT_OUTPUT_DIR
    db_path = build_wasm_demo(output_dir)
    print(f"Built offline WASM demo at {output_dir} with SQLite asset {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
