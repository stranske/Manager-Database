from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import httpx
import pytest

import adapters.edgar as edgar
from etl.logging_setup import reset_logging


class DummyClient:
    def __init__(self, responder):
        self._responder = responder

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, headers=None, params=None):
        return self._responder(url, params)


def _make_client(responder):
    return lambda: DummyClient(responder)


def _seed_managers(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT NOT NULL, cik TEXT)"
    )
    conn.execute(
        "INSERT INTO managers(manager_id, name, cik) VALUES (1, 'Elliott Investment Management', '0001791786')"
    )
    conn.commit()
    conn.close()


def _seed_alert_rule(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("""CREATE TABLE IF NOT EXISTS alert_rules (
            rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            event_type TEXT NOT NULL,
            condition_json TEXT NOT NULL,
            channels TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            manager_id INTEGER,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS alert_history (
            alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id INTEGER,
            rule_name TEXT NOT NULL,
            event_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            fired_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            delivered_channels TEXT NOT NULL,
            acknowledged INTEGER NOT NULL DEFAULT 0,
            acknowledged_by TEXT,
            acknowledged_at TIMESTAMP
        )""")
    conn.execute(
        """INSERT INTO alert_rules(name, event_type, condition_json, channels, enabled, manager_id)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            "Activism Watch",
            "activism_event",
            "{}",
            '["streamlit"]',
            1,
            None,
        ),
    )
    conn.commit()
    conn.close()


@pytest.mark.asyncio
async def test_parse_13d_extracts_core_fields():
    raw = Path("tests/data/sample_13d.txt").read_text()

    parsed = edgar.parse_13d(raw)

    assert parsed["subject_company"] == "Apple Inc."
    assert parsed["cusip"] == "037833100"
    assert parsed["ownership_pct"] == 5.1
    assert parsed["shares"] == 12345678
    assert parsed["group_members"] == ["Elliott Investment Management", "Elliott International"]
    assert "issuer is undervalued" in str(parsed["purpose_snippet"])
    assert parsed["filed_date"] is None
    assert parsed["event_date"] == "2024-02-14"


@pytest.mark.asyncio
async def test_parse_13g_extracts_core_fields():
    raw = Path("tests/data/sample_13g.txt").read_text()

    parsed = edgar.parse_13g(raw)

    assert parsed == {
        "subject_company": "Microsoft Corporation",
        "cusip": "594918104",
        "ownership_pct": 6.4,
        "shares": 8765432,
        "filed_date": "2024-02-20",
    }


@pytest.mark.parametrize(
    ("raw_text", "expected"),
    [
        ("Percent of Class: 5.1%", 5.1),
        ("Percent of Class: 5.1 %", 5.1),
        ("Percent of Class: 5.1", 5.1),
    ],
)
def test_parse_ownership_pct_variants(raw_text: str, expected: float):
    parsed = edgar.parse_13g(
        "\n".join(
            [
                "Name of Issuer",
                "Example Corp",
                "CUSIP Number",
                "123456789",
                "Amount Beneficially Owned: 100",
                raw_text,
            ]
        )
    )

    assert parsed["ownership_pct"] == expected


@pytest.mark.asyncio
async def test_list_new_filings_includes_13f_and_activism_forms(monkeypatch):
    submissions_payload = {
        "filings": {
            "recent": {
                "form": ["13F-HR", "10-K"],
                "filingDate": ["2024-05-01", "2024-05-02"],
                "accessionNumber": ["0001791786-24-000010", "0001791786-24-000011"],
            }
        }
    }
    efts_payload = {
        "hits": {
            "hits": [
                {
                    "_source": {
                        "formType": "SC 13D",
                        "filedAt": "2024-05-03T12:00:00Z",
                        "adsh": "0001791786-24-000012",
                        "primaryDocUrl": "/Archives/edgar/data/1791786/000179178624000012/d13d.htm",
                    }
                }
            ]
        }
    }

    def responder(url: str, params: dict[str, str] | None):
        if "submissions/CIK" in url:
            return httpx.Response(200, request=httpx.Request("GET", url), json=submissions_payload)
        if "search-index" in url:
            assert params is not None
            assert params["forms"] == "SC 13D,SC 13D/A,SC 13G,SC 13G/A"
            return httpx.Response(200, request=httpx.Request("GET", url), json=efts_payload)
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(edgar.httpx, "AsyncClient", _make_client(responder))

    filings = await edgar.list_new_filings(
        "0001791786",
        "2024-04-01",
        form_types=["13F-HR", "SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"],
    )

    assert filings == [
        {
            "accession": "0001791786-24-000010",
            "cik": "0001791786",
            "filed": "2024-05-01",
        },
        {
            "accession": "0001791786-24-000012",
            "cik": "0001791786",
            "filed": "2024-05-03",
            "form": "SC 13D",
            "url": "/Archives/edgar/data/1791786/000179178624000012/d13d.htm",
        },
    ]


@pytest.mark.asyncio
async def test_parse_dispatches_activism_forms():
    raw = Path("tests/data/sample_13d.txt").read_text()

    parsed = await edgar.parse(raw, form_type="SC 13D")

    assert isinstance(parsed, dict)
    assert parsed["cusip"] == "037833100"


@pytest.mark.asyncio
async def test_fetch_activism_filings_stores_rows_and_raw_documents(monkeypatch, tmp_path):
    reset_logging()
    import etl.activism_flow as activism_flow

    db_path = tmp_path / "activism.db"
    _seed_managers(db_path)
    _seed_alert_rule(db_path)
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(activism_flow, "DB_PATH", str(db_path))

    sample_13d = Path("tests/data/sample_13d.txt").read_text()
    sample_13g = Path("tests/data/sample_13g.txt").read_text()

    async def fake_list_new_filings(cik, since, form_types=None, manager_name=None):
        assert cik == "0001791786"
        assert form_types == activism_flow.ACTIVISM_FORMS
        assert manager_name == "Elliott Investment Management"
        return [
            {
                "accession": "0001791786-24-000012",
                "cik": cik,
                "filed": "2024-05-03",
                "form": "SC 13D",
                "url": "/Archives/edgar/data/1791786/000179178624000012/d13d.htm",
            },
            {
                "accession": "0001791786-24-000013",
                "cik": cik,
                "filed": "2024-05-04",
                "form": "SC 13G",
                "url": "/Archives/edgar/data/1791786/000179178624000013/g13g.htm",
            },
        ]

    async def fake_download(filing):
        if filing["form"] == "SC 13D":
            return sample_13d
        return sample_13g

    put_calls: list[dict[str, object]] = []

    def fake_put_object(**kwargs):
        put_calls.append(kwargs)

    monkeypatch.setattr(activism_flow.edgar, "list_new_filings", fake_list_new_filings)
    monkeypatch.setattr(activism_flow.edgar, "download", fake_download)
    monkeypatch.setattr(activism_flow.S3, "put_object", fake_put_object)

    rows = await activism_flow.fetch_activism_filings.fn(1, "2024-04-01")

    assert len(rows) == 2
    assert {row["filing_type"] for row in rows} == {"SC 13D", "SC 13G"}
    expected_prefix = hashlib.sha256(sample_13d.encode("utf-8")).hexdigest()[:16]
    assert put_calls[0]["Key"] == f"raw/activism/{expected_prefix}_0001791786-24-000012.txt"

    conn = sqlite3.connect(db_path)
    stored_rows = conn.execute(
        "SELECT manager_id, filing_type, subject_company, subject_cusip, filed_date FROM activism_filings ORDER BY filing_id"
    ).fetchall()
    stored_events = conn.execute(
        "SELECT filing_id, event_type, threshold_crossed FROM activism_events ORDER BY filing_id, event_type, threshold_crossed"
    ).fetchall()
    alert_history = conn.execute(
        "SELECT rule_name, event_type FROM alert_history ORDER BY alert_id"
    ).fetchall()
    conn.close()
    assert stored_rows == [
        (1, "SC 13D", "Apple Inc.", "037833100", "2024-05-03"),
        (1, "SC 13G", "Microsoft Corporation", "594918104", "2024-05-04"),
    ]
    assert stored_events == [
        (1, "group_formation", None),
        (1, "initial_stake", None),
        (1, "threshold_crossing", 5.0),
        (2, "initial_stake", None),
        (2, "threshold_crossing", 5.0),
    ]
    assert alert_history == [
        ("Activism Watch", "activism_event"),
        ("Activism Watch", "activism_event"),
        ("Activism Watch", "activism_event"),
        ("Activism Watch", "activism_event"),
        ("Activism Watch", "activism_event"),
    ]
    reset_logging()
