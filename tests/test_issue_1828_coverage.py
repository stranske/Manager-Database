"""Focused EDGAR adapter regressions for the ranked #1828 coverage round."""

from __future__ import annotations

from decimal import Decimal

from adapters import edgar


def test_edgar_normalizers_accept_sec_shapes_and_reject_poisoned_pacing(monkeypatch):
    monkeypatch.setattr(
        edgar, "logger", type("Logger", (), {"warning": lambda *_args, **_kwargs: None})()
    )

    assert edgar._parse_edgar_min_request_interval("0.25") == 0.25
    assert edgar._parse_edgar_min_request_interval("not-a-number") == 0.11
    assert edgar._normalize_form_type(" 13-f ") == "13F-HR"
    assert edgar._is_13f_form("13F-HR/A")
    assert edgar._is_activism_form("sc 13d/a")
    assert edgar._parse_int_field(None) == 0
    assert edgar._parse_int_field(True) == 1
    assert edgar._parse_int_field(Decimal("12.9")) == 12
    assert edgar._parse_int_field("1,234.9") == 1234


def test_edgar_table_and_sec_metadata_normalization_preserves_contract():
    class FakeTable:
        @staticmethod
        def to_dict(*, orient):
            assert orient == "records"
            return [
                {"Issuer": "Alpha", "Cusip": "001", "Value": "1,000", "Shares": "25"},
                {"nameOfIssuer": "Beta", "CUSIP": "002", "value": None, "sshPrnamt": None},
                "not-a-row",
            ]

    assert edgar._normalize_13f_rows_from_table(FakeTable()) == [
        {"nameOfIssuer": "Alpha", "cusip": "001", "value": 1000, "sshPrnamt": 25},
        {"nameOfIssuer": "Beta", "cusip": "002", "value": 0, "sshPrnamt": 0},
    ]
    assert edgar._normalize_filed_date("filed 20240506") == "2024-05-06"
    assert edgar._normalize_filed_date("unknown") is None
    assert edgar._extract_accession("0000123456-24-000789") == "0000123456-24-000789"
    assert edgar._extract_accession("000012345624000789") == "0000123456-24-000789"


def test_edgar_efts_hit_extraction_filters_invalid_records_and_deduplicates():
    payload = {
        "hits": {
            "hits": [
                {
                    "_source": {
                        "formType": "SC 13D",
                        "filedAt": "2024-05-06T12:00:00Z",
                        "adsh": "0000123456-24-000789",
                        "primaryDocUrl": "/Archives/filing.htm",
                    }
                },
                {"_source": {"formType": "SC 13G"}},
                "invalid",
            ]
        }
    }

    hits = edgar._extract_efts_hits(payload)
    filing = edgar._efts_hit_to_filing(hits[0], "123")
    assert filing == {
        "accession": "0000123456-24-000789",
        "cik": "123",
        "filed": "2024-05-06",
        "form": "SC 13D",
        "url": "/Archives/filing.htm",
    }
    assert edgar._efts_hit_to_filing(hits[1], "123") is None
    assert edgar._unique_filings([filing, filing, {**filing, "form": "SC 13G"}]) == [
        filing,
        {**filing, "form": "SC 13G"},
    ]


def test_edgar_text_helpers_parse_activism_fields_and_safe_empty_values():
    raw = """Name of Issuer: Example Holdings
CUSIP Number: 123 456 789
Percent of Class: 7.5%
Amount Beneficially Owned: 1,250
FILED AS OF DATE: 20240506
ITEM 2. Reporting Persons: Alpha Capital and Beta Partners
ITEM 4. The reporting persons intend to discuss strategic alternatives.
SIGNATURE
"""

    parsed = edgar.parse_13d(raw)
    assert parsed["subject_company"] == "Example Holdings"
    assert parsed["cusip"] == "123456789"
    assert parsed["ownership_pct"] == 7.5
    assert parsed["shares"] == 1250
    assert parsed["group_members"] == ["Alpha Capital", "Beta Partners"]
    assert parsed["filed_date"] == "2024-05-06"
    assert edgar._extract_label_value("Name of Issuer\n\nValue", "Name of Issuer") == "Value"
    assert edgar._parse_float("not a percent") is None
    assert edgar._parse_int("no digits") is None
