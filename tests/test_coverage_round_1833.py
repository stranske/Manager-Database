"""Focused regression coverage for coverage-autopilot issue #1833."""

import sys
from datetime import date

import pytest

import adapters.edgar as edgar
import adapters.news as news
from api import activism


def test_edgar_normalizes_efts_and_13f_table_variants():
    """SEC's two search payload shapes must retain usable filing metadata."""
    table = [
        {
            "Issuer": "Example Corp",
            "Cusip": "123456789",
            "Value": "1,250.9",
            "SharesPrnAmount": "42",
        },
        {"name": "Fallback", "CUSIP": "987", "value": None, "shares": ""},
    ]

    assert edgar._normalize_13f_rows_from_table(table) == [
        {"nameOfIssuer": "Example Corp", "cusip": "123456789", "value": 1250, "sshPrnamt": 42},
        {"nameOfIssuer": "Fallback", "cusip": "987", "value": 0, "sshPrnamt": 0},
    ]
    assert edgar._extract_efts_hits({"hits": {"hits": [{"_source": {}}]}}) == [{"_source": {}}]
    assert edgar._extract_efts_hits({"filings": [{"form": "SC 13D"}]}) == [{"form": "SC 13D"}]
    assert edgar._efts_hit_to_filing(
        {
            "_source": {
                "formType": "SC 13D",
                "filedAt": "2026-02-03T12:00:00Z",
                "accessionNo": "0000123456-26-000789",
                "primaryDocUrl": "/Archives/doc.htm",
            }
        },
        "1234",
    ) == {
        "accession": "0000123456-26-000789",
        "cik": "1234",
        "filed": "2026-02-03",
        "form": "SC 13D",
        "url": "/Archives/doc.htm",
    }


def test_edgar_parses_activism_metadata_and_deduplicates_filings():
    raw = """
        Name of Issuer: Example Holdings
        CUSIP Number: 123 456 789
        FILED AS OF DATE: 20260131
        Date of Event Which Requires Filing of this Statement: 2026-01-30
        Percent of Class: 12.5%
        Amount Beneficially Owned: 1,200
        ITEM 2. Reporting Persons: Alpha LP; Beta LLC and Gamma
        ITEM 4. Seek board representation and discuss a strategic review.
        SIGNATURE
    """

    parsed = edgar.parse_13d(raw)
    assert parsed["subject_company"] == "Example Holdings"
    assert parsed["cusip"] == "123456789"
    assert parsed["ownership_pct"] == 12.5
    assert parsed["shares"] == 1200
    assert parsed["group_members"] == ["Alpha LP", "Beta LLC", "Gamma"]
    assert parsed["event_date"] == "2026-01-30"
    assert edgar._unique_filings(
        [
            {"accession": "one", "url": "", "form": "13-F"},
            {"accession": "one", "url": "", "form": "13F-HR"},
            {"accession": "two", "url": "", "form": "SC 13G"},
        ]
    ) == [
        {"accession": "one", "url": "", "form": "13-F"},
        {"accession": "two", "url": "", "form": "SC 13G"},
    ]


def test_news_configuration_and_entity_helpers_reject_invalid_values(monkeypatch):
    monkeypatch.setenv("NEWS_RSS_FEEDS", " , https://example.test/feed , ")
    monkeypatch.setenv("NEWS_GDELT_MANAGERS", " Alpha , ,Beta ")
    monkeypatch.setenv("NEWS_TOPIC_KEYWORDS", "not-json")
    monkeypatch.setenv("NEWS_ENTITY_LABELS", " ORG, ticker ")
    monkeypatch.setenv("NEWS_ENTITY_CONFIDENCE_THRESHOLD", "not-a-number")
    monkeypatch.setenv("NEWS_ENTITY_TAGGING_MODE", "unsupported")

    assert news._configured_rss_feeds() == ["https://example.test/feed"]
    assert news._configured_gdelt_managers() == ["Alpha", "Beta"]
    assert news._configured_topic_keywords() == news.TOPIC_KEYWORDS
    assert news._configured_entity_labels() == ("org", "ticker")
    assert news._configured_model_confidence_threshold() == 0.55
    assert news.configured_entity_tagging_mode() == "keyword"
    assert news._entry_timestamp({"published": "not a date"}) is None
    assert news._gdelt_timestamp("not-a-date") is None


@pytest.mark.asyncio
async def test_news_dispatches_sources_and_avoids_optional_model_download(monkeypatch):
    async def fake_rss(since):
        return [{"source": "rss", "published_at": since}]

    async def fake_gdelt(since):
        return [{"source": "gdelt", "published_at": since}]

    monkeypatch.setattr(news, "_fetch_rss", fake_rss)
    monkeypatch.setattr(news, "_fetch_gdelt", fake_gdelt)
    monkeypatch.setitem(sys.modules, "gliner", None)
    news._GLINER_MODEL_CACHE.clear()

    assert await news.list_new_items("sec_press", None) == [
        {"source": "rss", "published_at": "1970-01-01T00:00:00+00:00"}
    ]
    assert await news.list_new_items("gdelt", "2026-01-01T00:00:00Z") == [
        {"source": "gdelt", "published_at": "2026-01-01T00:00:00Z"}
    ]
    with pytest.raises(ValueError, match="Unsupported news source"):
        await news.list_new_items("unknown", None)
    assert news._predict_model_entities("Alpha", ["organization"]) == []


def test_activism_coercion_and_absent_tables_are_safe(monkeypatch):
    assert activism._to_float("inf") is None
    assert activism._to_float("bad") is None
    assert activism._to_int("bad") is None
    assert activism._to_date("2026-02-03") == date(2026, 2, 3)
    assert activism._json_strings('["one", 2]') == ["one", "2"]
    assert activism._json_strings("not json") == []

    conn = object()
    monkeypatch.setattr(activism, "table_exists", lambda _conn, _table: False)
    assert activism.query_activism_filings(conn) == []
    assert activism.query_activism_events(conn) == []
    assert activism.query_activism_campaigns(conn) == []
    assert activism.query_activism_campaign_timeline(conn, 1) == []
    assert activism.query_activism_documents(conn, 1) == []
    assert activism.query_manager_activism_profile(conn, 1) is None
