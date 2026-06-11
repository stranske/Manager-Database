import asyncio
from pathlib import Path

import pytest

from adapters import edgar

SNAPSHOT_DIR = Path(__file__).parent / "fixtures" / "filing_snapshots"


@pytest.mark.nightly
def test_edgar_parser_retained_prior_snapshot_fixture():
    raw = (SNAPSHOT_DIR / "edgar_13f_prior_snapshot.xml").read_text()

    rows = asyncio.run(edgar.parse(raw))

    assert rows == [
        {
            "nameOfIssuer": "Example Corp",
            "cusip": "123456789",
            "value": 1000,
            "sshPrnamt": 100,
        },
        {
            "nameOfIssuer": "Sample Holdings Inc",
            "cusip": "987654321",
            "value": 2500,
            "sshPrnamt": 45,
        },
    ]
