import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.parser import parseResponse


def test_parse_response_handles_malformed_json():
    result = parseResponse("{not-json")
    assert result.ok is False
    assert "Malformed JSON" in (result.error or "")

