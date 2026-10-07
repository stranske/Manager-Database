"""Keep native database values and legacy text safe at manager response boundaries."""

from api import managers


def test_array_decoder_preserves_native_values_and_rejects_wrong_json_shapes():
    assert managers._json_array(["Alpha", 42]) == ["Alpha", "42"]
    assert managers._json_array(None) == []
    assert managers._json_array(" \t ") == []
    assert managers._json_array('{"not": "an array"}') == []
    assert managers._json_array("42") == []
    assert managers._json_array(42) == []
    assert managers._json_array("Single alias") == ["Single alias"]
    assert managers._json_array('["Alpha", 42]') == ["Alpha", "42"]


def test_array_decoder_trims_semicolon_legacy_values_and_discards_empty_parts():
    assert managers._json_array(" ; Alpha, Inc. ; ; Beta ; ") == ["Alpha, Inc.", "Beta"]


def test_array_decoder_trims_comma_legacy_values_and_discards_empty_parts():
    assert managers._json_array(" , Alpha , , Beta , ") == ["Alpha", "Beta"]


def test_registry_decoder_converts_native_keys_and_values_without_accepting_invalid_shapes():
    assert managers._json_dict({7: 42, "lei": "ABC"}) == {"7": "42", "lei": "ABC"}
    assert managers._json_dict('{"7": 42, "lei": "ABC"}') == {"7": "42", "lei": "ABC"}
    for raw in (None, " \t ", "not json", "[]", "42", 42):
        assert managers._json_dict(raw) == {}


def test_quality_flags_decoder_retains_only_objects_and_rejects_invalid_shapes():
    flag = {"code": "missing-cik", "severity": "warning"}
    assert managers._json_object_list([flag, None, "invalid", 42, []]) == [flag]
    assert managers._json_object_list(
        '[{"code": "missing-cik", "severity": "warning"}, null, "invalid", 42, []]'
    ) == [flag]
    for raw in (None, " \t ", "not json", "{}", "42", 42):
        assert managers._json_object_list(raw) == []


def test_legacy_manager_row_keeps_timestamps_separate_from_quality_flags():
    row = (
        object(),
        "Legacy manager",
        None,
        None,
        "Alpha; Beta",
        '["UK"]',
        ["legacy"],
        {7: 42},
        "2026-10-01T01:02:03Z",
        "2026-10-02T04:05:06Z",
    )
    result = managers._to_manager_response(row)
    assert result.manager_id == 0
    assert result.name == "Legacy manager"
    assert result.cik is None and result.lei is None
    assert result.aliases == ["Alpha", "Beta"]
    assert result.jurisdictions == ["UK"]
    assert result.tags == ["legacy"]
    assert result.registry_ids == {"7": "42"}
    assert result.quality_flags == []
    assert result.created_at == row[8]
    assert result.updated_at == row[9]
