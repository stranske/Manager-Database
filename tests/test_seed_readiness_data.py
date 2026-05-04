from __future__ import annotations

from scripts import seed_readiness_data


def test_seed_readiness_data_seeds_manager_and_document():
    calls: list[str] = []

    def fake_seed_managers() -> int:
        calls.append("managers")
        return 2

    def fake_store_document(text: str, kind: str, filename: str) -> int:
        calls.append("document")
        assert text == seed_readiness_data.READINESS_DOC_TEXT
        assert kind == "note"
        assert filename == seed_readiness_data.READINESS_DOC_FILENAME
        return 42

    assert (
        seed_readiness_data.seed_readiness_data(
            seed_managers_fn=fake_seed_managers,
            store_document_fn=fake_store_document,
        )
        == 42
    )
    assert calls == ["managers", "document"]
