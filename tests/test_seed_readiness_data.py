from __future__ import annotations

from scripts import seed_readiness_data


def test_seed_readiness_data_seeds_manager_and_document(monkeypatch):
    calls: list[str] = []

    monkeypatch.delenv("USE_SIMPLE_EMBED", raising=False)

    def fake_seed_managers() -> int:
        calls.append("managers")
        return 2

    def fake_resolve_manager_id(cik: str) -> int | None:
        calls.append("resolve")
        assert cik == seed_readiness_data.READINESS_MANAGER_CIK
        return 7

    def fake_store_document(
        text: str,
        manager_id: int | None,
        kind: str,
        filename: str,
    ) -> int:
        calls.append("document")
        assert text == seed_readiness_data.READINESS_DOC_TEXT
        assert manager_id == 7
        assert kind == "note"
        assert filename == seed_readiness_data.READINESS_DOC_FILENAME
        return 42

    assert (
        seed_readiness_data.seed_readiness_data(
            seed_managers_fn=fake_seed_managers,
            store_document_fn=fake_store_document,
            resolve_manager_id_fn=fake_resolve_manager_id,
        )
        == 42
    )
    assert calls == ["managers", "resolve", "document"]
    assert seed_readiness_data.os.environ["USE_SIMPLE_EMBED"] == "1"


def test_seed_readiness_data_allows_missing_manager_id():
    def fake_store_document(
        text: str,
        manager_id: int | None,
        kind: str,
        filename: str,
    ) -> int:
        assert manager_id is None
        return 9

    assert (
        seed_readiness_data.seed_readiness_data(
            seed_managers_fn=lambda: 1,
            store_document_fn=fake_store_document,
            resolve_manager_id_fn=lambda cik: None,
        )
        == 9
    )
