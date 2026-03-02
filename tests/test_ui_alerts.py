from ui import alerts as alerts_ui


def test_load_managers_pages_and_sorts_results(monkeypatch):
    calls: list[tuple[str, str, dict]] = []

    def _fake_api_request(method: str, path: str, *, params=None, json_body=None):
        calls.append((method, path, params or {}))
        offset = int((params or {}).get("offset", 0))
        if offset == 0:
            return True, {
                "items": [{"id": 2, "name": "Zed"}, {"id": 1, "name": "Amy"}],
                "total": 3,
            }
        return True, {"items": [{"id": 3, "name": "Bob"}], "total": 3}

    monkeypatch.setattr(alerts_ui, "_api_request", _fake_api_request)
    alerts_ui._load_managers.clear()

    managers = alerts_ui._load_managers()

    assert managers == [(1, "Amy"), (3, "Bob"), (2, "Zed")]
    assert calls == [
        ("GET", "/managers", {"limit": 100, "offset": 0}),
        ("GET", "/managers", {"limit": 100, "offset": 2}),
    ]


def test_load_managers_returns_empty_on_api_error(monkeypatch):
    monkeypatch.setattr(
        alerts_ui, "_api_request", lambda *args, **kwargs: (False, {"detail": "boom"})
    )
    alerts_ui._load_managers.clear()

    assert alerts_ui._load_managers() == []
