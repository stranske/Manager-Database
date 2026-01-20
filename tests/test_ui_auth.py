import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.append(str(Path(__file__).resolve().parents[1]))


class FakeStreamlit:
    def __init__(self, session_state=None):
        self.session_state = session_state or {}
        self.success_messages = []
        self.error_messages = []
        self.warning_messages = []

    def success(self, message: str) -> None:
        self.success_messages.append(message)

    def error(self, message: str) -> None:
        self.error_messages.append(message)

    def warning(self, message: str) -> None:
        self.warning_messages.append(message)


class FakeHasher:
    def __init__(self, _passwords):
        pass

    def generate(self):
        return ["hashed-pass"]


class FakeAuthenticator:
    def __init__(self, auth_status: bool | None, name: str = "Analyst"):
        self.auth_status = auth_status
        self.name = name
        self.logout_called = False

    def login(self, _label: str, _location: str):
        return self.name, self.auth_status, None

    def logout(self, _label: str, _location: str) -> None:
        self.logout_called = True


def _load_ui_module():
    # Reload to ensure each test starts with a clean module state.
    return importlib.reload(importlib.import_module("ui"))


def test_require_login_short_circuits_when_authenticated(monkeypatch):
    ui = _load_ui_module()
    fake_st = FakeStreamlit(session_state={"auth": True})

    # Ensure no auth UI is invoked when already authenticated.
    def exploding_auth(*_args, **_kwargs):
        raise AssertionError("Authenticate should not be called")

    monkeypatch.setattr(ui, "st", fake_st)
    monkeypatch.setattr(
        ui,
        "stauth",
        SimpleNamespace(Authenticate=exploding_auth, Hasher=FakeHasher),
    )

    assert ui.require_login() is True
    assert fake_st.success_messages == []
    assert fake_st.error_messages == []


def test_require_login_sets_session_on_success(monkeypatch):
    ui = _load_ui_module()
    fake_st = FakeStreamlit()
    authenticator = FakeAuthenticator(auth_status=True, name="Avery")
    monkeypatch.setenv("UI_USERNAME", "analyst")
    monkeypatch.setenv("UI_PASSWORD", "pass")
    monkeypatch.setattr(ui, "st", fake_st)
    monkeypatch.setattr(
        ui,
        "stauth",
        SimpleNamespace(
            Authenticate=lambda *args, **kwargs: authenticator,
            Hasher=FakeHasher,
        ),
    )

    assert ui.require_login() is True
    assert fake_st.session_state["auth"] is True
    assert authenticator.logout_called is True
    assert fake_st.success_messages == ["Welcome Avery!"]


def test_require_login_handles_invalid_credentials(monkeypatch):
    ui = _load_ui_module()
    fake_st = FakeStreamlit()
    authenticator = FakeAuthenticator(auth_status=False)
    monkeypatch.setenv("UI_USERNAME", "analyst")
    monkeypatch.setenv("UI_PASSWORD", "pass")
    monkeypatch.setattr(ui, "st", fake_st)
    monkeypatch.setattr(
        ui,
        "stauth",
        SimpleNamespace(
            Authenticate=lambda *args, **kwargs: authenticator,
            Hasher=FakeHasher,
        ),
    )

    assert ui.require_login() is False
    assert fake_st.error_messages == ["Invalid credentials"]


def test_require_login_skips_auth_when_credentials_missing(monkeypatch):
    ui = _load_ui_module()
    fake_st = FakeStreamlit()
    monkeypatch.delenv("UI_USERNAME", raising=False)
    monkeypatch.delenv("UI_PASSWORD", raising=False)
    monkeypatch.setattr(ui, "st", fake_st)

    assert ui.require_login() is True
    assert fake_st.session_state["auth"] is True
    assert fake_st.warning_messages == [
        "UI_USERNAME/UI_PASSWORD not set; skipping authentication in dev mode."
    ]
