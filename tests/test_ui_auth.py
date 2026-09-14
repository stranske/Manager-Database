import importlib
from types import SimpleNamespace

import pytest
from streamlit.testing.v1 import AppTest


@pytest.fixture
def configured_app(monkeypatch):
    monkeypatch.setenv("UI_USERNAME", "analyst")
    monkeypatch.setenv("UI_PASSWORD", "synthetic-test-password")
    # Other UI tests replace module globals; restore the real dependency boundary.
    importlib.reload(importlib.import_module("ui"))
    return AppTest.from_string(
        "import streamlit as st\n"
        "from ui import require_login\n"
        "if require_login():\n"
        "    st.write('Protected content')\n"
        "else:\n"
        "    st.write('Access denied')\n",
        default_timeout=15,
    )


def _button(app, label):
    return next(button for button in app.button if button.label == label)


def _submit(app, username="analyst", password="synthetic-test-password"):
    app.text_input[0].set_value(username)
    app.text_input[1].set_value(password)
    _button(app, "Login").click().run()
    assert not app.exception


def test_configured_login_renders_with_real_authenticator(configured_app):
    app = configured_app.run()

    assert not app.exception
    assert [field.label for field in app.text_input] == ["Username", "Password"]
    assert _button(app, "Login")
    assert app.session_state["auth"] is False
    assert app.markdown[-1].value == "Access denied"


@pytest.mark.parametrize(
    ("username", "password"),
    [("analyst", "wrong-password"), ("unknown", "synthetic-test-password")],
)
def test_configured_login_rejects_invalid_credentials(configured_app, username, password):
    app = configured_app.run()
    _submit(app, username, password)

    assert app.session_state["auth"] is False
    assert app.session_state["authentication_status"] is False
    assert app.error[0].value == "Invalid credentials"
    assert app.markdown[-1].value == "Access denied"
    assert all(button.label != "Logout" for button in app.button)


def test_valid_login_rerun_and_logout(configured_app):
    app = configured_app.run()
    _submit(app)

    assert app.session_state["auth"] is True
    assert app.session_state["authentication_status"] is True
    assert app.markdown[-1].value == "Protected content"
    assert _button(app, "Logout")

    app.run()
    assert not app.exception
    assert app.markdown[-1].value == "Protected content"
    assert len(app.text_input) == 0
    _button(app, "Logout").click().run()

    assert not app.exception
    assert app.session_state["auth"] is False
    assert app.session_state["authentication_status"] is None
    assert app.markdown[-1].value == "Access denied"
    app.run()
    assert not app.exception
    assert app.markdown[-1].value == "Access denied"
    assert _button(app, "Login")


def test_configured_login_does_not_trust_cached_dev_auth(configured_app):
    configured_app.session_state["auth"] = True
    app = configured_app.run()

    assert not app.exception
    assert app.session_state["auth"] is False
    assert app.markdown[-1].value == "Access denied"
    assert _button(app, "Login")


def test_require_login_fails_closed_without_dependency(monkeypatch):
    ui = importlib.reload(importlib.import_module("ui"))
    monkeypatch.setenv("UI_USERNAME", "analyst")
    monkeypatch.setenv("UI_PASSWORD", "synthetic-test-password")
    errors = []

    fake_st = SimpleNamespace(session_state={"auth": True}, error=errors.append)
    monkeypatch.setattr(ui, "st", fake_st)
    monkeypatch.setattr(ui, "stauth", None)

    assert ui.require_login() is False
    assert fake_st.session_state["auth"] is False
    assert errors == [
        "streamlit_authenticator is required when UI auth credentials are configured."
    ]


@pytest.mark.parametrize("password", [None, "   "])
def test_require_login_preserves_blank_credential_dev_mode(monkeypatch, password):
    ui = importlib.reload(importlib.import_module("ui"))

    fake_st = SimpleNamespace(session_state={})
    monkeypatch.setattr(ui, "st", fake_st)
    monkeypatch.setenv("UI_USERNAME", "analyst")
    if password is None:
        monkeypatch.delenv("UI_PASSWORD", raising=False)
    else:
        monkeypatch.setenv("UI_PASSWORD", password)

    assert ui.require_login() is True
    assert fake_st.session_state["auth"] is True
