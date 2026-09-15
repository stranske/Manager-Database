import importlib
import os
from unittest.mock import PropertyMock, patch

import pytest
from streamlit.testing.v1 import AppTest

APP_SOURCE = (
    "import streamlit as st\n"
    "from ui import require_login\n"
    "if require_login():\n"
    "    st.write('Protected content')\n"
    "else:\n"
    "    st.write('Access denied')\n"
)


@pytest.fixture
def configured_app(monkeypatch):
    monkeypatch.setenv("UI_USERNAME", "analyst")
    monkeypatch.setenv("UI_PASSWORD", "synthetic-test-password")
    monkeypatch.setenv("UI_COOKIE_KEY", "synthetic-independent-cookie-key-for-tests")
    # Other UI tests replace module globals; restore the real dependency boundary.
    importlib.reload(importlib.import_module("ui"))
    return AppTest.from_string(APP_SOURCE, default_timeout=15)


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


@pytest.mark.parametrize("authentication_status", [None, False])
def test_configured_login_does_not_trust_cached_dev_auth(configured_app, authentication_status):
    configured_app.session_state["auth"] = True
    configured_app.session_state["authentication_status"] = authentication_status
    app = configured_app.run()

    assert not app.exception
    assert app.session_state["auth"] is False
    assert app.markdown[-1].value == "Access denied"
    assert _button(app, "Login")


def test_require_login_fails_closed_without_dependency(configured_app, monkeypatch):
    app = configured_app.run()
    _submit(app)
    assert app.session_state["auth"] is True
    assert app.markdown[-1].value == "Protected content"

    ui = importlib.import_module("ui")
    monkeypatch.setattr(ui, "stauth", None)

    for _ in range(2):
        app.run()
        assert not app.exception
        assert app.session_state["auth"] is False
        assert app.markdown[-1].value == "Access denied"
        assert app.error[0].value == (
            "streamlit_authenticator is required when UI auth credentials are configured."
        )
        assert all(button.label != "Logout" for button in app.button)


def test_dev_mode_discards_previous_authenticated_session(configured_app, monkeypatch):
    app = configured_app.run()
    _submit(app)
    assert app.session_state["authentication_status"] is True

    username = os.environ["UI_USERNAME"]
    monkeypatch.delenv("UI_USERNAME")
    app.run()
    assert not app.exception
    assert app.markdown[-1].value == "Protected content"
    assert app.session_state["authentication_status"] is None
    assert app.session_state["username"] is None
    assert app.session_state["name"] is None

    monkeypatch.setenv("UI_USERNAME", username)
    app.run()
    assert not app.exception
    assert app.session_state["auth"] is False
    assert app.markdown[-1].value == "Access denied"
    assert _button(app, "Login")


@pytest.mark.parametrize("authenticated", [False, True])
@pytest.mark.parametrize("key", [None, "", " " * 32, "short-key"])
def test_configured_login_requires_cookie_secret(configured_app, monkeypatch, key, authenticated):
    app = configured_app.run()
    if authenticated:
        _submit(app)
        assert app.session_state["auth"] is True
        assert app.markdown[-1].value == "Protected content"

    if key is None:
        monkeypatch.delenv("UI_COOKIE_KEY", raising=False)
    else:
        monkeypatch.setenv("UI_COOKIE_KEY", key)
    # Invalid signing configuration must also revoke an already logged-in session.
    for _ in range(2):
        app.run()
        assert not app.exception
        assert app.session_state["auth"] is False
        assert app.markdown[-1].value == "Access denied"
        assert not app.text_input
        assert all(button.label != "Logout" for button in app.button)
        assert "UI_COOKIE_KEY" in app.error[0].value


def test_authenticator_receives_independent_secret_and_cached_hash(configured_app, monkeypatch):
    ui = importlib.import_module("ui")
    ui._password_hash.cache_clear()
    with (
        patch.object(ui.stauth.Hasher, "hash", wraps=ui.stauth.Hasher.hash) as hasher,
        patch.object(ui.stauth, "Authenticate", wraps=ui.stauth.Authenticate) as authenticate,
    ):
        app = configured_app.run()
        app.run()
        assert not app.exception
        assert hasher.call_count == 1
        first_hash = authenticate.call_args.args[0]["usernames"]["analyst"]["password"]
        assert first_hash.startswith("$2")
        assert authenticate.call_args.args[2] == "synthetic-independent-cookie-key-for-tests"
        assert authenticate.call_args.kwargs["auto_hash"] is False

        monkeypatch.setenv("UI_PASSWORD", "changed-synthetic-password")
        monkeypatch.setenv("UI_COOKIE_KEY", "rotated-independent-cookie-secret-for-tests")
        app.run()
        assert not app.exception
        assert hasher.call_count == 2
        assert authenticate.call_args.args[0]["usernames"]["analyst"]["password"] != first_hash
        assert authenticate.call_args.args[2] == "rotated-independent-cookie-secret-for-tests"
        assert authenticate.call_args.kwargs["auto_hash"] is False


def test_cookie_reauthentication_rejects_rotated_key(configured_app, monkeypatch):
    from extra_streamlit_components import CookieManager

    ui = importlib.import_module("ui")
    # Simulate browser transport only; signing and verification use the real library.
    with patch.object(CookieManager, "set") as set_cookie:
        app = configured_app.run()
        _submit(app)
        assert app.session_state["auth"] is True
        cookie_name, signed_cookie = set_cookie.call_args.args
        assert cookie_name == "mi_cookie"

    with patch.object(
        type(ui.st.context),
        "cookies",
        new_callable=PropertyMock,
        return_value={cookie_name: signed_cookie},
    ):
        returning_app = AppTest.from_string(APP_SOURCE, default_timeout=15).run()
        assert not returning_app.exception
        assert returning_app.session_state["auth"] is True
        assert returning_app.markdown[-1].value == "Protected content"
        assert not returning_app.text_input

        monkeypatch.setenv("UI_COOKIE_KEY", "rotated-independent-cookie-key-for-tests")
        fresh_app = AppTest.from_string(APP_SOURCE, default_timeout=15).run()
        assert not fresh_app.exception
        assert fresh_app.session_state["auth"] is False
        assert fresh_app.markdown[-1].value == "Access denied"
        assert _button(fresh_app, "Login")
        _submit(fresh_app)
        assert fresh_app.session_state["auth"] is True
        assert fresh_app.markdown[-1].value == "Protected content"


@pytest.mark.parametrize("field", ["UI_USERNAME", "UI_PASSWORD"])
@pytest.mark.parametrize("value", [None, "", "   "])
def test_require_login_preserves_blank_credential_dev_mode(
    configured_app, monkeypatch, field, value
):
    configured_value = os.environ[field]
    if value is None:
        monkeypatch.delenv(field, raising=False)
    else:
        monkeypatch.setenv(field, value)

    app = configured_app.run()
    assert not app.exception
    assert app.session_state["auth"] is True
    assert app.markdown[-1].value == "Protected content"
    assert len(app.text_input) == 0

    # Reconfiguring credentials must revoke the dev-mode flag on the next run.
    monkeypatch.setenv(field, configured_value)
    app.run()
    assert not app.exception
    assert app.session_state["auth"] is False
    assert app.markdown[-1].value == "Access denied"
    assert [widget.label for widget in app.text_input] == ["Username", "Password"]
    assert _button(app, "Login")
