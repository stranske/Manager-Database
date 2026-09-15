"""Shared UI utilities."""

from __future__ import annotations

import logging
import os
from functools import lru_cache

import streamlit as st

try:
    import streamlit_authenticator as stauth
except ImportError:  # pragma: no cover - optional UI dependency
    stauth = None

logger = logging.getLogger(__name__)


def _get_env_credential(key: str) -> str | None:
    value = os.getenv(key, "").strip()
    return value or None


@lru_cache(maxsize=1)
def _password_hash(password: str) -> str:
    """Reuse bcrypt work across reruns, replacing it when the password changes."""
    return str(stauth.Hasher.hash(password))


def require_login() -> bool:
    """Render a simple login form and return authentication status."""
    username = _get_env_credential("UI_USERNAME")
    password = _get_env_credential("UI_PASSWORD")
    if not username or not password:
        logger.info("UI_USERNAME/UI_PASSWORD not set; skipping authentication in dev mode.")
        # Development access must not preserve a previous configured identity.
        # Restoring credentials requires authentication again on the next rerun.
        for field in ("authentication_status", "username", "name"):
            st.session_state[field] = None
        st.session_state["auth"] = True
        return True
    # The library owns configured authentication; a previous dev-mode flag is
    # not evidence of login. Clear it before rendering, including failure paths.
    st.session_state["auth"] = False
    if stauth is None:
        st.error("streamlit_authenticator is required when UI auth credentials are configured.")
        return False

    cookie_key = _get_env_credential("UI_COOKIE_KEY")
    if not cookie_key or len(cookie_key) < 32:
        st.error("UI_COOKIE_KEY must be an independent random secret of at least 32 characters.")
        return False

    credentials = {
        "usernames": {username: {"name": username, "password": _password_hash(password)}}
    }
    authenticator = stauth.Authenticate(
        credentials,
        "mi_cookie",
        cookie_key,
        cookie_expiry_days=1,
        auto_hash=False,
    )
    # Rendered login returns None in 0.4.x; status lives in session_state.
    authenticator.login(location="main")
    if st.session_state.get("authentication_status") is True:
        authenticator.logout("Logout", "sidebar")
        # Clicking logout changes library state during this very render.
        if st.session_state.get("authentication_status") is True:
            st.session_state["auth"] = True
            st.success(f"Welcome {st.session_state.get('name') or username}!")
            return True
    elif st.session_state.get("authentication_status") is False:
        st.error("Invalid credentials")
    return False
