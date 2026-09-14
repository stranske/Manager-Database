"""Shared UI utilities."""

from __future__ import annotations

import hashlib
import logging
import os

import streamlit as st

try:
    import streamlit_authenticator as stauth
except ImportError:  # pragma: no cover - optional UI dependency
    stauth = None

logger = logging.getLogger(__name__)


def _get_env_credential(key: str) -> str | None:
    value = os.getenv(key, "").strip()
    return value or None


def require_login() -> bool:
    """Render a simple login form and return authentication status."""
    username = _get_env_credential("UI_USERNAME")
    password = _get_env_credential("UI_PASSWORD")
    if not username or not password:
        logger.info("UI_USERNAME/UI_PASSWORD not set; skipping authentication in dev mode.")
        st.session_state["auth"] = True
        return True
    # The library owns configured authentication; a previous dev-mode flag is
    # not evidence of login. Clear it before rendering, including failure paths.
    st.session_state["auth"] = False
    if stauth is None:
        st.error("streamlit_authenticator is required when UI auth credentials are configured.")
        return False

    credentials = {"usernames": {username: {"name": username, "password": password}}}
    authenticator = stauth.Authenticate(
        credentials,
        "mi_cookie",
        # Sign cookies with the configured secret, not a public constant.
        hashlib.sha256(password.encode()).hexdigest(),
        cookie_expiry_days=1,
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
