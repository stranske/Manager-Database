"""Shared UI utilities."""

from __future__ import annotations

import os

import streamlit as st
import streamlit_authenticator as stauth


def _get_env_credential(key: str) -> str | None:
    value = os.getenv(key, "").strip()
    return value or None


def require_login() -> bool:
    """Render a simple login form and return authentication status."""
    if st.session_state.get("auth"):
        return True

    username = _get_env_credential("UI_USERNAME")
    password = _get_env_credential("UI_PASSWORD")
    if not username or not password:
        st.warning("UI_USERNAME/UI_PASSWORD not set; skipping authentication in dev mode.")
        st.session_state["auth"] = True
        return True

    names = [username]
    usernames = [username]
    passwords = stauth.Hasher([password]).generate()

    authenticator = stauth.Authenticate(
        {
            "usernames": usernames,
            "names": names,
            "passwords": passwords,
        },
        "mi_cookie",
        "auth",
        cookie_expiry_days=1,
    )
    name, auth_status, _ = authenticator.login("Login", "main")
    if auth_status:
        authenticator.logout("Logout", "sidebar")
        st.session_state["auth"] = True
        st.success(f"Welcome {name}!")
        return True
    elif auth_status is False:
        st.error("Invalid credentials")
    return False
