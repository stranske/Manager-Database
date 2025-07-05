"""Shared UI utilities."""

from __future__ import annotations

import streamlit as st
import streamlit_authenticator as stauth


def require_login() -> bool:
    """Render a simple login form and return authentication status."""
    if st.session_state.get("auth"):
        return True

    names = ["analyst"]
    usernames = ["analyst"]
    passwords = stauth.Hasher(["pass"]).generate()

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
