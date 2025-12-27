import streamlit as st

from adapters.base import connect_db

from . import require_login


def save_note(content: str, filename: str) -> None:
    conn = connect_db()
    conn.execute(
        "CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY AUTOINCREMENT, filename TEXT, content TEXT)"
    )
    conn.execute(
        "INSERT INTO notes (filename, content) VALUES (?, ?)",
        (filename, content),
    )
    conn.commit()
    conn.close()


def main() -> None:
    if not require_login():
        st.stop()
    st.header("Upload Memo")
    uploaded = st.file_uploader("Upload text/markdown file", type=["txt", "md"])
    if uploaded and st.button("Save"):
        content = uploaded.getvalue().decode("utf-8")
        save_note(content, uploaded.name)
        st.success("Uploaded")


if __name__ == "__main__":
    main()
