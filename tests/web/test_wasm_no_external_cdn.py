from __future__ import annotations

import re
from html.parser import HTMLParser
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
INDEX_HTML = REPO_ROOT / "web" / "index.html"
EXTERNAL_URL = re.compile(r"https?://", re.IGNORECASE)
MODULE_IMPORT_URL = re.compile(
    r"""import\s+(?:[^'"]+\s+from\s+)?["'](?P<url>https?://[^"']+)["']""",
    re.IGNORECASE,
)


class RuntimeReferenceParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.references: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {name.lower(): value or "" for name, value in attrs}
        for attr in ("src", "href"):
            value = attr_map.get(attr)
            if value:
                self.references.append((f"{tag}[{attr}]", value))


def _runtime_references() -> list[tuple[str, str]]:
    parser = RuntimeReferenceParser()
    parser.feed(INDEX_HTML.read_text(encoding="utf-8"))
    return parser.references


def test_wasm_index_has_no_external_runtime_ref() -> None:
    html = INDEX_HTML.read_text(encoding="utf-8")
    external_imports = [match.group("url") for match in MODULE_IMPORT_URL.finditer(html)]
    external_tags = [
        f"{location}={value}"
        for location, value in _runtime_references()
        if EXTERNAL_URL.search(value)
    ]

    assert external_imports == []
    assert external_tags == []


def test_wasm_index_local_runtime_assets_exist() -> None:
    html = INDEX_HTML.read_text(encoding="utf-8")
    local_refs = [
        "./vendor/stlite/browser-0.80.4/build/stlite.js",
        "./vendor/pyodide/v0.27.3/full/pyodide.mjs",
        "./vendor/stlite/browser-0.80.4/build/wheels/stlite_lib-0.1.0-py3-none-any.whl",
        "./vendor/stlite/browser-0.80.4/build/wheels/streamlit-1.41.0-cp312-none-any.whl",
    ]

    for reference in local_refs:
        assert reference in html
        assert (INDEX_HTML.parent / reference).resolve().exists()
