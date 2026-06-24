from __future__ import annotations

import re
from html.parser import HTMLParser
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = REPO_ROOT / "web"
INDEX_HTML = WEB_DIR / "index.html"
DESIGN_SYSTEM_DIR = WEB_DIR / "design-system"
PYODIDE_VENDOR = WEB_DIR / "vendor" / "pyodide" / "v0.27.3" / "full"
STLITE_WHEELS = WEB_DIR / "vendor" / "stlite" / "browser-0.80.4" / "build" / "wheels"

EXTERNAL_URL = re.compile(r"^(?:https?:)?//", re.IGNORECASE)
MODULE_IMPORT_URL = re.compile(
    r"""(?:import\s+(?:[^'"]+\s+from\s+)?|import\s*\()\s*["'](?P<url>https?://[^"']+)["']\)?""",
    re.IGNORECASE,
)
PYODIDE_URL = re.compile(
    r"""pyodideUrl:\s*new URL\("(?P<url>[^"]+)"\s*,\s*window\.location\.href\)""",
)
WHEEL_URL = re.compile(
    r"""(?:stliteLib|streamlit):\s*new URL\("(?P<url>[^"']+)["']\s*,\s*window\.location\.href\)""",
)

PYODIDE_CORE_FILES = (
    "pyodide.mjs",
    "pyodide.asm.wasm",
    "python_stdlib.zip",
    "pyodide-lock.json",
)
PYODIDE_WHEEL_GLOBS = (
    "pandas-*.whl",
    "altair-*.whl",
    "httpx-*.whl",
    "sqlite3-*.whl",
    "pillow-*.whl",
    "protobuf-*.whl",
    "micropip-*.whl",
)
STLITE_WHEEL_GLOBS = ("streamlit-*.whl", "stlite_lib-*.whl")


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


def _assert_non_empty_file(path: Path, *, label: str) -> None:
    assert path.is_file(), f"expected {label} at {path}"
    assert path.stat().st_size > 0, f"expected non-empty {label} at {path}"


def _glob_one(directory: Path, pattern: str) -> Path:
    matches = sorted(directory.glob(pattern))
    assert matches, f"expected at least one {pattern} under {directory}"
    assert len(matches) == 1, (
        f"expected exactly one {pattern} under {directory}, found {len(matches)}: "
        f"{', '.join(match.name for match in matches)}"
    )
    return matches[0]


def test_wasm_index_has_no_external_cdn_or_runtime_urls() -> None:
    html = INDEX_HTML.read_text(encoding="utf-8")

    assert "cdn.jsdelivr.net" not in html, (
        "web/index.html must not reference cdn.jsdelivr.net for offline boot"
    )

    external_imports = [match.group("url") for match in MODULE_IMPORT_URL.finditer(html)]
    assert external_imports == [], (
        f"web/index.html must not import modules from external http(s) URLs: {external_imports}"
    )

    external_tags = [
        f"{location}={value}"
        for location, value in _runtime_references()
        if EXTERNAL_URL.search(value)
    ]
    assert external_tags == [], (
        "web/index.html must not load scripts or stylesheets from external http(s) URLs: "
        f"{external_tags}"
    )


def test_wasm_index_links_local_design_system_styles() -> None:
    html = INDEX_HTML.read_text(encoding="utf-8")
    refs = _runtime_references()
    stylesheet_refs = [value for location, value in refs if location == "link[href]"]

    assert "./design-system/tokens.css" in stylesheet_refs
    assert "./design-system/components.css" in stylesheet_refs
    body_match = re.search(
        r"<body\b[^>]*\bclass=['\"]([^'\"]+)['\"]",
        html,
        re.IGNORECASE,
    )
    assert body_match is not None
    body_classes = set(body_match.group(1).split())
    assert {"ds", "theme-air"}.issubset(body_classes)
    assert all(not EXTERNAL_URL.search(ref) for ref in stylesheet_refs)

    _assert_non_empty_file(DESIGN_SYSTEM_DIR / "tokens.css", label="design-system tokens")
    _assert_non_empty_file(
        DESIGN_SYSTEM_DIR / "components.css",
        label="design-system components",
    )


def test_wasm_index_points_at_local_vendor_runtime() -> None:
    html = INDEX_HTML.read_text(encoding="utf-8")

    pyodide_match = PYODIDE_URL.search(html)
    assert pyodide_match is not None, "web/index.html must set pyodideUrl via new URL(...)"
    pyodide_path = pyodide_match.group("url")
    assert pyodide_path.startswith("./vendor/"), (
        f"pyodideUrl must point at a local ./vendor path, got {pyodide_path!r}"
    )
    assert pyodide_path.endswith("pyodide.mjs"), (
        f"pyodideUrl must target pyodide.mjs, got {pyodide_path!r}"
    )

    wheel_paths = [match.group("url") for match in WHEEL_URL.finditer(html)]
    assert len(wheel_paths) == 2, (
        "web/index.html must declare local wheelUrls for stliteLib and streamlit"
    )
    for wheel_path in wheel_paths:
        assert wheel_path.startswith("./vendor/"), (
            f"wheelUrls must point at local ./vendor paths, got {wheel_path!r}"
        )
        assert wheel_path.endswith(".whl"), (
            f"wheelUrls must target vendored .whl files, got {wheel_path!r}"
        )


def test_vendored_pyodide_core_runtime_exists() -> None:
    for name in PYODIDE_CORE_FILES:
        _assert_non_empty_file(PYODIDE_VENDOR / name, label=f"Pyodide core file {name}")


def test_vendored_pyodide_wheels_exist() -> None:
    for pattern in PYODIDE_WHEEL_GLOBS:
        wheel = _glob_one(PYODIDE_VENDOR, pattern)
        _assert_non_empty_file(wheel, label=f"Pyodide wheel {wheel.name}")


def test_vendored_stlite_wheels_exist() -> None:
    assert STLITE_WHEELS.is_dir(), f"expected stlite wheel directory at {STLITE_WHEELS}"
    for pattern in STLITE_WHEEL_GLOBS:
        wheel = _glob_one(STLITE_WHEELS, pattern)
        _assert_non_empty_file(wheel, label=f"stlite wheel {wheel.name}")
