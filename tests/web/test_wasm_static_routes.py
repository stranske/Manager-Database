from __future__ import annotations

import contextlib
import functools
import http.server
import socketserver
import threading
from collections.abc import Iterator
from pathlib import Path
from urllib.request import urlopen

from scripts.build_wasm_demo import STATIC_ROUTE_PATHS, build_wasm_demo


@contextlib.contextmanager
def _static_server(directory: Path) -> Iterator[str]:
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=directory)
    with socketserver.TCPServer(("127.0.0.1", 0), handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_address[1]}"
        finally:
            server.shutdown()
            thread.join(timeout=5)


def _fetch_text(url: str) -> tuple[int, str]:
    with urlopen(url, timeout=10) as response:
        return response.status, response.read().decode("utf-8")


def _fetch_bytes(url: str) -> tuple[int, bytes]:
    with urlopen(url, timeout=10) as response:
        return response.status, response.read()


def test_wasm_demo_static_routes_serve_stlite_shell(tmp_path) -> None:
    web_dir = tmp_path / "web"
    build_wasm_demo(web_dir)

    with _static_server(web_dir) as base_url:
        for route in ("index.html", *(f"{path}/" for path in STATIC_ROUTE_PATHS)):
            status, text_body = _fetch_text(f"{base_url}/{route}")
            assert status == 200
            assert "Manager-Database Offline Demo" in text_body
            assert "stlite.js" in text_body
            assert "Error response" not in text_body
        status, bytes_body = _fetch_bytes(f"{base_url}/wasm_app.py")
        assert status == 200
        assert b"Offline stlite entrypoint" in bytes_body


def test_wasm_demo_static_route_entrypoints_use_parent_base(tmp_path) -> None:
    web_dir = tmp_path / "web"
    build_wasm_demo(web_dir)

    for route in STATIC_ROUTE_PATHS:
        html = (web_dir / route / "index.html").read_text(encoding="utf-8")
        assert '<base href="../" />' in html
        assert "./vendor/stlite/browser-" in html
        assert "/build/stlite.js" in html
