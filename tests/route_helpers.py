from collections.abc import Iterable
from typing import Any


def route_paths(routes: Iterable[Any]) -> set[str]:
    return {path for route in routes if isinstance(path := getattr(route, "path", None), str)}
