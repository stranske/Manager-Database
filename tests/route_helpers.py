from collections.abc import Iterable
from typing import Any


def route_paths(routes: Iterable[Any]) -> set[str]:
    paths: set[str] = set()
    for route in routes:
        if isinstance(path := getattr(route, "path", None), str):
            paths.add(path)

        nested_routes = getattr(getattr(route, "original_router", None), "routes", None)
        if nested_routes is not None:
            paths.update(route_paths(nested_routes))

    return paths
