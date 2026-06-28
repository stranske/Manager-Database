"""Small offline-demo fallbacks for optional API framework imports."""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any, TypeVar

T = TypeVar("T")


class OfflineAPIRouter:
    """No-op router used when the stlite demo imports API query helpers."""

    def get(self, *args: Any, **kwargs: Any) -> Callable[[T], T]:
        return lambda func: func


class OfflineBaseModel:
    """Minimal model shape needed by Streamlit query helpers in offline mode."""

    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)

    def model_dump(self) -> dict[str, Any]:
        return dict(self.__dict__)


def offline_field(
    *, default: Any = None, default_factory: Callable[[], Any] | None = None, **_: Any
) -> Any:
    if default_factory is not None:
        return default_factory()
    return default


def offline_query(default: Any = None, *args: Any, **kwargs: Any) -> Any:
    return default


def offline_api_imports() -> (
    tuple[type[OfflineAPIRouter], type[OfflineBaseModel], Callable[..., Any], Callable[..., Any]]
):
    if os.getenv("UI_OFFLINE") != "1":
        raise ModuleNotFoundError("FastAPI/Pydantic fallbacks are only available with UI_OFFLINE=1")
    return OfflineAPIRouter, OfflineBaseModel, offline_field, offline_query
