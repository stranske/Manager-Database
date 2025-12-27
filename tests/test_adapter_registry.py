import sys
import types
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from adapters.base import get_adapter


def test_get_adapter_returns_module():
    adapter = get_adapter("edgar")
    assert isinstance(adapter, types.ModuleType)
    assert hasattr(adapter, "list_new_filings")
