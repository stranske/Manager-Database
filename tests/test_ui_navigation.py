import importlib
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))


class FakeNavigation:
    def __init__(self) -> None:
        self.run_called = False

    def run(self) -> None:
        self.run_called = True


class FakeStreamlit:
    def __init__(self) -> None:
        self.page_calls: list[dict[str, object]] = []
        self.navigation_calls: list[dict[str, object]] = []
        self.navigation_instance = FakeNavigation()

    def Page(
        self, target, title: str, icon: str, url_path: str
    ):  # noqa: N802 - mirrors streamlit API
        page = {
            "target": target,
            "title": title,
            "icon": icon,
            "url_path": url_path,
        }
        self.page_calls.append(page)
        return page

    def navigation(self, pages, position: str):
        self.navigation_calls.append({"pages": pages, "position": position})
        return self.navigation_instance


def test_navigation_includes_research_page(monkeypatch):
    app = importlib.reload(importlib.import_module("ui.app"))
    fake_st = FakeStreamlit()
    monkeypatch.setattr(app, "st", fake_st)

    app.main()

    assert fake_st.navigation_calls[0]["position"] == "sidebar"
    url_paths = [page["url_path"] for page in fake_st.page_calls]
    titles = [page["title"] for page in fake_st.page_calls]

    assert "research" in url_paths
    assert "🔬 Research" in titles
    assert fake_st.navigation_instance.run_called is True
