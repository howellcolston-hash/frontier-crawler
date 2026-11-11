"""Checks for the fetcher: which tier answers, what is kept, and what is refused. No live network."""

from __future__ import annotations

import asyncio
import base64
from typing import Any, List, Optional

import httpx
import pytest

from frontier_crawler.harvester import cascade as cascade_module
from frontier_crawler.harvester.cascade import BlockedAddress, HarvesterCascade, ScrapeResult, check_public

WORDS = "The port authority published the berth schedule and the tariff for bulk cargo. " * 15
# Three pages, one line of text each, made with reportlab.
PDF = base64.b64decode("JVBERi0xLjMKJZOMi54gUmVwb3J0TGFiIEdlbmVyYXRlZCBQREYgZG9jdW1lbnQgKG9wZW5zb3VyY2UpCjEgMCBvYmoKPDwKL0YxIDIgMCBSCj4+CmVuZG9iagoyIDAgb2JqCjw8Ci9CYXNlRm9udCAvSGVsdmV0aWNhIC9FbmNvZGluZyAvV2luQW5zaUVuY29kaW5nIC9OYW1lIC9GMSAvU3VidHlwZSAvVHlwZTEgL1R5cGUgL0ZvbnQKPj4KZW5kb2JqCjMgMCBvYmoKPDwKL0NvbnRlbnRzIDkgMCBSIC9NZWRpYUJveCBbIDAgMCA1OTUuMjc1NiA4NDEuODg5OCBdIC9QYXJlbnQgOCAwIFIgL1Jlc291cmNlcyA8PAovRm9udCAxIDAgUiAvUHJvY1NldCBbIC9QREYgL1RleHQgL0ltYWdlQiAvSW1hZ2VDIC9JbWFnZUkgXQo+PiAvUm90YXRlIDAgL1RyYW5zIDw8Cgo+PiAKICAvVHlwZSAvUGFnZQo+PgplbmRvYmoKNCAwIG9iago8PAovQ29udGVudHMgMTAgMCBSIC9NZWRpYUJveCBbIDAgMCA1OTUuMjc1NiA4NDEuODg5OCBdIC9QYXJlbnQgOCAwIFIgL1Jlc291cmNlcyA8PAovRm9udCAxIDAgUiAvUHJvY1NldCBbIC9QREYgL1RleHQgL0ltYWdlQiAvSW1hZ2VDIC9JbWFnZUkgXQo+PiAvUm90YXRlIDAgL1RyYW5zIDw8Cgo+PiAKICAvVHlwZSAvUGFnZQo+PgplbmRvYmoKNSAwIG9iago8PAovQ29udGVudHMgMTEgMCBSIC9NZWRpYUJveCBbIDAgMCA1OTUuMjc1NiA4NDEuODg5OCBdIC9QYXJlbnQgOCAwIFIgL1Jlc291cmNlcyA8PAovRm9udCAxIDAgUiAvUHJvY1NldCBbIC9QREYgL1RleHQgL0ltYWdlQiAvSW1hZ2VDIC9JbWFnZUkgXQo+PiAvUm90YXRlIDAgL1RyYW5zIDw8Cgo+PiAKICAvVHlwZSAvUGFnZQo+PgplbmRvYmoKNiAwIG9iago8PAovUGFnZU1vZGUgL1VzZU5vbmUgL1BhZ2VzIDggMCBSIC9UeXBlIC9DYXRhbG9nCj4+CmVuZG9iago3IDAgb2JqCjw8Ci9BdXRob3IgKGFub255bW91cykgL0NyZWF0aW9uRGF0ZSAoRDoyMDI2MTEwOTIxMDYxNy0wNScwMCcpIC9DcmVhdG9yIChhbm9ueW1vdXMpIC9LZXl3b3JkcyAoKSAvTW9kRGF0ZSAoRDoyMDI2MTEwOTIxMDYxNy0wNScwMCcpIC9Qcm9kdWNlciAoUmVwb3J0TGFiIFBERiBMaWJyYXJ5IC0gXChvcGVuc291cmNlXCkpIAogIC9TdWJqZWN0ICh1bnNwZWNpZmllZCkgL1RpdGxlICh1bnRpdGxlZCkgL1RyYXBwZWQgL0ZhbHNlCj4+CmVuZG9iago4IDAgb2JqCjw8Ci9Db3VudCAzIC9LaWRzIFsgMyAwIFIgNCAwIFIgNSAwIFIgXSAvVHlwZSAvUGFnZXMKPj4KZW5kb2JqCjkgMCBvYmoKPDwKL0ZpbHRlciBbIC9BU0NJSTg1RGVjb2RlIC9GbGF0ZURlY29kZSBdIC9MZW5ndGggMTQ2Cj4+CnN0cmVhbQpHYXBBMDBhW3Q6JjQiV2w6W3EpYz0+Uy47SyZxbUQ9TyUoOioxKUpMTG5yR0lNZGw6WWB0bmRYcSkmblNRMGhHMSFQS1tlNUdEZCNnPEtJYV9ERXFVIUExQkpuU1BCRz88Xi9vZk1TcWU/YiM5MFNyRGJLXiJKKj9nIjVlTnJaXnVNb1MuN3Q+SWtAJlIzRE1+PmVuZHN0cmVhbQplbmRvYmoKMTAgMCBvYmoKPDwKL0ZpbHRlciBbIC9BU0NJSTg1RGVjb2RlIC9GbGF0ZURlY29kZSBdIC9MZW5ndGggMTQ2Cj4+CnN0cmVhbQpHYXBBMDBhW3Q6JjQiV2w6W3EpYz0+Uy47SyZxbUQ9TyUoOioxKUpMTG5yR0lNZGw6WWB0bmRYcSkmblNRMGhHMSFQS1tlNUdEZCNnPEtJYV9ERXFVIUExQkpuU1BCRz88XjBabG1VUG4wQUxWKDpXRHNhaFghUzBsdF5sN0lqK2IvXXFMcWFRTDRrNGdgY1t+PmVuZHN0cmVhbQplbmRvYmoKMTEgMCBvYmoKPDwKL0ZpbHRlciBbIC9BU0NJSTg1RGVjb2RlIC9GbGF0ZURlY29kZSBdIC9MZW5ndGggMTQ2Cj4+CnN0cmVhbQpHYXBBMDBhW3Q6JjQiV2w6W3EpYz0+T0FvXy9fdF0vRVsoLSoxKUpMTG5yR0lNZGw6WWB0bmRYcSkmblNQak0+MExnSDQ7KzQpZUttM19jVkA+cC4iVFM6cDFJVHBUQmY5VlU0R0VoSUkoW2E7Ty1ZJUdJaFghUzBsdF5tanJaXyZPb1MuN3Q+SWtAJyshTF5+PmVuZHN0cmVhbQplbmRvYmoKeHJlZgowIDEyCjAwMDAwMDAwMDAgNjU1MzUgZiAKMDAwMDAwMDA2MSAwMDAwMCBuIAowMDAwMDAwMDkyIDAwMDAwIG4gCjAwMDAwMDAxOTkgMDAwMDAgbiAKMDAwMDAwMDQwMiAwMDAwMCBuIAowMDAwMDAwNjA2IDAwMDAwIG4gCjAwMDAwMDA4MTAgMDAwMDAgbiAKMDAwMDAwMDg3OCAwMDAwMCBuIAowMDAwMDAxMTM5IDAwMDAwIG4gCjAwMDAwMDEyMTAgMDAwMDAgbiAKMDAwMDAwMTQ0NiAwMDAwMCBuIAowMDAwMDAxNjgzIDAwMDAwIG4gCnRyYWlsZXIKPDwKL0lEIApbPDkxMjc5NDkxYjgzNzJhZGRmZjc4NGJiMmZlZmFlMTMwPjw5MTI3OTQ5MWI4MzcyYWRkZmY3ODRiYjJmZWZhZTEzMD5dCiUgUmVwb3J0TGFiIGdlbmVyYXRlZCBQREYgZG9jdW1lbnQgLS0gZGlnZXN0IChvcGVuc291cmNlKQoKL0luZm8gNyAwIFIKL1Jvb3QgNiAwIFIKL1NpemUgMTIKPj4Kc3RhcnR4cmVmCjE5MjAKJSVFT0YK")


def _fetch(handler: Any, url: str = "https://a.test/x", browser: Optional[List[str]] = None, monkeypatch: Any = None) -> ScrapeResult:
    """Fetch through a fake server. When `browser` is a list, the browser tier is replaced and records its calls."""

    async def run() -> ScrapeResult:
        fetcher = HarvesterCascade(client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
        if browser is not None:
            async def fake_browser(url: str, prior_note: Optional[str], prior_status: int, *_: Any) -> ScrapeResult:
                browser.append(url)
                return ScrapeResult(url, 0, b"", None, 0, "HEADLESS_PLAYWRIGHT", "browser got nothing")

            monkeypatch.setattr(fetcher, "_browser", fake_browser)
        return await fetcher.fetch(url)

    return asyncio.run(run())


def _html(body: str, status: int = 200) -> Any:
    return lambda request: httpx.Response(status, text=f"<html><body>{body}</body></html>", headers={"content-type": "text/html"})


def test_json_is_the_api_tier() -> None:
    result = _fetch(lambda request: httpx.Response(200, json={"rate": 12.0}))
    assert result.tier_used == "API" and result.raw_payload == b'{"rate":12.0}' and result.error_message is None


def test_a_page_of_text_needs_no_browser(monkeypatch: pytest.MonkeyPatch) -> None:
    browser: List[str] = []
    result = _fetch(_html(f"<p>{WORDS}</p>"), browser=browser, monkeypatch=monkeypatch)
    assert result.tier_used == "DOM_TRAFILATURA" and result.word_count >= 150 and not browser


def test_an_index_page_of_links_needs_no_browser(monkeypatch: pytest.MonkeyPatch) -> None:
    browser: List[str] = []
    links = "".join(f'<a href="/notice/{i}">Notice {i}</a> ' for i in range(12))
    result = _fetch(_html(links), browser=browser, monkeypatch=monkeypatch)
    assert result.word_count < 150 and b"/notice/11" in result.raw_payload and not browser


def test_an_empty_shell_goes_to_the_browser_and_its_bytes_are_kept_if_that_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    browser: List[str] = []
    result = _fetch(_html('<div id="app"></div>'), browser=browser, monkeypatch=monkeypatch)
    assert browser == ["https://a.test/x"]
    assert b'id="app"' in result.raw_payload and result.error_message == "browser got nothing"


def test_a_dead_link_does_not_launch_a_browser_but_a_refusal_does(monkeypatch: pytest.MonkeyPatch) -> None:
    browser: List[str] = []
    gone = _fetch(_html("Not found", 404), browser=browser, monkeypatch=monkeypatch)
    assert gone.http_status == 404 and not browser
    refused = _fetch(_html("Forbidden", 403), browser=browser, monkeypatch=monkeypatch)
    assert len(browser) == 1 and refused.http_status == 403 and b"Forbidden" in refused.raw_payload


def test_a_dead_host_is_a_failed_result_not_an_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    browser: List[str] = []

    def down(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route", request=request)

    result = _fetch(down, browser=browser, monkeypatch=monkeypatch)
    assert result.raw_payload == b"" and "ConnectError" in result.error_message and not browser


def test_a_file_over_the_cap_is_recorded_and_not_kept(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cascade_module, "MAX_BYTES", 1000)
    declared = _fetch(lambda request: httpx.Response(200, content=b"x" * 5000, headers={"content-type": "text/csv"}))
    assert declared.too_large_bytes == 5000 and declared.raw_payload == b"" and declared.content_type == "text/csv"

    async def body() -> Any:
        for _ in range(50):
            yield b"x" * 100

    streamed = _fetch(lambda request: httpx.Response(200, content=body()))   # no declared length
    assert streamed.raw_payload == b"" and 1000 < streamed.too_large_bytes <= 1100 and streamed.error_message == "too large"


def test_pdf_text_is_read_from_every_page() -> None:
    result = _fetch(lambda request: httpx.Response(200, content=PDF, headers={"content-type": "application/pdf"}), url="https://a.test/gazette.pdf")
    assert result.tier_used == "PDF" and result.raw_payload == PDF and result.error_message is None
    assert "Gazette notice 0" in result.markdown_content and "Company 2" in result.markdown_content


def test_pdf_reading_stops_at_the_time_limit_and_says_so(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cascade_module, "PDF_SECONDS", -1.0)
    result = _fetch(lambda request: httpx.Response(200, content=PDF, headers={"content-type": "application/pdf"}))
    assert result.raw_payload == PDF and result.error_message == "text read for the first 0 of 3 pages"


def test_a_damaged_pdf_is_still_stored() -> None:
    result = _fetch(lambda request: httpx.Response(200, content=b"%PDF-1.4 broken", headers={"content-type": "application/pdf"}))
    assert result.raw_payload == b"%PDF-1.4 broken" and result.error_message.startswith("pdf not read")


@pytest.mark.parametrize("url", ["http://localhost:8080/admin", "http://169.254.169.254/latest/meta-data/", "http://10.0.0.5/", "http://[::1]/", "http://db.internal/"])
def test_private_addresses_are_refused(url: str) -> None:
    with pytest.raises(BlockedAddress):
        check_public(url)
    with pytest.raises(BlockedAddress):
        _fetch(lambda request: pytest.fail("a request was sent"), url=url)


def test_public_addresses_pass() -> None:
    check_public("https://www.example.org/")
    check_public("http://8.8.8.8/")


def test_a_redirect_to_a_private_address_is_refused() -> None:
    async def run() -> None:
        await cascade_module._guard_request(httpx.Request("GET", "http://169.254.169.254/"))

    with pytest.raises(BlockedAddress):
        asyncio.run(run())


def test_the_cascade_times_its_phases_and_counts_timeouts() -> None:
    def server(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/slow":
            raise httpx.ReadTimeout("slow", request=request)
        return _html(f"<p>{WORDS}</p>")(request)

    async def run() -> HarvesterCascade:
        fetcher = HarvesterCascade(client=httpx.AsyncClient(transport=httpx.MockTransport(server)))
        assert (await fetcher.fetch("https://a.test/x")).word_count > 100
        assert (await fetcher.fetch("https://a.test/slow")).raw_payload == b""
        return fetcher

    fetcher = asyncio.run(run())
    assert fetcher.timings["http"] > 0 and fetcher.timings["extract_html"] > 0
    assert fetcher.counts == {"http_timeouts": 1, "browser_timeouts": 0, "browser_launches": 0, "browser_pages": 0, "browser_relaunches": 0}


def test_a_timeout_is_flagged_and_one_call_can_ask_for_a_longer_limit() -> None:
    limits: List[float] = []

    def server(request: httpx.Request) -> httpx.Response:
        limits.append(request.extensions["timeout"]["read"])
        if request.url.path == "/slow":
            raise httpx.ReadTimeout("slow", request=request)
        return httpx.Response(404, text="gone")

    async def run() -> List[ScrapeResult]:
        fetcher = HarvesterCascade(client=httpx.AsyncClient(transport=httpx.MockTransport(server)))
        return [await fetcher.fetch("https://a.test/slow"), await fetcher.fetch("https://a.test/slow", timeout=90), await fetcher.fetch("https://a.test/gone")]

    slow, slower, gone = asyncio.run(run())
    assert slow.timed_out and slower.timed_out and not gone.timed_out and limits == [30.0, 90.0, 30.0]


def test_each_fetch_reports_its_own_wait_for_the_hosts_turn() -> None:
    from frontier_crawler.harvester.throttle import HostThrottle

    async def run() -> List[ScrapeResult]:
        fetcher = HarvesterCascade(client=httpx.AsyncClient(transport=httpx.MockTransport(_html(f"<p>{WORDS}</p>"))), throttle=HostThrottle(0.05))
        return [await fetcher.fetch("https://a.test/x"), await fetcher.fetch("https://a.test/y")]

    first, second = asyncio.run(run())
    assert first.throttle_wait < 0.02 and second.throttle_wait > 0.03


def test_a_timer_fault_does_not_break_a_fetch() -> None:
    class Broken(dict):
        def __setitem__(self, key: Any, value: Any) -> None:
            raise RuntimeError("timer fault")

    async def run() -> ScrapeResult:
        fetcher = HarvesterCascade(client=httpx.AsyncClient(transport=httpx.MockTransport(_html(f"<p>{WORDS}</p>"))))
        fetcher.timings = Broken()
        return await fetcher.fetch("https://a.test/x")

    assert asyncio.run(run()).word_count > 100


class FakeChromium:
    """Stands in for playwright.async_api, offline. A URL says what its page does: "bad" fails to load,
    "dies" kills the browser while loading (every time; "dies-once" only the first time), "slow" takes a moment,
    "hangs" never answers the first time."""

    class Error(Exception):
        pass

    class TimeoutError(Error):
        pass

    def __init__(self, fail_launches: int = 0) -> None:
        self.fail_launches = fail_launches
        self.drivers: List[Any] = []
        self.browsers: List[Any] = []
        self.contexts: List[Any] = []
        self.died: List[str] = []
        self.active = self.peak = 0

    def async_playwright(self) -> Any:
        fake = self

        class Page:
            def __init__(self, browser: Any) -> None:
                self.browser, self.url = browser, ""

            async def goto(self, url: str, wait_until: str, timeout: int) -> Any:
                fake.active += 1
                fake.peak = max(fake.peak, fake.active)
                try:
                    await asyncio.sleep(0.01 if "slow" in url else 0)
                    if "hangs" in url and url not in fake.died:
                        fake.died.append(url)
                        await asyncio.sleep(60)
                    if not self.browser.connected:
                        raise fake.Error("Target page, context or browser has been closed")
                    if "dies" in url and not ("once" in url and url in fake.died):
                        fake.died.append(url)
                        self.browser.connected = False
                        raise fake.Error("Target page, context or browser has been closed")
                    if "bad" in url:
                        raise fake.Error("net::ERR_NAME_NOT_RESOLVED")
                    if "timesout" in url:
                        raise fake.TimeoutError(f"Timeout {timeout}ms exceeded")
                    self.url = url
                    return type("Response", (), {"status": 200})()
                finally:
                    fake.active -= 1

            async def inner_text(self, selector: str) -> str:
                return "rendered text"

            async def content(self) -> str:
                return "<html><body>rendered text</body></html>"

        class Context:
            def __init__(self, browser: Any) -> None:
                self.browser, self.closed = browser, False

            async def new_page(self) -> Any:
                return Page(self.browser)

            async def close(self) -> None:
                self.closed = True

        class Browser:
            def __init__(self) -> None:
                self.connected = True

            def is_connected(self) -> bool:
                return self.connected

            async def new_context(self, user_agent: str) -> Any:
                if not self.connected:
                    raise fake.Error("Browser has been closed")
                fake.contexts.append(Context(self))
                return fake.contexts[-1]

            async def close(self) -> None:
                self.connected = False

        class Driver:
            def __init__(self) -> None:
                self.stopped = False
                self.chromium = self

            async def start(self) -> Any:
                fake.drivers.append(self)
                return self

            async def launch(self, headless: bool) -> Any:
                if fake.fail_launches:
                    fake.fail_launches -= 1
                    raise fake.Error("chromium failed to start")
                fake.browsers.append(Browser())
                return fake.browsers[-1]

            async def stop(self) -> None:
                self.stopped = True

        return Driver()


@pytest.fixture
def chromium(monkeypatch: pytest.MonkeyPatch) -> FakeChromium:
    import types

    fake = FakeChromium()
    module = types.ModuleType("playwright.async_api")
    module.Error, module.TimeoutError, module.async_playwright = fake.Error, fake.TimeoutError, fake.async_playwright
    monkeypatch.setitem(__import__("sys").modules, "playwright", types.ModuleType("playwright"))
    monkeypatch.setitem(__import__("sys").modules, "playwright.async_api", module)
    return fake


def _render(urls: List[str], browser_slots: int = 4) -> Any:
    """Render the URLs through the browser tier at once; return (results, cascade) after the cascade closed."""

    async def run() -> Any:
        async with HarvesterCascade(browser_slots=browser_slots) as fetcher:
            results = []
            for batch in urls:
                results += await asyncio.gather(*(fetcher._browser(url, None, 200) for url in batch.split()))
        return results, fetcher

    return asyncio.run(run())


def _browser_counts(fetcher: HarvesterCascade) -> dict:
    return {key: value for key, value in fetcher.counts.items() if key.startswith("browser_")}


def test_one_browser_serves_every_page_and_each_context_is_closed(chromium: FakeChromium) -> None:
    results, fetcher = _render(["https://a.test/1 https://a.test/2 https://a.test/3", "https://a.test/4 https://a.test/bad"])
    assert [bool(r.raw_payload) for r in results] == [True, True, True, True, False]
    assert results[0].tier_used == "HEADLESS_PLAYWRIGHT" and results[0].markdown_content == "rendered text"
    assert "ERR_NAME_NOT_RESOLVED" in results[4].error_message
    assert len(chromium.browsers) == 1 and len(chromium.contexts) == 5 and all(c.closed for c in chromium.contexts)
    assert _browser_counts(fetcher) == {"browser_timeouts": 0, "browser_launches": 1, "browser_pages": 5, "browser_relaunches": 0}


def test_a_page_load_timeout_is_flagged_and_leaves_the_browser_running(chromium: FakeChromium) -> None:
    results, fetcher = _render(["https://a.test/timesout https://a.test/1"])
    assert results[0].timed_out and "Timeout 30000ms" in results[0].error_message and not results[1].timed_out
    assert len(chromium.browsers) == 1 and fetcher.counts["browser_timeouts"] == 1


def test_closing_the_cascade_closes_browser_and_playwright(chromium: FakeChromium) -> None:
    _render(["https://a.test/1"])
    assert not chromium.browsers[0].connected and chromium.drivers[0].stopped


def test_a_browser_that_dies_is_relaunched_and_the_page_retried(chromium: FakeChromium) -> None:
    results, fetcher = _render(["https://a.test/dies-once"])
    assert results[0].raw_payload and len(chromium.browsers) == 2 and chromium.drivers[0].stopped
    assert all(c.closed for c in chromium.contexts)
    assert _browser_counts(fetcher) == {"browser_timeouts": 0, "browser_launches": 2, "browser_pages": 1, "browser_relaunches": 1}


def test_a_second_death_fails_the_page_and_the_next_page_launches_afresh(chromium: FakeChromium) -> None:
    results, fetcher = _render(["https://a.test/dies", "https://a.test/after"])
    assert results[0].raw_payload == b"" and results[0].http_status == 200 and "has been closed" in results[0].error_message
    assert results[1].raw_payload and len(chromium.browsers) == 3
    assert all(d.stopped for d in chromium.drivers) and all(c.closed for c in chromium.contexts)
    assert _browser_counts(fetcher) == {"browser_timeouts": 0, "browser_launches": 3, "browser_pages": 2, "browser_relaunches": 2}


def test_pages_on_a_browser_that_died_together_relaunch_it_once(chromium: FakeChromium) -> None:
    results, fetcher = _render(["https://a.test/1", "https://a.test/slow-1 https://a.test/slow-2 https://a.test/dies-once"])
    assert all(r.raw_payload for r in results) and len(chromium.browsers) == 2
    assert fetcher.counts["browser_relaunches"] == 1


def test_a_browser_found_disconnected_is_replaced_before_use(chromium: FakeChromium) -> None:
    async def run() -> HarvesterCascade:
        async with HarvesterCascade() as fetcher:
            assert (await fetcher._browser("https://a.test/1", None, 200)).raw_payload
            chromium.browsers[0].connected = False
            assert (await fetcher._browser("https://a.test/2", None, 200)).raw_payload
        return fetcher

    fetcher = asyncio.run(run())
    assert len(chromium.browsers) == 2 and chromium.drivers[0].stopped and fetcher.counts["browser_relaunches"] == 1


def test_a_failed_launch_stops_playwright_and_is_tried_once_more(chromium: FakeChromium) -> None:
    chromium.fail_launches = 2
    results, fetcher = _render(["https://a.test/1", "https://a.test/2"])
    assert results[0].raw_payload == b"" and "chromium failed to start" in results[0].error_message
    assert results[1].raw_payload and all(d.stopped for d in chromium.drivers) and len(chromium.drivers) == 3


def test_browser_slots_cap_pages_rendering_at_once(chromium: FakeChromium) -> None:
    results, _ = _render([" ".join(f"https://a.test/slow-{i}" for i in range(6))], browser_slots=2)
    assert all(r.raw_payload for r in results) and chromium.peak == 2


def test_a_hung_browser_is_discarded_and_the_page_retried(chromium: FakeChromium, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cascade_module, "BROWSER_HUNG_SECONDS", 0.05)
    results, fetcher = _render(["https://a.test/hangs"])
    assert results[0].raw_payload and len(chromium.browsers) == 2 and chromium.drivers[0].stopped
    assert all(c.closed for c in chromium.contexts) and fetcher.counts["browser_relaunches"] == 1
