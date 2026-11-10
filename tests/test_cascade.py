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
            async def fake_browser(url: str, prior_note: Optional[str], prior_status: int) -> ScrapeResult:
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
