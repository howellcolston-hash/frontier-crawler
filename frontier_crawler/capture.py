"""Record the data requests a page makes, so a derived client can be written from them.

A page that shows figures loaded from a backend asks that backend for them as it opens.
capture() opens the page in the browser, optionally scrolls and clicks, and returns every
such request with a sample of its reply. A derived client spec is written from the result.

Input contract: result = await capture(url, clicks=[...], scrolls=n)
The result is plain JSON. Credential headers (cookies, authorization, API-key and CSRF headers) are
noted as present and their values are not written. A key the page itself puts in a URL or a
request body is recorded as sent.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence
from urllib.parse import urlsplit

from frontier_crawler.harvester.cascade import CREDENTIAL_HEADERS, USER_AGENT, check_public

MAX_ENTRIES = 80
SAMPLE_CHARS = 3000
# Requests that report on the visitor, not data for the page.
_NOISE = (
    "google-analytics", "googletagmanager", "doubleclick", "segment.", "sentry", "datadog", "intercom", "hotjar",
    "clarity.ms", "newrelic", "matomo", "piwik", "facebook.com/tr", "/collect", "/beacon", "/telemetry",
)
_KEPT_HEADERS = ("accept", "content-type", "origin", "referer")


def is_noise(url: str) -> bool:
    lowered = url.lower()
    return any(marker in lowered for marker in _NOISE)


def is_first_party(url: str, page_url: str) -> bool:
    """shortcut: compares host names, so a backend on an unrelated domain reads as third-party; check those by eye."""
    host = (urlsplit(url).hostname or "").lower()
    base = (urlsplit(page_url).hostname or "").lower().removeprefix("www.")
    return bool(host) and (host == base or host.endswith("." + base) or base.endswith("." + host))


def kept_headers(headers: Dict[str, str]) -> Dict[str, str]:
    """The headers a client may need to repeat. A credential is noted as present, never copied."""
    kept: Dict[str, str] = {}
    for name, value in headers.items():
        lowered = name.lower()
        if lowered in CREDENTIAL_HEADERS:
            kept[lowered] = "<present, not recorded>"
        elif lowered in _KEPT_HEADERS or (lowered.startswith("x-") and not lowered.startswith("x-forwarded")):
            kept[lowered] = value
    return kept


def json_shape(value: Any, depth: int = 4) -> Any:
    """The structure of a JSON reply without its data: keys, types, and list lengths."""
    if isinstance(value, dict):
        if depth == 0:
            return "object"
        return {key: json_shape(item, depth - 1) for key, item in list(value.items())[:40]}
    if isinstance(value, list):
        return {"list_len": len(value), "item": json_shape(value[0], depth - 1) if value and depth else None}
    return type(value).__name__


async def capture(
    url: str, clicks: Sequence[str] = (), scrolls: int = 2, timeout_s: float = 30.0, screenshot: Optional[Path] = None
) -> Dict[str, Any]:
    from playwright.async_api import Error as PlaywrightError
    from playwright.async_api import async_playwright

    check_public(url)
    responses: List[Any] = []
    actions: List[str] = []
    timeout_ms = int(timeout_s * 1000)
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        try:
            page = await (await browser.new_context(user_agent=USER_AGENT)).new_page()
            page.on("response", lambda response: responses.append(response))
            pending: set = set()
            page.on("request", lambda request: pending.add(request))
            page.on("requestfinished", lambda request: pending.discard(request))
            page.on("requestfailed", lambda request: pending.discard(request))

            async def settle() -> None:
                # Wait until no request has been in flight for a second. The browser's own
                # "network idle" state is reached once at load and says nothing after a click.
                quiet = 0
                for _ in range(int(timeout_s * 4)):
                    await page.wait_for_timeout(250)
                    quiet = quiet + 1 if not pending else 0
                    if quiet >= 4:
                        return
                actions.append(f"still loading after {timeout_s:.0f}s: {len(pending)} request(s) unanswered")

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            except PlaywrightError as exc:
                actions.append(f"open: failed: {exc}"[:300])
            await settle()
            for _ in range(scrolls):
                await page.mouse.wheel(0, 4000)
                await page.wait_for_timeout(1200)
            for text in clicks:
                # A button or link with that name first: the same word often also sits in a heading.
                for locator in (page.get_by_role("button", name=text), page.get_by_role("link", name=text), page.get_by_text(text)):
                    try:
                        await locator.first.click(timeout=4000)
                        await settle()
                        actions.append(f"click {text!r}: done")
                        break
                    except PlaywrightError:
                        continue
                else:
                    actions.append(f"click {text!r}: nothing clickable with that text")
            final_url = page.url
            check_public(final_url)
            # What a person could press next, and what the page looks like, so a missed request can be chased.
            clickable = await page.evaluate(
                """() => [...document.querySelectorAll('button, [role=button], input[type=submit], a[href]')]
                    .filter(e => e.offsetParent !== null)
                    .map(e => (e.tagName.toLowerCase() + ': ' + (e.innerText || e.value || e.getAttribute('aria-label') || '').trim().replace(/\\s+/g, ' ')).slice(0, 80))
                    .filter(t => !t.endsWith(': '))"""
            )
            if screenshot is not None:
                await page.screenshot(path=str(screenshot), full_page=False)
            entries = []
            for response in responses:
                request = response.request
                content_type = response.headers.get("content-type", "")
                if request.resource_type not in ("xhr", "fetch") and "json" not in content_type:
                    continue
                try:
                    body = await response.body()
                except PlaywrightError:
                    body = b""
                entry: Dict[str, Any] = {
                    "method": request.method,
                    "url": request.url,
                    "status": response.status,
                    "content_type": content_type,
                    "first_party": is_first_party(request.url, final_url),
                    "noise": is_noise(request.url),
                    "request_headers": kept_headers(request.headers),
                    "post_data": (request.post_data or "")[:SAMPLE_CHARS] or None,
                    "bytes": len(body),
                    "sample": body[:SAMPLE_CHARS].decode("utf-8", errors="replace"),
                }
                try:
                    entry["json_shape"] = json_shape(json.loads(body))
                except ValueError:
                    entry["json_shape"] = None
                entries.append(entry)
        finally:
            await browser.close()
    # Likely data endpoints first: the site's own, not tracking, JSON, largest reply.
    entries.sort(key=lambda e: (not e["first_party"], e["noise"], e["json_shape"] is None, -e["bytes"]))
    return {
        "page": url,
        "final_url": final_url,
        "captured_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "actions": actions,
        "clickable": list(dict.fromkeys(clickable))[:80],
        "requests_seen": len(entries),
        "entries": entries[:MAX_ENTRIES],
    }
