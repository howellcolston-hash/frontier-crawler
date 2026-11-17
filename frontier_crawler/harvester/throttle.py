"""Per-host request spacing. One slow host must not slow the others, and no host is hit in bursts.

Input contract: await throttle.wait(url) before each request to that URL's host.
min_interval_s is the gap between request starts to one host. It is PROVISIONAL.
intervals: host -> a longer gap for that host (a robots.txt Crawl-delay, a derived client's min_interval_s).
hold(url, seconds): no request to that host before then (a server's Retry-After); see courtesy.py.
record(url, seconds, status): after each plain reply, seconds being how long the server took to answer. A host's gap moves
toward that time and doubles on a refusal, never below its set gap (Scrapling's AutoThrottle, slowing only).
"""

from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable, Dict, Optional
from urllib.parse import urlsplit

MAX_LEARNED_S = 60.0   # PROVISIONAL: the longest gap record() can set for a host
# PROVISIONAL. Replies that mean the host is refusing or struggling (Scrapling's blocked codes, less the 401 and 407 of a login).
REFUSAL_STATUS = (403, 429, 444, 500, 502, 503, 504)


class HostThrottle:
    def __init__(
        self,
        min_interval_s: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        intervals: Optional[Dict[str, float]] = None,
    ) -> None:
        self._interval = max(0.0, min_interval_s)
        self._clock = clock
        self._sleep = sleep
        self.intervals: Dict[str, float] = dict(intervals or {})   # used where longer than min_interval_s
        self.learned: Dict[str, float] = {}   # host -> the gap record() arrived at, for this run only
        self._next_ok: Dict[str, float] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    def hold(self, url: str, seconds: float) -> None:
        host = urlsplit(url).netloc.lower()
        self._next_ok[host] = max(self._next_ok.get(host, 0.0), self._clock() + seconds)

    def record(self, url: str, seconds: float, status: int) -> None:
        host = urlsplit(url).netloc.lower()
        floor = max(self._interval, self.intervals.get(host, 0.0))
        current = self.learned.get(host, floor)
        gap = max((current + seconds) / 2, seconds)   # a slow reply counts at once, a fast one halfway
        if status in REFUSAL_STATUS:
            gap = max(gap, current * 2)
        self.learned[host] = max(floor, min(gap, MAX_LEARNED_S))

    async def wait(self, url: str) -> None:
        host = urlsplit(url).netloc.lower()
        if not host:
            return
        lock = self._locks.setdefault(host, asyncio.Lock())
        async with lock:
            # A loop: a hold may arrive while this request sleeps.
            while (delay := self._next_ok.get(host, 0.0) - self._clock()) > 0:
                await self._sleep(delay)
            self._next_ok[host] = self._clock() + max(self._interval, self.intervals.get(host, 0.0), self.learned.get(host, 0.0))
