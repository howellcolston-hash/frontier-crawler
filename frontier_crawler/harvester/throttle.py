"""Per-host request spacing. One slow host must not slow the others, and no host is hit in bursts.

Input contract: await throttle.wait(url) before each request to that URL's host.
min_interval_s is the gap between request starts to one host. It is PROVISIONAL.
intervals: host -> a longer gap for that host (a robots.txt Crawl-delay, a derived client's min_interval_s).
hold(url, seconds): no request to that host before then (a server's Retry-After); see courtesy.py.
"""

from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable, Dict, Optional
from urllib.parse import urlsplit


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
        self._next_ok: Dict[str, float] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    def hold(self, url: str, seconds: float) -> None:
        host = urlsplit(url).netloc.lower()
        self._next_ok[host] = max(self._next_ok.get(host, 0.0), self._clock() + seconds)

    async def wait(self, url: str) -> None:
        host = urlsplit(url).netloc.lower()
        if not host:
            return
        lock = self._locks.setdefault(host, asyncio.Lock())
        async with lock:
            # A loop: a hold may arrive while this request sleeps.
            while (delay := self._next_ok.get(host, 0.0) - self._clock()) > 0:
                await self._sleep(delay)
            self._next_ok[host] = self._clock() + max(self._interval, self.intervals.get(host, 0.0))
