"""Per-host request spacing. One slow host must not slow the others, and no host is hit in bursts.

Input contract: await throttle.wait(url) before each request to that URL's host.
min_interval_s is the gap between request starts to one host. It is PROVISIONAL.
"""

from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable, Dict
from urllib.parse import urlsplit


class HostThrottle:
    def __init__(
        self,
        min_interval_s: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._interval = max(0.0, min_interval_s)
        self._clock = clock
        self._sleep = sleep
        self._next_ok: Dict[str, float] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    async def wait(self, url: str) -> None:
        host = urlsplit(url).netloc.lower()
        if not host or self._interval == 0.0:
            return
        lock = self._locks.setdefault(host, asyncio.Lock())
        async with lock:
            delay = self._next_ok.get(host, 0.0) - self._clock()
            if delay > 0:
                await self._sleep(delay)
            self._next_ok[host] = self._clock() + self._interval
