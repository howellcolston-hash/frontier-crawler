"""Courtesy pacing, so a site has no reason to ban the crawler: the Crawl-delay a robots.txt asks for, and sitting out
when a server says it is overloaded.

Input contract:
    seconds = crawl_delay(robots_body, USER_AGENT)          # None when robots.txt sets none for us or for *
    cool = sit_out(throttle, url, status, retry_after)       # after each reply; see sit_out
"""

from __future__ import annotations

import datetime as dt
import email.utils
import math
from typing import Optional

from frontier_crawler.harvester.throttle import HostThrottle

MAX_CRAWL_DELAY_S = 30.0   # PROVISIONAL: a longer Crawl-delay is taken as this
MAX_SIT_OUT_S = 300.0      # PROVISIONAL: a longer Retry-After (or a 429 without one) leaves the host alone for the rest of the run


def crawl_delay(robots: bytes, user_agent: str) -> Optional[float]:
    """The Crawl-delay of the robots.txt group naming us (a User-agent value found in user_agent), else of the * group,
    capped at MAX_CRAWL_DELAY_S. Only the delay is read: rules are not applied."""
    ours: Optional[float] = None
    anyone: Optional[float] = None
    agents: list = []
    in_rules = False
    for line in robots.decode("utf-8", errors="replace").splitlines():
        name, _, value = line.split("#", 1)[0].partition(":")
        name, value = name.strip().lower(), value.strip()
        if name == "user-agent":
            if in_rules:   # a User-agent after rules starts a new group
                agents, in_rules = [], False
            agents.append(value.lower())
        elif name:
            in_rules = True
            if name != "crawl-delay":
                continue
            try:
                seconds = float(value)
            except ValueError:
                continue
            if not math.isfinite(seconds) or seconds < 0:
                continue
            for agent in agents:
                if agent == "*" and anyone is None:
                    anyone = seconds
                elif agent not in ("", "*") and agent in user_agent.lower() and ours is None:
                    ours = seconds
    found = ours if ours is not None else anyone
    return None if found is None else min(found, MAX_CRAWL_DELAY_S)


def retry_after(value: Optional[str], now: Optional[dt.datetime] = None) -> Optional[float]:
    """Seconds a Retry-After header asks for (a number of seconds or an HTTP date), or None when absent or unreadable."""
    value = (value or "").strip()
    if value.isdigit():
        return float(value)
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=dt.timezone.utc)
    return max(0.0, (when - (now or dt.datetime.now(dt.timezone.utc))).total_seconds())


def sit_out(throttle: Optional[HostThrottle], url: str, status: int, retry_after_header: Optional[str]) -> Optional[bool]:
    """Make a host sit out a 429, or a 503 that says when to come back. None: the reply asks for no pause. False: the
    throttle now holds the host for Retry-After. True: the wait is past MAX_SIT_OUT_S, or a 429 gave none, so the caller
    leaves the host alone for the rest of the run."""
    seconds = retry_after(retry_after_header)
    if status != 429 and not (status == 503 and seconds is not None):
        return None
    if seconds is None or seconds > MAX_SIT_OUT_S:
        return True
    if throttle is not None:
        throttle.hold(url, seconds)
    return False
