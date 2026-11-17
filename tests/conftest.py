"""Shared test setup. Tests are offline: the fingerprint tier (curl_cffi) never goes to the network unless a test swaps in
its own _curl_get."""

from __future__ import annotations

from typing import Any

import pytest

from frontier_crawler.harvester.cascade import HarvesterCascade


@pytest.fixture(autouse=True)
def offline_fingerprint_tier(monkeypatch: pytest.MonkeyPatch) -> None:
    async def offline(self: Any, url: str, limits: Any) -> Any:
        raise ConnectionError("tests are offline")

    monkeypatch.setattr(HarvesterCascade, "_curl_get", offline)
