"""Checks for the join-key counter: a tender notice scores high, a cookie banner and menu low, repeats count once."""

import json
from pathlib import Path

from frontier_crawler.joinkeys import join_keys

TENDER = """Tender Notice
Reference No: MOH/PROC/2026/0045
The Ministry of Health invites sealed bids for the supply of 5,000 t of cement (HS 2523.29).
Closing date: 15 December 2026, 10:00. Bid security: USD 25,000.
Contract awarded to Acme Trading Ltd and Société Générale de Construction S.A. on 2026-11-03 for EUR 1.2 million.
Vessel MV Ocean Star, IMO 9321483. Contact: procurement@health.example, Tel: +254 20 2717077.
"""
BANNER = """Home | About Us | Services | News | Contact Us
We use cookies to improve your experience. Accept All Cookies. Reject All. Manage Preferences.
Privacy Policy
Cookie Settings
Terms and Conditions
Copyright 2026. All rights reserved.
"""


def test_a_tender_notice_scores_high() -> None:
    keys = join_keys(TENDER)
    assert keys["dates"] == 2 and keys["amounts"] == 3 and keys["ids"] == 3 and keys["contacts"] == 2
    assert keys["names"] >= 3 and keys["per_100_words"] > 15


def test_a_cookie_banner_and_menu_score_low() -> None:
    assert join_keys(BANNER) == {"dates": 0, "amounts": 0, "ids": 0, "names": 0, "contacts": 0, "per_100_words": 0.0}
    assert join_keys("") == {"dates": 0, "amounts": 0, "ids": 0, "names": 0, "contacts": 0, "per_100_words": 0.0}


def test_repeated_values_count_once_and_years_and_phones_are_not_ids() -> None:
    once, thrice = join_keys(TENDER), join_keys(TENDER * 3)
    assert {k: v for k, v in once.items() if k != "per_100_words"} == {k: v for k, v in thrice.items() if k != "per_100_words"}
    assert thrice["per_100_words"] < once["per_100_words"]
    assert join_keys("In 2026 and 2025, call +44 20 7946 0958.")["ids"] == 0


def test_the_counts_go_on_a_stored_fetch_row(tmp_path: Path) -> None:
    import asyncio
    from tests.test_pipeline import FakeCascade, _engine, _job, _page, _rows

    asyncio.run(_engine(tmp_path, FakeCascade({"https://a.test/x": _page("https://a.test/x", text=TENDER)}), "r1").run([_job("https://a.test/x")]))
    (row,) = _rows(tmp_path / "store", "fetches")
    assert json.loads(row["join_keys"]) == join_keys(TENDER)
