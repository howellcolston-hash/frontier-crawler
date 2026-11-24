import datetime as dt

from tools import clock

UTC = dt.timezone.utc


def at(h, m, day=24):
    return dt.datetime(2026, 11, day, h, m, tzinfo=UTC)


def test_window_and_catch_up():
    s = clock.due(at(0, 30), at(6, 0))
    titles = [(t.strftime("%H:%M"), title) for t, _, title, _ in s]
    # 00:17 crawl is 13 minutes late, inside its grace; 06:17 is past the window's end
    assert titles[0] == ("00:17", "crawl crawl") and ("06:17", "crawl crawl") not in titles
    assert [t for t, _, _, _ in s] == sorted(t for t, _, _, _ in s)
    assert ("02:31", "jobs collection daily") in titles


def test_grace_drops_stale_slots_and_wraps_midnight():
    s = clock.due(at(23, 0), at(23, 0) + clock.WINDOW)
    titles = [(t.strftime("%H:%M"), title) for t, _, title, _ in s]
    assert ("18:17", "crawl crawl") not in titles  # 4h43 late: over its grace
    assert ("00:17", "crawl crawl") in titles and ("02:31", "jobs collection daily") in titles


def test_every_slot_has_a_title_the_workflows_produce():
    for hm, wf, title, inputs, grace in clock.SLOTS:
        dt.time.fromisoformat(hm)
        assert grace > 0
        if wf == "crawl.yml":
            assert title == f"crawl {inputs['command']}"
        else:
            assert title == f"jobs {inputs['job']} {inputs.get('command', '')}"
