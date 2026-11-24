"""The clock (.github/workflows/clock.yml): starts this repository's daily runs on time by dispatching them.

GitHub starts scheduled runs here hours late and drops some (on 2026-11-23 two of four crawls ran), while a
dispatched run starts at once. A clock run sleeps to each slot in its window, dispatches it, and before the
6-hour job limit dispatches the next clock, which queues behind it in the `clock` group.

A slot is fired when its time is up to `grace` minutes past and no run with its title was created since that
time, so a clock that starts late catches up once and two clocks never fire one slot twice.
"""
import datetime as dt
import json
import os
import subprocess
import sys
import time

# (HH:MM UTC, workflow, run title, inputs, grace minutes). All PROVISIONAL. Titles match the workflows' run-name.
SLOTS = [
    ("00:17", "crawl.yml", "crawl crawl", {"command": "crawl"}, 90),
    ("06:17", "crawl.yml", "crawl crawl", {"command": "crawl"}, 90),
    ("12:17", "crawl.yml", "crawl crawl", {"command": "crawl"}, 90),
    ("18:17", "crawl.yml", "crawl crawl", {"command": "crawl"}, 90),
    ("03:47", "crawl.yml", "crawl ocr", {"command": "ocr"}, 120),
    ("09:47", "crawl.yml", "crawl derived", {"command": "derived"}, 120),
    # the morning pair: run records and digest after the 00:17 crawl, then the daily collection chain (about 2 hours),
    # so the guide is written before the 06:41 director session
    ("02:03", "jobs.yml", "jobs engine-sync digest", {"job": "engine-sync", "command": "digest"}, 180),
    ("02:31", "jobs.yml", "jobs collection daily", {"job": "collection", "command": "daily", "dry_run": "false"}, 240),
    ("03:37", "jobs.yml", "jobs long ", {"job": "long"}, 240),
    ("10:17", "jobs.yml", "jobs collect-daily ", {"job": "collect-daily"}, 240),
    ("11:47", "jobs.yml", "jobs scout ", {"job": "scout"}, 240),
    ("13:41", "jobs.yml", "jobs raw-checks ", {"job": "raw-checks"}, 240),
    ("16:23", "jobs.yml", "jobs engine-sync sync", {"job": "engine-sync", "command": "sync"}, 180),
]
WINDOW = dt.timedelta(hours=5, minutes=30)  # PROVISIONAL: under the 6-hour limit of a hosted job
REPO = os.environ.get("GITHUB_REPOSITORY", "howellcolston-hash/frontier-crawler")


def due(start: dt.datetime, end: dt.datetime) -> list:
    """(time, workflow, title, inputs) of every slot from start - grace to end, in time order."""
    out = []
    for day in (start.date() - dt.timedelta(days=1), start.date(), start.date() + dt.timedelta(days=1)):
        for hm, wf, title, inputs, grace in SLOTS:
            t = dt.datetime.combine(day, dt.time.fromisoformat(hm), dt.timezone.utc)
            if start - dt.timedelta(minutes=grace) <= t < end:
                out.append((t, wf, title, inputs))
    return sorted(out, key=lambda s: s[0])


def gh(*args: str) -> str:
    return subprocess.run(["gh", *args], check=True, capture_output=True, text=True).stdout


def fired(wf: str, title: str, since: dt.datetime) -> bool:
    runs = json.loads(gh("api", f"repos/{REPO}/actions/workflows/{wf}/runs?per_page=50&event=workflow_dispatch"
                              f"&created=>={since.strftime('%Y-%m-%dT%H:%M:%SZ')}"))["workflow_runs"]
    return any(r["display_title"].strip() == title.strip() for r in runs)


def dispatch(wf: str, inputs: dict) -> None:
    args = ["workflow", "run", wf, "--repo", REPO, "--ref", "main"]
    for k, v in inputs.items():
        args += ["-f", f"{k}={v}"]
    gh(*args)


def main() -> int:
    start = dt.datetime.now(dt.timezone.utc)
    end = start + WINDOW
    fails = 0
    for t, wf, title, inputs in due(start, end):
        wait = (t - dt.datetime.now(dt.timezone.utc)).total_seconds()
        if wait > 0:
            time.sleep(wait)
        try:
            if fired(wf, title, t - dt.timedelta(minutes=2)):
                print(f"{t:%H:%M} skipped: already started")
                continue
            dispatch(wf, inputs)
            print(f"{t:%H:%M} {title.strip()}: dispatched")
        except subprocess.CalledProcessError as e:  # one failed call must not stop the day's other slots
            fails += 1
            print(f"{t:%H:%M} {title.strip()}: dispatch failed ({e.returncode})")
    left = (end - dt.datetime.now(dt.timezone.utc)).total_seconds()
    if left > 0:
        time.sleep(left)
    for attempt in range(5):  # the chain's one link: without it no clock runs until the schedule restarts one
        try:
            dispatch("clock.yml", {})  # the next clock; it waits in the clock group until this run ends
            break
        except subprocess.CalledProcessError:
            time.sleep(60)
    else:
        print("next clock: dispatch failed")
        return 1
    print("next clock dispatched")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
