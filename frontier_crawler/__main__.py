"""Command line.

    python -m frontier_crawler crawl [--max-fetches N] [--max-minutes M]    follow the crawl queue from the seeds, storing and judging as it goes
    python -m frontier_crawler judge [--start D --end D]    judge stored documents that have no Jev answers yet (default: last 7 days)
    python -m frontier_crawler ocr [--start D --end D]      read stored scanned PDFs with Tesseract, then judge them (default: last 7 days)
    python -m frontier_crawler capture URL [--click TEXT]   record the data requests a page makes, to write a derived client from
    python -m frontier_crawler derived [--check ID]         run the derived API clients, or test one without storing

Environment:
    HF_TOKEN, CRAWLER_HF_REPO    Hugging Face dataset repo that holds everything: tables, crawl state, configuration, run records.
                                 Without both, the folder CRAWLER_DATA_DIR (default ./data) is used instead.
    JEV_API_KEY                  Jev key. Without it documents are stored and left pending.
    CRAWLER_JEV_MONTHLY_USD      safety ceiling on Jev spend per month in USD (default 10).
    CRAWLER_VERBOSE              set to log fetch failures and other detail. Off by default: see below.

Configuration is read from the store: config/seeds.jsonl, config/questions.json (optional) and
config/clients/*.json. What a run did is written to the store as runs/<run id>.json. Only counts
are printed, so a run's log says how much was done and not where.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from frontier_crawler import capture as capture_module
from frontier_crawler import derived as derived_module
from frontier_crawler.frontier import crawl, load_seeds
from frontier_crawler.harvester.cascade import USER_AGENT, HarvesterCascade, _guard_request
from frontier_crawler.harvester.throttle import HostThrottle
from frontier_crawler.judge import questions as judging
from frontier_crawler.judge.jev import JevClient
from frontier_crawler.pipeline import Engine
from frontier_crawler.storage.store import HubStore, LocalStore, Store

# PROVISIONAL. A stop against a runaway, set well above expected use, not a tuned budget.
DEFAULT_MONTHLY_USD = 10.0


def open_store() -> Store:
    token, repo = os.environ.get("HF_TOKEN", ""), os.environ.get("CRAWLER_HF_REPO", "")
    if token and repo:
        return HubStore(repo, token)
    print("notice: HF_TOKEN or CRAWLER_HF_REPO is not set; using the local data folder", file=sys.stderr)
    return LocalStore(Path(os.environ.get("CRAWLER_DATA_DIR", "data")))


def open_jev(http: httpx.AsyncClient) -> Optional[JevClient]:
    key = os.environ.get("JEV_API_KEY", "")
    if not key:
        print("notice: JEV_API_KEY is not set; documents are stored and left pending judgement", file=sys.stderr)
        return None
    return JevClient(key, http)


def _config(store: Store, workdir: Path, remote: str) -> Optional[Path]:
    local = workdir / remote
    return local if store.get(remote, local) else None


def _configure_questions(store: Store, workdir: Path) -> None:
    path = _config(store, workdir, "config/questions.json")
    if path is not None:
        judging.configure(json.loads(path.read_text(encoding="utf-8")))


def _specs(store: Store, workdir: Path) -> List[derived_module.Spec]:
    for remote in store.list("config/clients"):
        if remote.endswith(".json"):
            store.get(remote, workdir / remote)
    return derived_module.load_specs(workdir / "config" / "clients")


def _report(ctx: Dict[str, Any], stats: Dict[str, Any]) -> None:
    ctx["stats"] = {k: v for k, v in stats.items() if k != "shards"}


async def _crawl(args: argparse.Namespace, ctx: Dict[str, Any]) -> None:
    store = ctx["store"]
    monthly = float(os.environ.get("CRAWLER_JEV_MONTHLY_USD", DEFAULT_MONTHLY_USD))
    with tempfile.TemporaryDirectory(prefix="crawler-crawl-") as workdir:
        seeds_path = _config(store, Path(workdir), "config/seeds.jsonl")
        if seeds_path is None:
            raise SystemExit("no config/seeds.jsonl in the store")
        seeds = load_seeds(seeds_path)
        _configure_questions(store, Path(workdir))
        async with httpx.AsyncClient() as http, HarvesterCascade(throttle=HostThrottle(args.min_interval)) as cascade:
            engine = Engine(store, cascade, workdir=Path(workdir), jev=open_jev(http), monthly_usd=monthly, concurrency=args.concurrency)
            stats = await crawl(store, engine, seeds, workdir=Path(workdir), max_fetches=args.max_fetches, batch=args.batch, max_minutes=args.max_minutes)
    _report(ctx, stats)
    if stats["errors"] or stats["seeds_with_nothing"]:
        ctx["status"] = "partial"
    if stats["total"] and stats["errors"] == stats["total"]:
        raise RuntimeError("every fetch failed")


async def _judge(args: argparse.Namespace, ctx: Dict[str, Any]) -> None:
    end = dt.date.fromisoformat(args.end) if args.end else dt.datetime.now(dt.timezone.utc).date()
    start = dt.date.fromisoformat(args.start) if args.start else end - dt.timedelta(days=6)
    store = ctx["store"]
    monthly = float(os.environ.get("CRAWLER_JEV_MONTHLY_USD", DEFAULT_MONTHLY_USD))
    async with httpx.AsyncClient() as http:
        jev = open_jev(http)
        if jev is None and args.command == "judge":
            ctx["status"] = "skipped"
            ctx["notes"].append("JEV_API_KEY is not set")
            return
        with tempfile.TemporaryDirectory(prefix="crawler-judge-") as workdir:
            _configure_questions(store, Path(workdir))
            engine = Engine(store, workdir=Path(workdir), jev=jev, monthly_usd=monthly, concurrency=args.concurrency)
            await engine.start()
            days = [start + dt.timedelta(days=n) for n in range((end - start).days + 1)]
            try:
                await (engine.ocr_days(days) if args.command == "ocr" else engine.judge_days(days))
            finally:
                stats = await engine.finish()
    _report(ctx, stats)
    ctx["notes"].append(f"covered fetch days {start} to {end}; earlier days need --start")
    if stats["jev_stopped"]:
        ctx["status"] = "partial"


def _api_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=30.0, headers={"User-Agent": USER_AGENT}, event_hooks={"request": [_guard_request]})


async def _capture(args: argparse.Namespace) -> None:
    """A local tool: writes the capture under --out and prints the requests it saw. Not for a public log."""
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    result = await capture_module.capture(args.url, clicks=args.click or [], screenshot=path.with_suffix(".png"))
    path.write_text(json.dumps(result, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    data = [e for e in result["entries"] if e["first_party"] and not e["noise"] and e["json_shape"] is not None]
    print(f"capture written to {path}: {result['requests_seen']} requests seen, {len(data)} look like data")
    for entry in result["entries"][:25]:
        flag = "data " if entry in data else "     "
        print(f"{flag}{entry['method']:4} {entry['status']} {entry['bytes']:>8}  {entry['url'][:160]}")
    for action in result["actions"]:
        print(action)


async def _derived(args: argparse.Namespace, ctx: Dict[str, Any]) -> None:
    store = ctx["store"]
    with tempfile.TemporaryDirectory(prefix="crawler-derived-") as workdir:
        specs = derived_module.load_specs(Path(args.clients)) if args.clients else _specs(store, Path(workdir))
        async with _api_client() as http:
            if args.check:  # a local test call: prints the reply's shape, stores nothing
                (spec,) = [s for s in specs if s.id == args.check] or [None]
                if spec is None:
                    raise SystemExit(f"no client named {args.check}")
                print(json.dumps(await derived_module.check(spec, http), indent=1, ensure_ascii=False))
                return
            fetcher = derived_module.DerivedFetcher(http, HostThrottle(args.min_interval))
            engine = Engine(store, fetcher, workdir=Path(workdir), concurrency=1)
            await engine.start()
            try:
                results = await derived_module.run_derived(engine, specs, fetcher)
            finally:
                stats = await engine.finish()
    broken = {name: result["status"] for name, result in results.items() if result["status"] != "ok"}
    _report(ctx, {**stats, "clients": results, "broken_clients": broken})
    if broken:
        ctx["status"] = "partial"  # each one needs deriving again


def _run(args: argparse.Namespace, params: Dict[str, Any], job: Any) -> None:
    """Run one command, then write what happened to the store as runs/<run id>.json and print counts only."""
    started = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    ctx: Dict[str, Any] = {"store": open_store(), "status": "ok", "notes": [], "stats": {}}
    error: Optional[BaseException] = None
    try:
        asyncio.run(job(args, ctx))
    except BaseException as exc:  # recorded in the store; the message may name a site, so it is not printed
        ctx["status"], error = "failed", exc
        ctx["notes"].append(f"{type(exc).__name__}: {exc}")
    stats = ctx["stats"]
    record = {
        "command": args.command, "status": ctx["status"], "started": started.isoformat(),
        "finished": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        "params": params, "notes": ctx["notes"], "stats": stats,
        "runner": f"github:{os.environ['GITHUB_RUN_ID']}" if os.environ.get("GITHUB_RUN_ID") else "local",
    }
    name = f"{started.strftime('%Y%m%dT%H%M%SZ')}-{args.command}.json"
    with tempfile.TemporaryDirectory(prefix="crawler-run-") as workdir:
        path = Path(workdir) / name
        path.write_text(json.dumps(record, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
        try:
            ctx["store"].put_many([(path, f"runs/{name}")], f"run record {name}")
        except Exception as exc:  # the store's own errors name the repository
            print(f"run record not written: {type(exc).__name__}", file=sys.stderr)
    counts = {k: v for k, v in stats.items() if isinstance(v, (int, float)) and not isinstance(v, bool)}
    print(json.dumps({"status": ctx["status"], "stopped_by": stats.get("stopped_by"), **counts}, sort_keys=True, indent=1))
    if error is not None:
        raise SystemExit(f"run failed: {type(error).__name__} (details are in the run record)")


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m frontier_crawler", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    crawler = sub.add_parser("crawl", help="follow the crawl queue from the seeds")
    crawler.add_argument("--max-fetches", type=int, default=200, help="most fetches in this run")
    crawler.add_argument("--max-minutes", type=float, default=None, help="stop starting new rounds after this many minutes")
    crawler.add_argument("--batch", type=int, default=50, help="fetches per round; the queue is re-ranked between rounds")
    crawler.add_argument("--min-interval", type=float, default=1.0, help="seconds between requests to one host")
    crawler.add_argument("--concurrency", type=int, default=4)
    for name, text in (("judge", "judge stored documents that have no Jev answers"), ("ocr", "read stored scanned PDFs with Tesseract")):
        later = sub.add_parser(name, help=text)
        later.add_argument("--start")
        later.add_argument("--end")
        later.add_argument("--concurrency", type=int, default=4)
    capturer = sub.add_parser("capture", help="record the data requests a page makes")
    capturer.add_argument("url")
    capturer.add_argument("--click", action="append", help="text of something to click after the page loads; repeatable")
    capturer.add_argument("--out", default="captures", help="folder the capture is written to")
    clients = sub.add_parser("derived", help="run the derived API clients")
    clients.add_argument("--check", metavar="ID", help="call one client for real and describe the reply, storing nothing")
    clients.add_argument("--clients", metavar="DIR", help="read client specs from this folder instead of the store")
    clients.add_argument("--min-interval", type=float, default=1.0)
    args = parser.parse_args(argv)
    # Log lines name the pages they are about, so they are off unless asked for.
    logging.basicConfig(level=logging.INFO if os.environ.get("CRAWLER_VERBOSE") else logging.CRITICAL, format="%(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    if args.command == "capture":
        asyncio.run(_capture(args))
    elif args.command == "derived" and args.check:
        asyncio.run(_derived(args, {"store": open_store()}))
    elif args.command == "crawl":
        _run(args, {"max_fetches": args.max_fetches, "max_minutes": args.max_minutes}, _crawl)
    elif args.command == "derived":
        _run(args, {}, _derived)
    else:
        _run(args, {"start": args.start, "end": args.end}, _judge)


if __name__ == "__main__":
    main()
