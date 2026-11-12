# frontier-crawler

A crawler that starts from a few seed pages, follows links outward on its own, stores every page it is served, and judges the pages afterwards.

- **Store first, judge later.** Pages are saved before any model reads them. Each answer about a page is stored as a probability, so thresholds can change later without asking again.
- **Lines, not a fixed list.** A site that yields content becomes its own line of the crawl, and that first page counts toward the new line: a line's record is its own pages. A line that stops paying sinks and keeps its queue; links into it from better lines lift it, never above what its own judged pages are worth, and a small share of fetches re-tests groups drawn at random. Priority falls with each line between a line and its seed, for good (a link to a new site is priced as the line it would become), and with what is already held from a site and from its registered domain (example.com for www.example.com), so the crawl reads a site well before it moves on, without going deep on one place or drifting far from its seeds. A small share of fetches goes to registered domains never fetched. An optional second rule (`ranking.json`, below) replaces these set factors with an estimate per site learned from what its fetches were worth, and explores by sampling from it.
- **Three ways to read a page**: plain HTTP, a headless browser when a page is built by script, and OCR (Tesseract) for scanned PDFs.
- **Derived clients.** Where a page loads its figures from a backend, a small JSON spec can fetch that backend directly; a client whose replies change shape is reported as broken.
- **One store.** Tables (Parquet), crawl state, configuration and run records live together in a Hugging Face dataset repository, or in a local folder.

## Run it

```
pip install -r requirements.txt
python -m playwright install --only-shell chromium
mkdir -p data && cp -r examples/config data/
python -m frontier_crawler crawl --max-minutes 2
```

That crawls from `examples/config/seeds.jsonl` into `./data`. `python -m frontier_crawler --help` lists the other commands (`judge`, `ocr`, `capture`, `derived`), and the top of `frontier_crawler/__main__.py` lists the environment variables.

## Configuration

Read from the store's `config/` folder at the start of a run:

| file | what |
|---|---|
| `seeds.jsonl` | starting pages, one JSON object per line |
| `questions.json` | optional: the questions asked about each document and how the answers combine into relevance. The format and the built-in default are `EXAMPLE` in `frontier_crawler/judge/questions.py` |
| `steer.json` | optional: a weight per site, `{"version": 1, "sites": {"example.com": 0.5}}`, keyed by registered domain (subdomains included). It multiplies priority: 1 is neutral, below 1 sinks a site, above 1 lifts it, 0 stops fetching from it while keeping its queue. A malformed file is ignored and counted in the run record |
| `watch.json` | optional: pages to re-read on a schedule, `{"version": 1, "urls": [{"id": "notices", "url": "https://example.com/notices", "every_hours": 24}]}`. At the start of a crawl, each one never read or read at least `every_hours` ago is fetched before the ranked queue, oldest due first, up to a fifth of the run's fetches. Its fetch row has `line_id` `watch:<id>`; when its bytes changed, the links on it that the queue has not seen are queued under its site's line. A malformed file or entry is ignored and counted in the run record |
| `ranking.json` | optional: how the queue ranks, `{"version": 1, "mode": "bandit", "temperature": 1}`. `fixed` (the default) is the priority rule in `frontier_crawler/frontier.py`; `bandit` allocates fetches by Thompson sampling over each site's discounted record of what its fetches were worth, with `temperature` scaling the randomness (0 always takes the best estimate, 1 is the default); `shadow` ranks as `fixed` and records what `bandit` would have taken. A malformed file or unknown mode runs `fixed` and is counted in the run record |
| `clients/*.json` | optional: derived client specs |

Judging uses [Jev](https://docs.typesafe.ai) and needs `JEV_API_KEY`. Without it pages are stored and left pending.

## Tables

Parquet shards in the store, in folders by the UTC day the fetching run started (`<table>/year=/month=/day=/`):

| table | one row per |
|---|---|
| `fetches` | fetch attempt: where the URL came from, status, outcome, content hash, size, text and `title` (an HTML page's `<title>` or a PDF's metadata title, at most 300 characters; absent from shards before engine 0.4.0, so read it as nullable) and, for a stored page, `join_keys`: JSON counts of the distinct dates, amounts, ids, names and contacts in its text, and their total per 100 words (regular expressions, `frontier_crawler/joinkeys.py`; absent before engine 0.5.0). Rows from engine 0.2.0 and earlier also carry the bytes in `body` |
| `bodies` | content hash stored: the bytes as served (rendered HTML for the browser tier). Identical bytes are stored once, across runs; `python -m frontier_crawler body SHA256` writes one out |
| `links` | distinct link on a stored HTML page, in page order, whether or not the queue took it |
| `ocr_text` | scanned PDF read with Tesseract, with `join_keys` for the text read (from engine 0.5.0) |
| `judgement_calls`, `judgements` | Jev call about a document, and each answer in it |

Crawl state (`state/`) holds the queue, lines, hosts (with when their sitemaps were last read), what each URL last served (`url_hashes.parquet`: 64-bit keys), when each watched page was last read, the set of stored body hashes and Jev spend.

The queue (`state/queue/`) is kept on disk so a run's memory does not grow with it: queued entries in bucket files sorted by group (a line, a host and a seed), every other URL it has known as a 64-bit key in `seen.parquet`, the few entries kept whole (retries, unparseable links, fetched starting URLs) in `kept.parquet`, and `manifest.json`, written last. A run loads only the best-ranked groups and a random sample of the rest, more as it needs them; a link into a group left on disk is appended to it unread. A group keeps at most 1,000 entries and the queue 5 million; what is cut is counted in the run record and can be recovered from the `links` table. A store with only the older `frontier.parquet` and `url_state.parquet` is read from those once; they are left in place.

## Logs

A run prints counts only. What it fetched, and anything that went wrong, is written to the store as `runs/<run id>.json`. Among its fields:

- `seed_url_problems`: starting URLs that served nothing, a block page or a thin page (HTML under 20 words, such as an outage notice). Each is tried again in the next run. A thin page counts as no content in its line's yield.
- `seed_url_redirects`: starting URLs that were sent to another site, with the site they ended on.
- `sitemap_sites_read`, `sitemaps_read`, `sitemap_failures`, `sitemap_urls_queued`: a seed's starting sites, and sites on a domain steered above 1, have their `robots.txt` and the sitemaps it names (else `/sitemap.xml`) read at most once a week; up to 500 new URLs a site, newest first, join the queue as ordinary links. This only finds pages: `robots.txt` rules are not applied.
- `ranking`: `mode`, `temperature`, `arms` (groups with queued URLs), `arms_tried` (taken from in this run); in `bandit` mode `greedy_takes` and `exploration_takes` (picks that were not the best estimate) with `mean_reward_greedy` and `mean_reward_exploration`; in `shadow` mode `shadow_takes`, `shadow_agreed` (takes where `bandit` would have taken the same URL), `shadow_mean_reward_agreed` and `shadow_mean_reward_differed` (what `fixed`'s picks were worth, split by that), and `shadow_mean_estimate_fixed` and `shadow_mean_estimate_bandit` (the bandit's estimate of each rule's picks). `ranking_file_ignored` counts a malformed `ranking.json`.
- `queue`: `groups_hot` and `groups_cold`, `entries_hot` and `entries_cold` (in memory, or left on disk), `groups_promoted` (loaded beyond the first ranked choice), `entries_appended_cold`, `cut_group_cap` and `cut_total_cap`, `seen` (URLs known), `scans` (passes over the queue on disk), `load_seconds`, `select_seconds`, `save_seconds`, `bytes_in` and `bytes_out` for the queue's state, `peak_rss_mb_after_select`, and `migrated` for the run that read the older file. `peak_rss_mb` is the whole run's.
- `watch_entries`, `watch_due`, `watch_fetched`, `watch_changed`, `watch_unchanged`, `watch_failed`, `watch_links_queued`, `watch_file_ignored`, `watch_entries_ignored`: the watched pages of `config/watch.json`. Set `CRAWLER_VERBOSE=1` for detail on a machine whose logs are private.

## Tests

`python -m pytest -q`. They are offline.

## Licence

AGPL-3.0. See `LICENSE`.
