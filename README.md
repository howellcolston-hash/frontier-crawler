# frontier-crawler

A crawler that starts from a few seed pages, follows links outward on its own, stores every page it is served, and judges the pages afterwards.

- **Store first, judge later.** Pages are saved before any model reads them. Each answer about a page is stored as a probability, so thresholds can change later without asking again.
- **Lines, not a fixed list.** A site that yields content becomes its own line of the crawl, and that first page counts toward the new line: a line's record is its own pages. A line that stops paying sinks and keeps its queue; links into it from better lines lift it, never above what its own judged pages are worth, and a small share of fetches re-tests groups drawn at random. Priority falls with each line between a line and its seed, for good (a link to a new site is priced as the line it would become), and with what is already held from a site and from its registered domain (example.com for www.example.com), so the crawl reads a site well before it moves on, without going deep on one place or drifting far from its seeds. A small share of fetches goes to registered domains never fetched. An optional second rule (`ranking.json`, below) replaces these set factors with an estimate per site learned from what its fetches were worth, and explores by sampling from it.
- **Three ways to read a page**: plain HTTP, a headless browser when a page is built by script, and OCR (Tesseract) for scanned PDFs.
- **Derived clients.** Where a page loads its figures from a backend, a small JSON spec can fetch that backend directly; a client whose replies change shape is reported as broken. A spec can also page through an HTML list the crawl cannot reach (form posts, deep paging) and queue its detail pages for the crawl.
- **Courtesy.** Requests to one host are spaced; a robots.txt `Crawl-delay` lengthens the gap, and a server that answers 429, or 503 with `Retry-After`, is waited out or left alone for the rest of the run.
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
| `seeds.jsonl` | starting pages, one JSON object per line (fields below) |
| `questions.json` | optional: the questions asked about each document and how the answers combine into relevance. The format and the built-in default are `EXAMPLE` in `frontier_crawler/judge/questions.py`. An optional `jev_sample`, 0 to 1, is the largest share of new documents Jev is asked about: the smaller of it and the share the budget allows is taken, by content hash, so a rerun picks the same documents, and the `judge` and `ocr` commands keep to the same documents |
| `steer.json` | optional: a weight per site, `{"version": 1, "sites": {"example.com": 0.5}}`, keyed by registered domain (subdomains included). It multiplies priority: 1 is neutral, below 1 sinks a site, above 1 lifts it, 0 stops fetching from it while keeping its queue. A malformed file is ignored and counted in the run record |
| `watch.json` | optional: pages to re-read on a schedule, `{"version": 1, "urls": [{"id": "notices", "url": "https://example.com/notices", "every_hours": 24}]}`. At the start of a crawl, each one never read, or read at least its interval ago, is fetched before the ranked queue, oldest due first, up to a fifth of the run's fetches. Its fetch row has `line_id` `watch:<id>`; when its bytes changed, the links on it that the queue has not seen are queued under its site's line. Its interval follows its own change rate: from its re-reads that served changed or unchanged bytes and the hours between them, Cho and Garcia-Molina's estimator for a Poisson change process gives a rate, with `every_hours` as the prior (it is the interval until there is history), and the page is due when the chance it changed since its last read reaches one half, within 6 hours to 30 days (all provisional, `WATCH_*` in `frontier_crawler/frontier.py`). A malformed file or entry is ignored and counted in the run record |
| `ranking.json` | optional: how the queue ranks, `{"version": 1, "mode": "bandit", "temperature": 1, "scope": "source"}`. `fixed` (the default) is the priority rule in `frontier_crawler/frontier.py`; `bandit` allocates fetches by Thompson sampling over each site's discounted record of what its fetches were worth, with `temperature` scaling the randomness (0 always takes the best estimate, 1 is the default); `shadow` ranks as `fixed` and records what `bandit` would have taken. `scope` is `source` (the default) or `open`: under `source` a seed queues only URLs on its own sites (the registered domains of its `urls` and its `sites`); a link off them is kept in the `links` table and never queued, so it starts no line, and the same holds for sitemap URLs and the links of watched pages. One exception: a link off the sites, found on a page of the seed's own sites, is queued in that page's line when its path ends in a document extension (`DOCUMENT_EXTENSIONS`, provisional: .pdf .xls .xlsx .csv .doc .docx .ods .odt .json .xml .zip), for record files served from another host; it starts no line, its own links are fenced, and it counts toward the seed's allowance and run share. Entries queued under `open` stay queued and are not taken. `open` follows any link, as before. A malformed file, unknown mode or unknown scope runs `fixed` with `source` and is counted in the run record |
| `clients/*.json` | optional: derived client specs (below) |

A seed line: `{"id": "notices", "entity": "Example notices", "urls": ["https://www.example.com/notices"], "sites": ["example.org"], "allow": ["/notices/"], "deny": ["\\.zip$"], "allowance": 5000}`.

| field | meaning |
|---|---|
| `id`, `entity`, `urls` (or `url`) | required: a unique id, what the pages are about, its starting pages |
| `weight`, `until`, `note` | optional: its share (1 neutral, 0 off), the last day it is followed, a note |
| `sites` | optional: registered domains beside those of its `urls` that it may queue under scope `source` |
| `allow`, `deny` | optional: regular expressions (searched anywhere in the URL). A URL it queues, past its starting pages, must match one `allow` pattern if any are given and no `deny` pattern. A malformed pattern is ignored and counted in the run record (`seed_patterns_ignored`) |
| `allowance` | optional: the most pages it may ever store, all its lines and runs together (counted from `state/lines.parquet`). Once reached, its entries stay queued and are not taken |

No seed, all its lines together, takes more than 15% (`SEED_RUN_SHARE`, provisional) of a run's `--max-fetches`; past it, its entries stay queued. This holds in every ranking mode and scope.

## Derived clients

One JSON file per client in `config/clients/`, named by its `id`. `python -m frontier_crawler derived` runs them all once; `derived --check ID` calls one for real and describes the reply without storing it.

```json
{
  "id": "example-notices",
  "entity": "Example notices",
  "seed_id": "example",
  "reply": "html",
  "request": {"method": "POST", "url": "https://www.example.test/notices/search", "form": {"q": "", "type": "all"}},
  "paginate": {"in": "form", "field": "page", "start": 1, "step": 1, "max_pages": 3000},
  "fingerprint": {"contains": ["Notice list", "Published"], "min_bytes": 2000},
  "follow": "^https://www\\.example\\.test/notices/\\d+$",
  "min_interval_s": 3
}
```

| field | meaning |
|---|---|
| `id`, `entity`, `seed_id` | the file name; what the data is about and the seed it belongs to, stored on every fetch row (`line_id` `derived:<id>`) |
| `request` | `method` (GET or POST), `url`, optional `params` (query string), `headers`, and a body: `json`, or `form` (a POST only, sent form-encoded) |
| `paginate` | optional. `field` is set to `start`, `start + step`, … in `params`, `json` (dotted path allowed) or `form`, up to `max_pages` |
| `reply` | `json` (default) or `html` |
| `records_path`, `fingerprint.keys`, `fingerprint.min_records` | `json`: the dotted path to the record list (empty when the reply is the list), the keys the records must have and the records the first page must hold. Paging stops at a page with no records |
| `fingerprint.contains`, `fingerprint.min_bytes` | `html`: strings every healthy page holds, and the first page's least size. Paging stops at a non-200, a page lacking the strings, or a page with the same bytes as the one before |
| `follow` | optional, `html` only: a regular expression; links on each page whose absolute URL matches it join the crawl queue as ordinary links of the seed's line, so the crawl fetches the detail pages |
| `min_interval_s` | optional: seconds between requests to the spec's host, when longer than the run's gap |

A client whose first page is not a 200 or no longer matches its fingerprint is listed under `broken_clients` in the run record. Headers never hold cookies, authorization, API-key or CSRF values: the loader refuses a spec that has them. A run loads the crawl queue only when a spec has `follow`.

Judging uses [Jev](https://docs.typesafe.ai) and needs `JEV_API_KEY`. Without it pages are stored and left pending.

## Tables

Parquet shards in the store, in folders by the UTC day the fetching run started (`<table>/year=/month=/day=/`):

| table | one row per |
|---|---|
| `fetches` | fetch attempt: where the URL came from, status, outcome, content hash, size, text and `title` (an HTML page's `<title>` or a PDF's metadata title, at most 300 characters; absent from shards before engine 0.4.0, so read it as nullable) and, for a stored page, `join_keys`: JSON counts of the distinct dates, amounts, ids, names and contacts in its text, and their total per 100 words (regular expressions, `frontier_crawler/joinkeys.py`; absent before engine 0.5.0). From engine 0.6.0 the text leaves out elements a reader cannot see (hidden by markup or inline style) and zero-width characters, so a model reading it is not handed hidden instructions; `tier_used` `HTTP_IMPERSONATE` marks a page served to a browser's TLS fingerprint after plain HTTP was refused. From engine 0.7.0, `etag` and `last_modified` hold those response headers from the plain tiers. Rows from engine 0.2.0 and earlier also carry the bytes in `body` |
| `bodies` | content hash stored: the bytes as served (rendered HTML for the browser tier, with any open shadow roots copied in as `<shadow-root>` elements). Identical bytes are stored once, across runs; `python -m frontier_crawler body SHA256` writes one out. From engine 0.7.0 a PDF whose text was read in full is kept as text only: its fetch row is complete and it has no body here. A scan (under 20 words, `ocr.SCAN_MAX_WORDS`) keeps its bytes, because its text does not exist until the `ocr` command reads them and OCR is lossy; so does a PDF whose reading hit the time limit. `CRAWLER_PDF_BODIES=keep` keeps every PDF's bytes |
| `links` | distinct link on a stored HTML page, in page order, whether or not the queue took it |
| `ocr_text` | scanned PDF read with Tesseract, with `join_keys` for the text read (from engine 0.5.0) |
| `judgement_calls`, `judgements` | Jev call about a document, and each answer in it |

Crawl state (`state/`) holds the queue, lines, hosts (with when their sitemaps were last read and their `Crawl-delay`), what each URL last served (`url_hashes.parquet`: 64-bit keys), when each watched page was last read (with its re-reads, how many changed and the hours between them), the set of stored body hashes and Jev spend.

The queue (`state/queue/`) is kept on disk so a run's memory does not grow with it: queued entries in bucket files sorted by group (a line, a host and a seed), every other URL it has known as a 64-bit key in `seen.parquet`, the few entries kept whole (retries, unparseable links, fetched starting URLs) in `kept.parquet`, and `manifest.json`, written last. A run loads only the best-ranked groups and a random sample of the rest, more as it needs them; a link into a group left on disk is appended to it unread. A group keeps at most 1,000 entries and the queue 5 million; what is cut is counted in the run record and can be recovered from the `links` table. A store with only the older `frontier.parquet` and `url_state.parquet` is read from those once; they are left in place.

## Logs

A run prints counts only. What it fetched, and anything that went wrong, is written to the store as `runs/<run id>.json`. Among its fields:

- `seed_url_problems`: starting URLs that served nothing, a block page or a thin page (HTML under 20 words, such as an outage notice). Each is tried again in the next run. A thin page counts as no content in its line's yield.
- `seeds_with_nothing`: active seeds whose own line has been fetched and has never stored anything, usually a dead or blocked starting URL. Inactive seeds (past `until`, or weight 0) are left out.
- `seed_url_redirects`: starting URLs that were sent to another site, with the site they ended on.
- `sitemap_sites_read`, `sitemaps_read`, `sitemap_failures`, `sitemap_urls_queued`: a seed's starting sites, and sites on a domain steered above 1, have their `robots.txt` and the sitemaps it names (else `/sitemap.xml`) read at most once a week; up to 500 new URLs a site, newest first, join the queue as ordinary links. Its `Crawl-delay` for this crawler (the group whose User-agent is `frontier-crawler`; else for `*`), capped at 30 seconds, becomes the site's gap between requests when longer than the run's, and is kept in `state/hosts.parquet` for later runs and for the derived clients. This only finds pages and sets the pace: `robots.txt` rules are not applied.
- `scope`, `links_out_of_scope`, `links_off_pattern`, `links_known`, `documents_off_site`, `seed_patterns_ignored`, `seeds_at_allowance`: the scope that ran, links kept out of the queue by the scope or by a seed's patterns, links on stored pages not queued because the queue already knew them (queued or fetched under any line; a link found on several pages counts once a page), documents off a seed's sites queued from its pages, malformed patterns ignored, and the seeds that have stored their allowance.
- `pdf_bodies`, `pdf_text_only`, `pdf_text_only_bytes`: the PDF setting, and the PDFs stored as text only with the bytes not written for them.
- `hosts_cooled`: hosts left alone for the rest of the run, after 3 fetches in a row that served nothing, or a 429, or a `Retry-After` longer than 5 minutes (a shorter one is waited out).
- `ranking`: `mode`, `temperature`, `arms` (groups with queued URLs), `arms_tried` (taken from in this run); in `bandit` mode `greedy_takes` and `exploration_takes` (picks that were not the best estimate) with `mean_reward_greedy` and `mean_reward_exploration`; in `shadow` mode `shadow_takes`, `shadow_agreed` (takes where `bandit` would have taken the same URL), `shadow_mean_reward_agreed` and `shadow_mean_reward_differed` (what `fixed`'s picks were worth, split by that), and `shadow_mean_estimate_fixed` and `shadow_mean_estimate_bandit` (the bandit's estimate of each rule's picks). `ranking_file_ignored` counts a malformed `ranking.json`.
- `queue`: `groups_hot` and `groups_cold`, `entries_hot` and `entries_cold` (in memory, or left on disk), `groups_promoted` (loaded beyond the first ranked choice), `entries_appended_cold`, `cut_group_cap` and `cut_total_cap`, `seen` (URLs known), `scans` (passes over the queue on disk), `load_seconds`, `select_seconds`, `save_seconds`, `bytes_in` and `bytes_out` for the queue's state, `peak_rss_mb_after_select`, and `migrated` for the run that read the older file. `peak_rss_mb` is the whole run's.
- `watch_entries`, `watch_due`, `watch_fetched`, `watch_changed`, `watch_unchanged`, `watch_failed`, `watch_links_queued`, `watch_file_ignored`, `watch_entries_ignored`: the watched pages of `config/watch.json`. `watch_intervals` counts watched pages by their interval in hours, `watch_with_history` those with re-reads to estimate from, `watch_sooner` and `watch_later` those due more or less often than their `every_hours`, and `watch_changes_per_day` the 10th, 50th and 90th percentile of the estimated change rate of those with history. Set `CRAWLER_VERBOSE=1` for detail on a machine whose logs are private.

## Tests

`python -m pytest -q`. They are offline.

## Licence

AGPL-3.0. See `LICENSE`.
