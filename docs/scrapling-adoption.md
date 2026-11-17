# What the crawler takes from Scrapling

[Scrapling](https://github.com/D4Vinci/Scrapling) (BSD-3-Clause, read at commit bb223f2, version 0.5.1) is a scraping library with plain-HTTP and browser fetchers, a spider framework and an adaptive parser. This table lists each of its capabilities against what the engine has, and what was done about it.

Verdicts: **adopt now** (done in this change), **library dependency** (done, through a package rather than copied code), **copy the idea** (the idea, not the code; "later" when not done here), **already equal or better**, **hold for Colston** (a choice with a real cost; see the last section).

Code copied from Scrapling keeps its notice; the licence is in `licenses/Scrapling-BSD-3-Clause.txt`. Scrapling itself is not a dependency: its fetchers need its `fetchers` extra (Playwright, Patchright, browserforge and more), and the parts worth having are small.

## Fetching

| Capability | What it is | Engine today | Verdict | Why |
|---|---|---|---|---|
| TLS-fingerprint impersonation | `Fetcher` sends requests through curl_cffi with a real browser's TLS and HTTP/2 fingerprint | `cascade._impersonated`, `_curl_get`: a refused plain request (403, or a 503 with no Retry-After) or a server that hangs up on httpx is asked once more with Chrome's fingerprint, through the throttle, before the browser tier; tier `HTTP_IMPERSONATE` | library dependency (`curl_cffi`) | Many firewalls refuse by TLS fingerprint alone; a curl request costs a fraction of a browser page |
| Stealthy headers | Browser `Accept`, `Accept-Language` and the rest | `cascade.REQUEST_HEADERS` on both plain tiers | adopt now (headers); hold (browser User-Agent) | Some firewalls refuse a request without them. The User-Agent stays our own (see the last section) |
| Google referer | `referer: https://www.google.com/` on every request | none | hold for Colston | It claims a visit from a search we did not make |
| Referer flow | A followed link carries the page it was found on as its referer | none: queue entries carry the parent's fetch id, not its URL | copy the idea, later | Honest and sometimes needed, but the parent URL would have to be added to queue state |
| HTTP/3 | `http3=True` (curl `V3ONLY`) | HTTP/1.1 and HTTP/2 | hold for Colston | Scrapling warns it breaks with impersonation; QUIC over UDP is often blocked on CI runners; no refusal seen that it would fix |
| Safe redirects | `follow_redirects="safe"`: refuse a redirect to a private or internal address | httpx: `_guard_request` checks every hop's literal address; curl tier: `allow_redirects="safe"`, which checks after DNS, plus `check_public` on the final URL | already equal (httpx), adopt now (curl) | Pinned by an offline test against a local server |
| Redirect limit | `max_redirects` | `MAX_REDIRECTS = 20` on the curl tier, as httpx's default | adopt now | Both plain tiers follow the same chains |
| Size cap | `max_body_bytes` | `MAX_BYTES` streamed cap (httpx); curl `MAXFILESIZE_LARGE` | already equal; adopt now (curl) | |
| Encoding fallback | `default_encoding` and forced encoding | `readers.decode`: declared charset, then detection, legacy supersets | already equal or better | |
| Retries with delay | `retries`, `retry_delay` | timeout retry lane in `frontier.take()`; browser retry after a dead browser | already equal or better | Retries are paced by the host throttle, not a fixed delay |
| Proxy rotation | `ProxyRotator`, cyclic or custom strategy; proxy-error detection | none | hold for Colston | Only useful with paid or residential proxies |
| DNS over HTTPS | Route DNS through a DoH resolver | none | hold for Colston | Matters only with proxies |

## Browser tier

| Capability | What it is | Engine today | Verdict | Why |
|---|---|---|---|---|
| Resource blocking | `disable_resources`: drop fonts, images, media, beacons, objects, text tracks, CSP reports, websockets, stylesheets | `cascade.SKIPPED_RESOURCES`, `_screen` | adopt now (less stylesheets and websockets) | Faster pages, fewer bytes. Stylesheets decide what is hidden; some pages load their data over websockets |
| Ad and tracker blocking | `block_ads`: about 3,500 domains (Peter Lowe's list); `blocked_domains` with subdomain matching | `cascade.TRACKER_DOMAINS`, `is_tracker` (subdomain walk), 19 analytics and ad hosts | copy the idea | The subdomain walk is Scrapling's. The full list is a third-party list with its own terms; taking it is held |
| Private-address screening of sub-requests | (not in Scrapling) | `_screen` aborts every page sub-request to a private address | adopt now (our own) | A rendered page could otherwise make the browser call internal services |
| Shadow DOM piercing | `pierce_shadow`: open shadow roots copied into the HTML as `<shadow-root>` | `cascade.SHADOW_SNAPSHOT_JS` (Scrapling's script, unchanged); text from the snapshot when it has more than `inner_text` | adopt now | Web-component pages otherwise store an empty shell |
| Network idle | `network_idle` | `NETWORK_IDLE_SECONDS` wait after load, taking what is there on timeout | already equal or better | |
| Wait selector | `wait_selector`, `wait_selector_state` | none | copy the idea, later | Needs per-site configuration (a seed or derived-client field); nothing to wait for in a generic crawl |
| Network history | `record_requests`: keep the page's background requests and replies | `capture.py` (`capture` command) records them with reply shapes for derived clients; the crawl now counts JSON replies to a rendered page's XHR/fetch calls and lists the top pages in the run record as `api_backed_pages` | already equal (capture); adopt now (count in crawl) | Points derived-client work at pages with an undocumented backend without storing their replies |
| Page actions, pre-navigation setup | `page_action`, `page_setup`, `init_script` | `capture.py` clicks and scrolls | already equal | The crawl renders pages, it does not drive them |
| Session state | Cookies and storage kept, saved and loaded | a fresh context per page | already equal or better | Deliberate: nothing carries from one site to the next |
| Locale, timezone | `locale`, `timezone_id` | browser defaults | hold for Colston | Part of fingerprint shaping |
| Real Chrome, remote CDP | Use an installed Chrome or a remote browser | bundled Chromium headless shell | hold for Colston | Needs a machine with Chrome; no gain on Actions |
| Cloudflare solving | `solve_cloudflare` | none | hold for Colston | See below |
| Stealth browser | Patchright, canvas noise, WebRTC and CDP leak fixes, headless patches | none | hold for Colston | See below |

## Spiders

| Capability | What it is | Engine today | Verdict | Why |
|---|---|---|---|---|
| Sitemaps from robots.txt | `SitemapSpider` reads `Sitemap:` lines (protego) | `readers.robots_sitemaps`; a byte-order mark no longer hides the first line | already equal; adopt now (BOM) | |
| Sitemap-index descent | Child sitemaps followed recursively | indexes inside indexes followed, newest first, within `MAX_SITEMAP_FILES`, loops skipped | adopt now | Was one level |
| Sitemap alternate links | `sitemap_alternate_links`: `xhtml:link rel="alternate"` | read as entries | adopt now | Other-language pages of a site. They pass the scope fence like any sitemap URL |
| Gzipped sitemaps and feeds | Decompressed automatically | gzip by magic bytes, capped at `MAX_SITEMAP_BYTES` | already equal | |
| XML feeds | `XMLFeedSpider`: RSS and Atom items | `readers.read_feed`: a feed's item links are its links; a feed named in robots.txt reads as a sitemap; `<link rel="alternate" type="...rss/atom">` on a page is followed | adopt now | Feeds list a site's newest records |
| CSV feeds | `CSVFeedSpider`: rows as dictionaries | CSV stored whole as served | already equal | Rows are extraction work, later, not the crawler's |
| Link extraction | `LinkExtractor`: allow/deny patterns, domains, ignored extensions, canonical URLs | `readers.extract_links`, `normalise`, `_SKIP_EXTENSIONS`; per-seed allow/deny and the scope fence (PR 26). Now also `<area>` links and a `title`/`alt` anchor for image links | already equal; adopt now (`area`, anchors) | |
| robots.txt rules | `robots_txt_obey`: `can_fetch` per URL (protego) | rules not applied; Crawl-delay read | hold for Colston | See below. Nothing here weakens what the engine does |
| Crawl-delay and Request-rate | Read from robots.txt | `courtesy.crawl_delay` reads both and takes the longer; a group naming us still wins over `*` | adopt now (Request-rate) | Only ever slows the crawl |
| AutoThrottle | Per-domain delay follows response latency; doubles on a block; never below the spider's own delay | `HostThrottle.record`: after each plain reply the host's gap moves toward its response time and doubles on 403, 429, 444, 500, 502, 503 or 504, capped at `MAX_LEARNED_S` (60 s), never below the set gap or Crawl-delay; for this run only | adopt now (slowing only) | Scrapling's rule, without its speed-up below the set gap |
| Retry-After back-off | Wait what `Retry-After` asks | `courtesy.sit_out`: hold the host, or cool it past `MAX_SIT_OUT_S` | already equal or better | |
| Blocked-response detection | `is_blocked`: 401, 403, 407, 429, 444, 500, 502, 503, 504; `retry_blocked_request` | `_REFUSED_STATUS` in the line yield, `block_marker` phrases on short pages (now also "verify you are human" interstitials), browser retry on 403/503, now the fingerprint retry first | adopt now (phrases, fingerprint retry) | |
| Retry of gateway errors | 500, 502, 504 retried | not retried in the run | copy the idea, later | The retry lane is keyed to timeouts; widening it touches queue state |
| Per-domain concurrency | `concurrent_requests_per_domain` | per-host in-flight limit in `take()` | already equal | |
| Pause and resume | Checkpoints of pending requests and seen set | every run saves the whole queue and seen set (`state/queue/`); a run stops itself at `--max-minutes` | already equal or better | |
| Development cache | Responses cached to disk and replayed | every stored body is kept by hash (`bodies`); tests use fake servers and a fake browser | already equal or better | |
| Stats | Status counts, bytes per domain, timings | run record: counts, seconds, bytes by tier | already equal | |

## Parsing

| Capability | What it is | Engine today | Verdict | Why |
|---|---|---|---|---|
| Hidden-content filter (`--ai-targeted`) | Before text reaches a model, drop elements hidden by inline style (declaration by declaration, comments and strings blanked), `aria-hidden`, hidden slots, `<template>`, and zero-width and control characters | `readers.drop_hidden`, `is_hidden`, `clean_text` (Scrapling's rule; any `hidden` attribute, not only slots); in the browser tier `DROP_HIDDEN_JS` removes the same from the live page before `inner_text` | adopt now | Pages are read later by Jev and other models; hidden text is the usual prompt-injection carrier. Stored bytes stay as served |
| Markdown conversion | `markdownify` of the main content | trafilatura markdown | already equal or better | |
| Adaptive relocation | Element fingerprints stored, then found again after a site changes its markup | none | copy the idea, later | Serves per-page-shape extraction (derived HTML list clients), not the crawler |
| `find_similar` | From one element, find the others built the same way | none | copy the idea, later | Same: would let a derived client find a list's rows from one example |

## Held for Colston

Each has a real cost; none is built.

- **Cloudflare challenge solving.** More registries reachable, against a site's explicit choice to stop automated visitors; needs the stealth browser and breaks when Cloudflare changes.
- **Patched stealth browser** (Patchright, canvas noise, WebRTC and CDP leak fixes, fingerprint spoofing beyond TLS and headers). Gets past fingerprinting walls; a large, fast-moving dependency whose purpose is to look like a person.
- **Paid or residential proxy rotation.** Gets past IP bans and geo-blocks; costs money, and residential pools route through other people's connections.
- **A browser User-Agent and a Google referer.** More pages served; the crawler would stop naming itself, and robots.txt groups written for our name would no longer match how we look.
- **Obeying robots.txt rules.** The polite norm and lower ban risk; registries that disallow everything would drop out. Today only Crawl-delay and Request-rate are read.
- **HTTP/3, browser locale and timezone, real Chrome or remote browsers, DNS over HTTPS.** No refusal seen that they would fix.
- **Scrapling's full ad and tracker list.** Faster rendering; it is Peter Lowe's list under its own terms, not Scrapling's BSD licence.
