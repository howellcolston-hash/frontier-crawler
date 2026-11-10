# frontier-crawler

A crawler that starts from a few seed pages, follows links outward on its own, stores every page it is served, and judges the pages afterwards.

- **Store first, judge later.** Pages are saved before any model reads them. Each answer about a page is stored as a probability, so thresholds can change later without asking again.
- **Lines, not a fixed list.** A site that yields content becomes its own line of the crawl. A line that stops paying freezes and keeps its queue; a link from elsewhere can thaw it. Priority falls with what is already held from a site, so the crawl spreads out instead of going deep on one place.
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
| `clients/*.json` | optional: derived client specs |

Judging uses [Jev](https://docs.typesafe.ai) and needs `JEV_API_KEY`. Without it pages are stored and left pending.

## Logs

A run prints counts only. What it fetched, and anything that went wrong, is written to the store as `runs/<run id>.json`. Set `CRAWLER_VERBOSE=1` for detail on a machine whose logs are private.

## Tests

`python -m pytest -q`. They are offline.

## Licence

AGPL-3.0. See `LICENSE`.
