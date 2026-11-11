# frontier-crawler

A generic web crawler: fetch, store, judge, follow links. Configuration, crawl state and data live in a private store, never in this repository.

- Use the `ponytail` skill (`.claude/skills/ponytail/`, DietrichGebert/ponytail, MIT, copied unchanged) for every code change here, and tell any subagent you brief to load it first. Paraphrasing it in a brief is not enough.
- The crawler meets opposition (blocks, rate limits, broken sites), so keep its safeguards and tests even where a lean-code rule would call them redundant. Remove code that is no longer used.
- This repository and its Actions logs are public. Never log, print or commit URLs, hosts, seed names or page text: counts, seconds and bytes only.
- Tunable constants are marked `PROVISIONAL`. Keep that mark on new ones.
- Tests are offline (`python -m pytest -q`). Every change keeps them passing.
