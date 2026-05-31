# CLAUDE.md

FundCrawler — async scraper for ~21,000 Chinese mutual funds from EastMoney + Morningstar China.
Python 3.14, `asyncio` + `aiohttp`.

## Non-obvious

- Morningstar sits behind **CloudFront WAF** that checks `Accept`/`Accept-Language`. Missing → 403/timeout. `_BASE_HEADERS` mimics browser.
- `rc.record()` called on **every HTTP attempt** (including retries). Each one is a real request hitting the server; RC sees the full QPS picture.
- Rate controllers are **domain-dimension** (one per host), not per API endpoint. Same-domain endpoints share one RC, all use 3s timeout.
- **No pipeline slots** — all funds enter Phase 1 at once. Phase 2 requests get **RC priority** over Phase 1, creating natural pipeline flow.
- AIMD with **dual threshold**: fail_rate >50% → ×0.5 (heavy backoff), ≥20% → ×0.75 (gentle), <20% → +step. MS uses `increase_step=3`, EM uses +1.
- Morningstar **starts conservative** (8 concurrent, 1.0s window); EastMoney starts aggressive (20, 0.5s window). `max_rate=200` so AIMD finds the server's limit.
- MS has **infinite retries** (3s timeout per attempt, backoff capped at 0.03s). When unreachable, RC degrades to `min_rate=1` — single concurrent keeps retrying until network recovers. Accept slow, never fail.
- **Sentinel strings** (`NO_DATA`, `DATA_ERROR`, `DATA_IGNORE`) distinguish "source says no data" from "crawl failed" from "intentionally skipped".
- `PreviousReleaseVersion` branch = pre-AI-rewrite fallback.
