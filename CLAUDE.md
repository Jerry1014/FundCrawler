# CLAUDE.md

FundCrawler — async scraper for ~21,000 Chinese mutual funds from EastMoney + Morningstar China.
Python 3.14, `asyncio` + `aiohttp`.

## Non-obvious

- Morningstar sits behind **CloudFront WAF** that checks `Accept`/`Accept-Language`. Missing → 403/timeout. `_BASE_HEADERS` mimics browser.
- `rc.record()` called **once per `fetch()`, on final outcome only**. Per-attempt recording counts transient retry-failures as real failures, falsely triggering AIMD rate reduction.
- Rate controllers are **domain-dimension** (one per host), not per API endpoint. Same-domain endpoints share one RC but get different timeouts (search 8s, quicktake 12s).
- **No pipeline slots** — all funds enter Phase 1 at once. Phase 2 requests get **RC priority** over Phase 1, creating natural pipeline flow without artificial limits.
- Morningstar **starts conservative** (8 concurrent, 1.0s window) to avoid anti-crawl; EastMoney starts aggressive (20, 0.5s window). `max_rate=200` is intentionally high so AIMD finds the **server's real limit**, not our ceiling.
- **Sentinel strings** (`NO_DATA`, `DATA_ERROR`, `DATA_IGNORE`) distinguish "source says no data" from "crawl failed" from "intentionally skipped".
- `PreviousReleaseVersion` branch = pre-AI-rewrite fallback.