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
- MS **无限重试**（was 10 in a branch — user reversed）。Each HTTP attempt goes through acquire/release so RC sees true QPS。When unreachable, RC degrades to `min_rate=1` — single concurrent keeps probing；when reachable, successes let RC climb。Accept slow, never fail。
- `_resize` clamps `_available` to `_permits` on shrink — without this, released permits accumulate beyond the new rate, defeating the rate reduction.
- When MS fund ID is missing, P2 `build_return_url` / `build_risk_url` return `""`, so `fetch()` returns `None` without making HTTP requests — safety guard for edge cases (with infinite retries, MS ID should eventually be found).
- Engine progress bar has a **spinner**（`⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏`）that rotates even when completion count stalls — user knows the program is alive, just rate-limited.
- **Sentinel strings** (`NO_DATA`, `DATA_ERROR`, `DATA_IGNORE`) distinguish "source says no data" from "crawl failed" from "intentionally skipped". Do NOT use NO_DATA to mask crawl failures.
- `PreviousReleaseVersion` branch = pre-AI-rewrite fallback.
