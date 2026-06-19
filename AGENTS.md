# AGENTS.md

FundCrawler — async scraper for ~21,000 Chinese mutual funds (Tiantian + Morningstar China). Python 3.14, `asyncio` + `aiohttp`.

## Hidden traps

| What | Why it bites |
|------|-------------|
| `aiohttp` NOT in `requirements.txt` | `pip install -r requirements.txt` won't install it. Manual: `pip install aiohttp requests tqdm fake-useragent` |
| Morningstar WAF | CloudFront checks `Accept` + `Accept-Language` headers. Missing → 403/silent timeout. `_BASE_HEADERS` in `page_fetcher.py:119` |
| MS returns UTF-8 BOM sometimes | `json.loads` rejects it. `_load_json()` in `morningstar.py:38` strips BOM with `.lstrip("\ufeff")` |
| `_resize` clamp (`page_fetcher.py:63`) | On shrink, `_available` capped to `_permits`. Without this released permits accumulate past new rate → reduction is defeated |
| Empty URL guard | When MS fund ID missing, Phase 2 `build_*_url` returns `""`, `fetch()` returns `None` with zero HTTP. Intended for edge cases only — with infinite retry, MS ID should eventually be found |
| MS infinite retry | `max_retries=None`. Unreachable → RC degrades to `min_rate=1` (single probe keeps trying). Reachable → AIMD climbs back to `increase_step=+3` |
| Sentinel strings (`constants.py`) | `NO_DATA` = source says no data. `DATA_ERROR` = crawl failed. `DATA_IGNORE` = intentionally skipped. Never use `NO_DATA` to mask crawl failures |
| `ResultWriter` falsy → `DATA_ERROR` | `getattr(ctx, attr) or DATA_ERROR` — empty string `""` or `0` in context becomes `DATA_ERROR` in CSV |

## Architecture notes (not obvious from code)

- Each fund enters Phase 1 immediately — no pipeline slots, no worker pool. All steps within a phase run in parallel via `asyncio.gather`.
- Phase 2 acquires RC with `priority=1`, Phase 1 with `priority=0`. P1 waiters yield to P2 waiters (`page_fetcher.py:45`).
- `rc.record()` is called on **every HTTP attempt** including retries → RC sees true QPS.
- Progress bar counts completed funds (not in-flight). Blinking dot (`"●"/" "` at 0.5s) proves liveness when rate-limited.

## Extension points

- Fund source → implement `TargetLoader` (duck-type: `async get_fund_list() → list[FundContext]`)
- Data source → add a `Step` to `STEPS` with `provides` declaring output fields
- Output format → replace `ResultWriter`

## Validation

**每次涉及 Step / `FundAttrKey` / `FundContext` / `page_parser` 的修改后，必须跑：**

```bash
pytest test_run.py -m slow
```

爬 10 只基金验证全流程可通，确认 CSV 列数和各列有数据率 OK。

## Fallback

`PreviousReleaseVersion` branch — pre-AI-rewrite version. Switch to it if Dev has unexpected regressions.
