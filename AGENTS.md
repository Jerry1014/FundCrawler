# AGENTS.md

FundCrawler — async scraper for ~21,000 Chinese mutual funds from EastMoney + Morningstar China.
Python 3.14, `asyncio` + `aiohttp`.

## Entry points

- `run.py` — full crawl (all ~21K funds) → `result/result.csv`
- `test_run.py` — smoke test (100 funds), run via `pytest test_run.py -m slow`
- `result_analyse.py` — post-hoc CSV analysis

## Dependencies

`aiohttp` is **NOT** in `requirements.txt`. Runtime install:

    pip install aiohttp requests tqdm fake-useragent

## Testing

```bash
pytest tests/                       # unit tests only
pytest test_run.py -m "slow"        # integration: 100 actual funds
pytest -m "not slow"                # skip integration
```

No CI, no linter, no typechecker, no `pyproject.toml`.

## Architecture

5 steps in 2 phases. `module/page_parser/__init__.py:27` defines `STEPS`:

| Phase | Step        | Domain      |
|-------|-------------|-------------|
| 1     | overview    | EastMoney   |
| 1     | manager     | EastMoney   |
| 1     | morningstar | Morningstar |
| 2     | return      | Morningstar |
| 2     | risk        | Morningstar |

Phase 2 depends on `morningstar` (needs MS fund ID). All funds enter Phase 1 at once — no pipeline slots. Within each phase, steps run in parallel via `asyncio.gather`.

## Rate control (domain-dimension)

One `RateController` per host, not per endpoint. Config in `module/page_fetcher.py:127`:
- EastMoney: start=20 concurrent, window=0.5s, step=+1
- Morningstar: start=8 concurrent, window=1.0s, step=**+3**

AIMD with dual threshold (each window):
- fail_rate >50% → ×0.5
- fail_rate ≥20% → ×0.75
- fail_rate <20% → +step

Phase 2 acquires with `priority=1`, Phase 1 with `priority=0`. P1 waiters yield to P2 waiters (`page_fetcher.py:45`).

`rc.record()` is called on **every HTTP attempt** including retries. Rate controller sees true QPS.

MS infinite retry via `max_retries=None` (EM: 2 retries). When unreachable, RC degrades to `min_rate=1` — single probe keeps trying; when reachable, AIMD climbs back.

## Gotchas

### `_resize` clamp (`page_fetcher.py:63`)

On shrink, `_available` is clamped to `_permits`. Without this, released permits accumulate beyond the new rate, defeating the reduction.

### Empty URL guard

When MS fund ID is missing, Phase 2 `build_return_url` / `build_risk_url` return `""`, and `fetch()` returns `None` without HTTP request. With infinite retries, MS ID should eventually be found — the guard handles edge cases only.

### Morningstar WAF

CloudFront WAF checks `Accept` and `Accept-Language` headers. `_BASE_HEADERS` in `page_fetcher.py:119` mimics a browser. Missing headers → 403/timeout.

### Sentinel strings (`constants.py:39`)

- `NO_DATA` — source says no data
- `DATA_ERROR` — crawl failed (empty/missing)
- `DATA_IGNORE` — intentionally skipped

Do NOT use `NO_DATA` to mask crawl failures.

### ResultWriter field names

`result_writer.py:52` uses `getattr(ctx, attr) or DATA_ERROR` — an empty string or falsy value in the context becomes `DATA_ERROR` in CSV.

### `PreviousReleaseVersion` branch

Pre-AI-rewrite fallback. Switch to it if current branch has unexpected regressions.

### Progress bar

`tqdm` with a blinking dot spinner (`"●"/" "`) rotating at 0.5s. The bar counts completed funds, not in-flight ones — spinner proves liveness when rate-limited.

## Extension points

- Fund source → implement `TargetLoader` (duck-typed: `async get_fund_list() → list[FundContext]`)
- Data source → add a `Step` to `STEPS` list
- Output format → replace `ResultWriter`
