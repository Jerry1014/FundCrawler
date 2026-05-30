# CLAUDE.md

FundCrawler — async web scraper for Chinese mutual fund data (~21,000 funds) from EastMoney (天天基金网) and Morningstar China (晨星中国). Outputs `result/result.csv`, consumed by `result_analyse.py`.

Python 3.14, `asyncio` + `aiohttp`.

## Common Commands

```bash
pytest                          # unit tests (no network)
pytest -m slow                  # smoke test (500 funds live, via test_run.py)
python run.py                   # full crawl
python result_analyse.py        # analyze results
```

## Architecture

```
run.py ──► engine.run(target_loader) ──► CSV
                │
        ┌───────┼────────┐
        ▼       ▼        ▼
   TargetLoader  Fetcher  STEPS (parsers/)
   (fund list)  (HTTP)    (parse HTML/JSON → FundContext)
                          Writer (async CSV)
```

**Engine** — Semaphore(20) bounds concurrent fund pipelines. Each fund progresses through phases: all steps whose deps are satisfied run via `asyncio.gather`. Phase 1: overview + manager + morningstar. Phase 2: return + risk (both depend on morningstar ID).

**STEPS** (`parsers/__init__.py`) — Declarative dependency graph. Each `Step` is a frozen dataclass: `name`, `build_url`, `parse`, `deps`. Primary extension point.

**Fetcher** — Per-domain `RateController` instances with AIMD: fail_rate ≥20% → ×0.75, else +1. Key non-obvious details:

- Morningstar sits behind **CloudFront WAF** that checks `Accept`/`Accept-Language` headers. Missing headers → 403/timeout. `_BASE_HEADERS` mimics a real browser.
- `rc.record()` is called **once per `fetch()`, on the final outcome only**. Calling it per retry-attempt would count transient failures as real failures, falsely triggering rate reduction even when the request ultimately succeeded.
- Morningstar starts conservative (3–5 concurrent) with a longer adjustment window (1.0s). EastMoney starts aggressive (20) with a shorter window (0.5s). AIMD naturally finds each domain's safe ceiling.

**TargetLoader** — Duck-typed: anything with `async get_fund_list() → list[FundContext]`. `RetryTargetLoader` wraps another loader and skips funds already in CSV (resume support).

**Writer** — Async CSV, one `asyncio.Lock`. Column mapping in `_COLUMNS` is the single source of truth.

## Key Design Decisions

- **Dependency-driven parallelism**: Phase depth is the only bottleneck — 5 steps complete in 2 phases regardless of fund count.
- **Sentinel strings** (`NO_DATA`, `DATA_ERROR`, `DATA_IGNORE`) in CSV vs None/empty — distinguishes "source says no data" from "crawl failed".
- **Per-domain AIMD** beats uniform throttling because EastMoney tolerates high concurrency while Morningstar anti-crawl varies by time/pattern.
- **PreviousReleaseVersion** branch is the pre-AI-rewrite fallback.

## Testing

- pytest + pytest-asyncio. `tests/case/` has real saved HTTP fixtures. No mocking framework.
- `@pytest.mark.slow` for network tests (`test_run.py`).
