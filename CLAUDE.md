# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FundCrawler is an async web scraper that crawls Chinese mutual fund data (~21,000 funds) from **EastMoney** (天天基金网) and **Morningstar China** (晨星中国). Output is a single CSV at `result/result.csv`, consumed by `result_analyse.py` for fund screening and ranking.

Python 3.14, `asyncio` + `aiohttp`. V2 was a full AI-assisted rewrite (May 2026).

## Common Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run unit tests (no network)
pytest

# Run unit tests + smoke test (crawls 100 funds live)
pytest -m slow

# Full crawl (~30 min for all ~21,000 funds)
python run.py

# Analyze crawled results
python result_analyse.py
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

**`crawler/engine.py`** — Orchestrator. Gets fund list from a `TargetLoader`, then crawls each fund through the **STEPS pipeline**. A semaphore limits to 8 concurrent fund pipelines. Within each fund, steps are batched into **phases**: all steps whose dependencies are satisfied run in parallel via `asyncio.gather`. Phases repeat until all 5 steps complete.

**`crawler/parsers/__init__.py`** — The **STEPS dependency graph** (5 steps):

| Step | Depends On | Source |
|------|-----------|--------|
| `overview` | — | EastMoney HTML (fund type, size, fees, NAV) |
| `manager` | — | EastMoney HTML (manager name, appointment date) |
| `morningstar` | — | Morningstar JSON API (internal fund ID) |
| `return` | `morningstar` | Morningstar JSON API (5yr/10yr annualized returns) |
| `risk` | `morningstar` | Morningstar JSON API (std dev, Sharpe, Alpha, Beta, R²) |

Each `Step` is a frozen dataclass with `name`, `build_url`, `parse`, and `deps`. This is the primary extension point — add a new data source by creating a new `Step` and appending to `STEPS`.

**`crawler/fetcher.py`** — Async HTTP client with **adaptive rate control** (AIMD: Additive Increase, Multiplicative Decrease). Three per-domain `RateController` instances:
- `_eastmoney` (initial=5), `_ms_search` (initial=3), `_ms_quicktake` (initial=5)
- Failure rate ≥10% → halve concurrency; all-success interval → +1. Capped by min/max.
- 3 retries with exponential backoff (×1.5). Random UA per request.

**`crawler/target_loader.py`** — Duck-typed loaders (all expose `async get_fund_list() → list[FundContext]`):
- `WebTargetLoader` — fetches full fund list from EastMoney API
- `SmallBatchLoader(limit=N)` — fetches N funds for testing
- `StaticTargetLoader(funds)` — fixed list for tests
- `RetryTargetLoader(inner)` — wraps another loader, filters out funds already in CSV (resume support)

**`crawler/fund_context.py`** — Single `@dataclass` carrying all 19 fund attributes through the pipeline. Parsers mutate it in-place.

**`crawler/writer.py`** — Async CSV writer. One `asyncio.Lock` protects file I/O. Missing fields are written as `DATA_ERROR`. Column mapping lives in `_COLUMNS` (the single source of truth linking `FundAttrKey` to attribute names).

**`utils/constants.py`** — `FundAttrKey` enum (CSV column headers), `PageType` enum (legacy), sentinel strings (`NO_DATA`, `DATA_ERROR`, `DATA_IGNORE`), and the shared regex `number_in_eng` for parsing Chinese-formatted numbers (e.g. `1,234.56`).

**`result_analyse.py`** — Post-processing: reads CSV, filters by fund type/size/manager tenure, then ranks by Sharpe ratio (top 10% with R² > 60) and picks top 3 by Alpha-minus-fees and top 3 by return-minus-fees. Runs three canned analyses (纯债, 指数/混合, 全部).

## Key Design Decisions

- **Dependency-driven parallelism**: The STEPS graph means a fund's `return` and `risk` pages fetch in parallel once `morningstar` resolves. Pipeline semaphore (8 slots) bounds total concurrency. Wall-clock time is dominated by the slowest step chain per fund, not by the number of funds.
- **Sentinel values**: `NO_DATA` = source says "no data", `DATA_ERROR` = crawl/parse failed, `DATA_IGNORE` = intentionally skipped. These are string markers in CSV, not None/empty, to distinguish missing data from fetch failures.
- **AIMD rate control** beats fixed-rate throttling because EastMoney/Morningstar anti-crawl behavior varies by time of day and request pattern.
- **PreviousReleaseVersion branch** exists as a fallback — the current `Dev` branch is an AI refactor. If weird bugs appear, compare against that branch.

## Testing

- **Framework**: pytest + pytest-asyncio (all async tests use `@pytest.mark.asyncio`)
- **Fixtures**: `tests/case/` contains real saved HTML/JSON responses from both sources
- **Markers**: `@pytest.mark.slow` for network-dependent tests (smoke test in `test_run.py`)
- **No mocking framework** — tests use real saved fixtures or in-memory temp files
