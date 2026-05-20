# FundCrawler V2

## 核心

自适应流量控制：在线探测失败率 → 动态调并发 → 在反爬阈值内跑出最大 QPS。

## 三个拓展点

| 拓展点 | 模块 | 新增成本 |
|--------|------|---------|
| 爬哪些基金 | `target_loader.py` | 加一种 Loader |
| 爬什么网站 | `parsers/` | 加一个文件 + STEPS 一行 |
| 怎么保存 | `writer.py` | 换一个 Writer |

### 为什么增加页面不会拖慢速度

无依赖的页面并发执行。依赖深度是唯一瓶颈，页面数量不是。

```
Phase 1: overview, manager, morningstar          ← 3 并发
Phase 2: return, risk (等 morningstar 完成)      ← 2 并发
```

依赖声明在 `parsers/__init__.py`，Engine 根据声明自动分组。

## 模块

```
run.py → engine.run()
           ├─ target_loader  → [FundContext, ...]
           ├─ fetcher        → HTTP + RateController + retry
           ├─ parsers/STEPS  → 依赖声明 + URL + 解析
           └─ writer         → CSV
```

## 目录

```
crawler/
├── engine.py            # 组装，~60 行
├── fund_context.py      # 数据对象，~60 行
├── fetcher.py           # HTTP + 限流 + 重试，~150 行
├── target_loader.py     # 4 种 Loader，~90 行
├── writer.py            # CSV，~50 行
└── parsers/
    ├── __init__.py      # STEPS
    ├── eastmoney.py     # overview + manager
    └── morningstar.py   # morningstar + return + risk
tests/                   # 54 单测
utils/                   # constants, fake_ua, top_k
```

## 关键设计决策

**asyncio 而非多进程**：旧架构的手写 Queue 状态机正是 `await` 原生提供的能力。120 行 `do_run()` 收敛到 15 行 `while`。GIL 无影响——单次解析 0.77ms，CPU 占比 <5%。

**按数据源拆分而非按页面**：同一网站的逻辑天然内聚。加新浪财经 = `parsers/sina.py` + STEPS 一行，其余零改动。

**RateController 嵌入 Fetcher**：速率控制是 HTTP 客户端的内部策略，不是通用组件。不被复用的不拆出去。

**无抽象基类**：个人项目，鸭子类型够用。`target_loader` 只需 `async get_fund_list()`。
