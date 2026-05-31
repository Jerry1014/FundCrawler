# 基金数据爬虫

#### 重要提示

![GitHub license](https://img.shields.io/github/license/tindy2013/subconverter.svg)
- 202605 重大代码修改（纯AI重构），报错/奇怪bug，尝试切换PreviousReleaseVersion分支使用

        购买基金前，请务必在官方网站上确认爬取的数据无误！
        爬虫仅供学习交流使用，请不要对目标网站造成负担，并在心里默默感谢网站提供的免费数据
        推荐书籍《聪明的投资者》、《投资最重要的事》
        推荐网站 晨星中国：www.morningstar.cn

- 爬取的数据

        所有的开放式基金（包括目前暂停申购和认购中的基金，不包括货币/场内基金）
        基金代码,基金简称,(晨星)基金代码,基金类型,资产规模(亿),基金管理人,基金净值
        基金经理(最近连续最长任职),基金经理的上任时间
        管理费率(每年),托管费率(每年),销售服务费率(每年)
        五年回报(年化),十年回报(年化)
        标准差(五年%),标准差(十年%),夏普比率(五年),夏普比率(十年)
        阿尔法系数(相对于基准指数%),贝塔系数(相对于基准指数),R平方(相对于基准指数)

![Image text](docs/img/result_2.png)
- 爬取全部数据需要30min左右（21032个基金），取决于网络环境，瓶颈为网站的反爬策略


# 食用方法

- Python3.14
- 安装依赖 pip install -r requirements.txt
- 爬取基金数据
  - 结果保存在 result/result.csv
  - 运行test_run.py 爬一点点数据看下效果
  - 运行run.py 爬取完整数据
- 爬取结果分析，参考 result_analyse.py

# 技术相关
## 架构
```mermaid
flowchart TB
    subgraph 输入["① 基金列表"]
        TL[TargetLoader<br/>天天基金网 API]
    end

    subgraph 核心["② 爬虫引擎 Engine"]
        E[每只基金一个协程<br/>无槽位限制]
    end

    subgraph 网络["③ 速率控制 Fetcher"]
        direction LR
        RC_EM[EastMoney RC<br/>起步20 · 0.5s窗口]
        RC_MS[Morningstar RC<br/>起步8 · 1s窗口 · P2优先]
    end

    subgraph 解析["④ 数据解析 parsers/"]
        direction LR
        STEPS[5个Step · 2个Phase<br/>overview manager morningstar<br/>return risk]
    end

    subgraph 输出["⑤ 结果输出"]
        WR[ResultWriter] --> CSV[(result.csv)]
    end

    输入 -->|基金列表| 核心
    核心 -->|并发请求| 网络
    网络 -->|HTTP响应| 解析
    解析 -->|FundContext| 核心
    核心 -->|写入| 输出
```

每只基金 5 个数据源，按依赖自动分两阶段——morningstar ID 就绪后 Phase 2 才开始。每个 phase 内 `gather` 并行，瓶颈只取决于最慢的那个 step。

## 流程（动态并发控制）

从**一只基金的视角**看完整流程，以及它在哪些环节被 RateController 管控：

```mermaid
flowchart TB
    subgraph fund["一只基金的生命周期"]
        start([开始]) --> p1[Phase 1 · gather 并行]
        p1 --> ov[overview] & mg[manager] & ms[morningstar]
        ov --> em1{{EM RC<br/>获取许可}}
        mg --> em1
        ms --> ms1{{MS RC<br/>普通优先级}}
        em1 --> p1done[Phase 1 完成]
        ms1 --> p1done
        p1done --> p2[Phase 2 · gather 并行]
        p2 --> ret[return] & risk[risk]
        ret --> ms2{{MS RC<br/>高优先级 · 插队}}
        risk --> ms2
        ms2 --> write[写入 CSV]
    end

    subgraph rc["全局 RateController（所有基金共享）"]
        loop["每 0.5s/1s 统计成败"] --> adj{失败率 ≥ 20%?}
        adj -- 是 --> dec[并发 × 0.75]
        adj -- 否 --> inc[并发 + 1]
        dec --> limit[更新信号量额数]
        inc --> limit
    end

    ms2 -.->|P2 优先获取| rc
    ms1 -.->|P2 排队时让路| rc
    em1 -.->|独立管控| rc
```

**关键机制**：

- 每个 Step 发出 HTTP 请求前必须通过对应域名的 RC 信号量——有空额直接通过，没空额排队等。
- Phase 2 请求在 Morningstar RC 中**高优先级**：释放一个额数时，优先唤醒 Phase 2 的等待者。这让快完成的基金不被新基金挤占。
- RC 按域名独立运行，不做跨域名协调。EastMoney 的反爬策略与 Morningstar 完全不同，各自探测各自的上限。

| 域名 | 起步并发 | 探测间隔 | 备注 |
|------|---------|---------|------|
| EastMoney | 20 | 0.5s | 无反爬，AIMD 快速爬升 |
| Morningstar | 8 | 1.0s | 保守起步，P2 优先 |

同域名不同接口共享 RC，但超时按接口差异化：搜索 8s、详情 12s。
### 扩展点

换基金来源 → 实现 `TargetLoader`；加数据源 → 添加 `Step` 到 `STEPS`；换输出格式 → 替换 `ResultWriter`。
