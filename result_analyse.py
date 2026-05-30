"""爬取性能分析 —— 读取 profile.csv，定位瓶颈"""

import csv
import sys
from collections import defaultdict


def analyze(path: str = "./result/profile.csv") -> None:
    records = []
    with open(path, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            r["duration_ms"] = float(r["duration_ms"])
            r["sem_wait_ms"] = float(r["sem_wait_ms"])
            records.append(r)

    if not records:
        print("无数据")
        return

    total = len(records)
    success = sum(1 for r in records if r["success"] == "1")
    failure = total - success

    print(f"总请求: {total}  成功: {success}  失败: {failure}  "
          f"失败率: {failure/total:.1%}")
    print()

    # 按域名 + 端点分组
    groups: dict[str, list[float]] = defaultdict(list)
    for r in records:
        key = f"{r['domain']}/{r['endpoint']} phase={r['phase']}"
        groups[key].append(r["duration_ms"])

    print(f"{'端点':<40} {'数量':>5} {'耗时均值':>8} {'P50':>8} {'P90':>8} {'P99':>8}")
    print("-" * 80)
    for key in sorted(groups.keys()):
        vals = sorted(groups[key])
        n = len(vals)
        avg = sum(vals) / n
        p50 = vals[int(n * 0.5)]
        p90 = vals[int(n * 0.9)]
        p99 = vals[min(int(n * 0.99), n - 1)]
        print(f"{key:<40} {n:>5} {avg:>7.0f}ms {p50:>7.0f}ms {p90:>7.0f}ms {p99:>7.0f}ms")

    print()

    # 信号量等待拆解
    print(f"{'端点':<40} {'信号量等待均值':>12} {'P90':>8}")
    print("-" * 60)
    for key in sorted(groups.keys()):
        waits = [r["sem_wait_ms"] for r in records
                 if f"{r['domain']}/{r['endpoint']} phase={r['phase']}" == key]
        if waits:
            waits_sorted = sorted(waits)
            n = len(waits_sorted)
            avg_wait = sum(waits) / n
            p90_wait = waits_sorted[int(n * 0.9)]
            print(f"{key:<40} {avg_wait:>7.0f}ms {p90_wait:>8.0f}ms")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "./result/profile.csv"
    analyze(path)
