"""重爬结果 CSV 中含有 DATA_ERROR 的基金"""

import asyncio
import csv
import logging
import sys
from pathlib import Path

from module.constants import log_format, DATA_ERROR, FundAttrKey as K
from module.engine import run
from module.result_writer import ResultWriter
from module.target_loader import StaticTargetLoader

_HEADER_TO_KEY = {k.value: k for k in K}


def _detect_fields(headers: list[str]) -> frozenset[K]:
    return frozenset(_HEADER_TO_KEY[h] for h in headers if h in _HEADER_TO_KEY)


def _split(path: Path) -> tuple[list[dict], list[dict], list[str]]:
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = list(reader.fieldnames or [])
        rows = list(reader)

    good, errors = [], []
    for r in rows:
        (errors if DATA_ERROR in r.values() else good).append(r)
    return good, errors, headers


def main(csv_path: str) -> None:
    path = Path(csv_path).resolve()
    if not path.exists():
        print(f"文件不存在: {path}")
        sys.exit(1)

    good, errors, headers = _split(path)
    print(f"总基金: {len(good) + len(errors)}  正常: {len(good)}  错误: {len(errors)}")

    if not errors:
        print("无错误数据，退出")
        return

    valid_errors: list[tuple[str, str]] = []
    for r in errors:
        code = r[K.FUND_CODE.value]
        if not code or code == DATA_ERROR:
            continue
        name = r[K.FUND_SIMPLE_NAME.value]
        valid_errors.append((code, name if name and name != DATA_ERROR else code))

    if not valid_errors:
        print("所有错误基金的代码均无效，无法重爬")
        return

    print(f"可重爬: {len(valid_errors)} 只")

    print("清理 CSV ...")
    with open(path, 'w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(good)

    fields = _detect_fields(headers)
    loader = StaticTargetLoader(valid_errors)

    tmp = path.with_stem(path.stem + "_tmp")
    writer = ResultWriter(path=str(tmp.parent), filename=tmp.name, fields=fields)

    logging.basicConfig(level=logging.INFO, format=log_format)
    print("开始重爬 ...")
    asyncio.run(run(loader, fields=fields, writer=writer))

    with open(tmp, encoding="utf-8") as f:
        tmp_rows = list(csv.reader(f))

    if len(tmp_rows) > 1:
        with open(path, 'a', encoding='utf-8', newline='') as f:
            csv.writer(f).writerows(tmp_rows[1:])
        print(f"已追加 {len(tmp_rows) - 1} 行")
    else:
        print("重爬无结果 (全部再次失败)")

    tmp.unlink(missing_ok=True)
    print("完成")


if __name__ == "__main__":
    main('./result/result.csv')
