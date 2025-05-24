"""
爬取速率控制
"""
import time
from csv import DictWriter
from typing import Optional, TextIO


class RateControl:
    """
    爬取速率控制
    """
    RECORD_FILE = 'analyse.csv'
    FAIL_RATE = 'fail_rate'
    WORK_COUNT = 'work_count'
    RATE_CONTROL = 'rate_control'

    refresh_interval_s = 0.5
    analyse_mode = False

    def __init__(self, domain, max_rate: float):
        self._domain = domain

        # 请求的成功失败计数
        self._total_success_count: int = 0
        self._total_fail_count: int = 0
        self._cur_work_count: int = 0

        # 每一个时间间隔后刷新的 当前成功失败计数
        self._success_count: int = 0
        self._fail_count: int = 0
        self._last_success_count: int = 0
        self._last_fail_count: int = 0
        self._last_time_stamp: float = 0
        self._number_of_iterations: int = 0

        # 当前间隔的并发速度 最大限制速度
        self._cur_rate: float = max_rate / 2
        self._max_rate: float = max_rate

        # 分析模式下，会记录爬取过程中的 相关数据
        self._file: Optional[TextIO] = None
        self._writer: Optional[DictWriter] = None

        if self.analyse_mode:
            self._file = open(self._domain + '-' + RateControl.RECORD_FILE, 'w', newline='', encoding='utf-8')
            field_names = [RateControl.FAIL_RATE, RateControl.WORK_COUNT, RateControl.RATE_CONTROL]
            self._writer = DictWriter(self._file, fieldnames=field_names)
            self._writer.writeheader()

    def get_cur_rate(self) -> int:
        return int(self._cur_rate)

    def record(self, success_count: int, cur_count: int, cur_work_count: int) -> None:
        """
        记录当前的成功/失败率
        """
        self._total_success_count += success_count
        self._total_fail_count += (cur_count - success_count)
        self._cur_work_count: int = cur_work_count

        cur_time = time.time()
        if cur_time - self._last_time_stamp > self.refresh_interval_s:
            self._last_time_stamp = cur_time
            self.cal_rate()

    def cal_rate(self) -> None:
        """
        根据当前的成功失败任务个数，决策当前最合适的并发任务数
        """
        # 更新当前的成功失败计数（注意线程安全问题）
        last_success_count = self._last_success_count
        last_fail_count = self._last_fail_count
        cur_success_count = self._total_success_count
        cur_fail_count = self._total_fail_count
        self._success_count = cur_success_count - last_success_count
        self._fail_count = cur_fail_count - last_fail_count
        self._last_success_count = cur_success_count
        self._last_fail_count = cur_fail_count
        self._number_of_iterations += 1

        # 计算当前的并发度
        # change_rate [11, 1]
        change_rate = (1100 - self._number_of_iterations) / 100 if self._number_of_iterations < 1000 else 1
        total_count = self._success_count + self._fail_count
        fail_rate = (self._fail_count / total_count) if total_count else 1.0
        if fail_rate > 0.0:
            self._cur_rate = max(1.0, self._cur_rate - fail_rate * change_rate)
        else:
            # 随着迭代进行 增加的速度逐渐降低
            self._cur_rate = min(self._max_rate, self._cur_rate + change_rate)

        # 分析数据
        if self.analyse_mode:
            self._writer.writerow(
                {RateControl.FAIL_RATE: fail_rate, RateControl.WORK_COUNT: self._cur_work_count,
                 RateControl.RATE_CONTROL: self._cur_rate})

    def exit(self):
        if self.analyse_mode:
            self._file.flush()
            self._file.close()
