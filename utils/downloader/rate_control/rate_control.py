"""
爬取速率控制
"""
from csv import DictWriter
from os import cpu_count


class RateControl:
    """
    速率控制
    根据当前请求的失败率 决策当前的爬取速率
    """
    record_file = 'analyse.csv'
    fail_rate_key = 'fail_rate'
    tasks_num_key = 'tasks_num'
    threshold_key = 'threshold_num'

    # 初始的并发任务数，爬取多次后可以得到当前网络下的经验值
    init_num = 12

    def __init__(self):
        # 记录环，记录最近circle_count次的成功失败次数
        self._circle_count = 10
        self._success_count_ring = [0] * self._circle_count
        self._fail_count_ring = [0] * self._circle_count
        self._number_of_iterations = 0

        # 当前认为的最适合并发任务数 float
        self._cur_number = 1.0
        self._max_num = cpu_count() * 5.0
        self._last_number = RateControl.init_num

        # 控制参数 成功后的任务上升率
        self._min_rising_step = 0.01
        self._fail_has_recover = True

        # 分析模式下，会记录爬取过程中的 相关数据
        self._analyse_mode = False
        self._file = None
        self._writer = None

    def start_analyze(self):
        self._analyse_mode = True
        self._file = open(RateControl.record_file, 'w', newline='', encoding='utf-8')
        field_names = [RateControl.fail_rate_key, RateControl.tasks_num_key, RateControl.threshold_key]
        self._writer: DictWriter = DictWriter(self._file, fieldnames=field_names)
        self._writer.writeheader()

    def get_cur_number_of_concurrent_tasks(self, success_count: int, fail_count: int) -> int:
        """
        根据当前的成功失败任务个数，决策当前最合适的并发任务数
        """
        self._success_count_ring[self._number_of_iterations % self._circle_count] = success_count
        self._fail_count_ring[self._number_of_iterations % self._circle_count] = fail_count

        total = sum(self._success_count_ring) + sum(self._fail_count_ring)
        fail_rate = (sum(self._fail_count_ring) / total) if total != 0 else 0.0

        # 根据当前失败率 动态调整爬取任务数
        if fail_rate > 0.0:
            if self._fail_has_recover:
                # 一个失败潮 只调整阈值一次
                self._last_number = self._cur_number
            self._cur_number = 0
            self._fail_has_recover = False
        else:
            self._fail_has_recover = True
            # 根据与上一次失败时的距离，计算当前步长
            # 越接近于上一次失败的数值时，步长越小 self._min_rising_step <= x <= 1
            rate = ((self._last_number - self._cur_number) / self._last_number) ** 2
            step = max(self._min_rising_step, rate * self._min_rising_step * 10) \
                if self._cur_number < self._last_number else self._min_rising_step
            # 当失败率恢复时，尝试快速恢复到 之前失败时的1/2
            number = max(self._last_number / 2.0, self._cur_number + step)
            self._cur_number = min(self._max_num, number)

        if self._analyse_mode:
            self._writer.writerow({RateControl.fail_rate_key: fail_rate, RateControl.tasks_num_key: self._cur_number,
                                   RateControl.threshold_key: self._last_number})

        self._number_of_iterations += 1
        return int(self._cur_number)

    def shutdown(self):
        if self._analyse_mode:
            self._file.close()
