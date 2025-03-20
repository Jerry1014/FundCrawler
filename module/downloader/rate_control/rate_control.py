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
    analyse_mode = False

    # 初始的并发任务数，爬取多次后可以得到当前网络下的经验值
    init_num = 12

    def __init__(self, max: int):
        # 记录环，记录最近circle_count次的成功失败次数
        self._circle_count = 100
        self._success_count_ring = [0] * self._circle_count
        self._fail_count_ring = [0] * self._circle_count
        self._number_of_iterations = 1

        # 当前认为的最适合并发任务数 float
        self._cur_number = 1.0
        self._max_num = cpu_count() * 5.0

        # 分析模式下，会记录爬取过程中的 相关数据
        self._analyse_mode_start = False
        self._file = None
        self._writer = None

    def start_analyze(self):
        self._file = open(RateControl.record_file, 'w', newline='', encoding='utf-8')
        field_names = [RateControl.fail_rate_key, RateControl.tasks_num_key, RateControl.threshold_key]
        self._writer: DictWriter = DictWriter(self._file, fieldnames=field_names)
        self._writer.writeheader()

    def get_cur_number_of_concurrent_tasks(self, success_count: int, fail_count: int, concurrent_count: int) -> int:
        """
        根据当前的成功失败任务个数，决策当前最合适的并发任务数
        """
        if self.analyse_mode is True and self._analyse_mode_start is False:
            self.start_analyze()
            self._analyse_mode_start = True

        self._success_count_ring[self._number_of_iterations % self._circle_count] = success_count
        self._fail_count_ring[self._number_of_iterations % self._circle_count] = fail_count

        total = sum(self._success_count_ring) + sum(self._fail_count_ring)
        fail_rate = (sum(self._fail_count_ring) / total) if total != 0 else 0.0

        iterations_time = self._number_of_iterations >> 5
        rate = max(1.0 / iterations_time if iterations_time else 1, 0.001)
        if max(0.0, fail_rate - 0.1) > 0.0:
            # 减少的速率 随失败率的降低和迭代次数的增加 而降低
            need_cut_number = self._cur_number * (1 - fail_rate)
            self._cur_number = max(0.0, self._cur_number - need_cut_number * rate)
        else:
            # 随着迭代进行 增加的速度逐渐降低
            self._cur_number = min(self._max_num, self._cur_number + rate)

        if self.analyse_mode:
            self._writer.writerow({RateControl.fail_rate_key: fail_rate, RateControl.tasks_num_key: concurrent_count,
                                   RateControl.threshold_key: self._cur_number})

        self._number_of_iterations += 1
        return int(self._cur_number)

    def shutdown(self):
        if self.analyse_mode:
            self._file.close()
