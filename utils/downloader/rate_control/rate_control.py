"""
爬取速率控制
"""
from csv import DictWriter


class RateControl:
    """
    速率控制
    根据当前请求的失败率 决策当前的爬取速率
    """
    record_file = 'analyse.csv'
    fail_rate_key = 'fail_rate'
    tasks_num_key = 'tasks_num'

    def __init__(self):
        # 记录环，记录最近circle_count次的成功失败次数
        self._circle_count = 5
        self._success_count_ring = [0] * self._circle_count
        self._fail_count_ring = [0] * self._circle_count
        self._number_of_iterations = 0

        # 当前认为的最适合并发任务数 float
        self._cur_number_of_concurrent_tasks = 1.0
        # 上一次失败上升时的并发任务数
        self._last_cur_number_of_concurrent_tasks = self._cur_number_of_concurrent_tasks

        # 控制参数 成功后的任务上升率
        self._rising_step = 0.01

        # 分析模式下，会记录爬取过程中的 相关数据
        self._analyse_mode = False
        self._file = None
        self._writer = None

    def start_analyze(self):
        self._analyse_mode = True
        self._file = open(RateControl.record_file, 'w', newline='', encoding='utf-8')
        self._writer: DictWriter = DictWriter(self._file,
                                              fieldnames=[RateControl.fail_rate_key, RateControl.tasks_num_key])
        self._writer.writeheader()

    def get_cur_number_of_concurrent_tasks(self, success_count: int, fail_count: int) -> int:
        """
        根据当前的成功失败任务个数，决策当前最合适的并发任务数
        """
        self._success_count_ring[self._number_of_iterations % self._circle_count] = success_count
        self._fail_count_ring[self._number_of_iterations % self._circle_count] = fail_count

        total = sum(self._success_count_ring) + sum(self._fail_count_ring)
        fail_rate = (sum(self._fail_count_ring) / total) if total != 0 else 0.0

        # 计算
        if fail_rate > 0.0:
            self._last_cur_number_of_concurrent_tasks = self._cur_number_of_concurrent_tasks
            self._cur_number_of_concurrent_tasks = max(1, self._cur_number_of_concurrent_tasks / 2)
        else:
            self._cur_number_of_concurrent_tasks = max(self._last_cur_number_of_concurrent_tasks / 2,
                                                       self._cur_number_of_concurrent_tasks + self._rising_step)

        if self._analyse_mode:
            self._writer.writerow(
                {RateControl.fail_rate_key: fail_rate, RateControl.tasks_num_key: self._cur_number_of_concurrent_tasks})

        self._number_of_iterations += 1
        return int(self._cur_number_of_concurrent_tasks)

    def shutdown(self):
        if self._analyse_mode:
            self._file.close()
