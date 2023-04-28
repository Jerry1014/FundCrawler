import logging

from matplotlib import pyplot as plt


class RateControl:
    """
    速率控制
    根据当前请求的失败率 决策当前的爬取速率
    """

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

        # todo 记录 用于分析
        self._fail_rate_recode = []
        self._tasks_num_recode = []

    def get_cur_number_of_concurrent_tasks(self, success_count: int, fail_count: int) -> int:
        """
        根据当前的成功失败任务个数，决策当前最合适的并发任务数
        """
        self._success_count_ring[self._number_of_iterations % self._circle_count] = success_count
        self._fail_count_ring[self._number_of_iterations % self._circle_count] = fail_count

        total = sum(self._success_count_ring) + sum(self._fail_count_ring)
        fail_rate = (sum(self._fail_count_ring) / total) if total != 0 else 0.0

        #
        if fail_rate > 0.0:
            self._last_cur_number_of_concurrent_tasks = self._cur_number_of_concurrent_tasks
            self._cur_number_of_concurrent_tasks = max(1, self._cur_number_of_concurrent_tasks / 2)
        else:
            self._cur_number_of_concurrent_tasks = max(self._last_cur_number_of_concurrent_tasks / 2,
                                                       self._cur_number_of_concurrent_tasks + self._rising_step)
        logging.info(f"当前爬取失败率{fail_rate} 最大任务数{self._cur_number_of_concurrent_tasks}")

        self._fail_rate_recode.append(fail_rate)
        self._tasks_num_recode.append(self._cur_number_of_concurrent_tasks)

        self._number_of_iterations += 1
        return int(self._cur_number_of_concurrent_tasks)

    def draw_analyse(self):
        fig = plt.figure()
        plot1 = fig.add_subplot()

        x = range(len(self._fail_rate_recode))

        plot1.plot(x, self._fail_rate_recode, '-', label="fail_rate", color='r')
        plot1.legend()

        plot2 = plot1.twinx()
        plot2.plot(x, self._tasks_num_recode, '-', label="tasks_num", color='b')
        plot2.legend()
        plt.show()
