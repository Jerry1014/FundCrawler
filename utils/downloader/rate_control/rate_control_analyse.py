from csv import DictReader

from matplotlib import pyplot as plt

from utils.downloader.rate_control.rate_control import RateControl


def draw_analyse(path='./'):
    with open(path + RateControl.record_file, 'r', newline='') as csvfile:
        fail_rate_recode = []
        tasks_num_recode = []
        threshold_num_record = []

        # 读取数据
        reader: DictReader = DictReader(csvfile)
        for row in reader:
            fail_rate_recode.append(round(float(row[RateControl.fail_rate_key]), 3))
            tasks_num_recode.append(round(float(row[RateControl.tasks_num_key]), 3))
            threshold_num_record.append(round(float(row[RateControl.threshold_key]), 3))

        # 绘图
        fig = plt.figure()
        plot1 = fig.add_subplot()

        x = range(len(fail_rate_recode))

        plot1.plot(x, fail_rate_recode, '-', label="fail_rate", color='r')
        plot1.legend()

        plot2 = plot1.twinx()
        plot2.plot(x, tasks_num_recode, '-', label="tasks_num", color='b')
        plot2.plot(x, threshold_num_record, '-', label="threshold_num", color='y')
        plot2.legend()
        
        plt.show()


if __name__ == '__main__':
    draw_analyse()
