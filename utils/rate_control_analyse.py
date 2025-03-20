from csv import DictReader

from matplotlib import pyplot as plt

from module.downloader.rate_control.rate_control import RateControl


def draw_analyse(path='./'):
    with open(path + RateControl.RECORD_FILE, 'r', newline='') as csvfile:
        fail_rate_record = []
        work_count_record = []
        rate_control_record = []

        # 读取数据
        reader: DictReader = DictReader(csvfile)
        for row in reader:
            fail_rate_record.append(round(float(row[RateControl.FAIL_RATE]), 3))
            work_count_record.append(round(float(row[RateControl.WORK_COUNT]), 3))
            rate_control_record.append(round(float(row[RateControl.RATE_CONTROL]), 3))

        # 绘图
        fig = plt.figure()
        plot1 = fig.add_subplot()
        x = range(len(fail_rate_record))

        plot1.plot(x, fail_rate_record, '-', label=RateControl.FAIL_RATE, color='r')
        
        plot2 = plot1.twinx()
        plot2.plot(x, work_count_record, '-', label=RateControl.WORK_COUNT, color='b')
        plot2.plot(x, rate_control_record, '-', label=RateControl.RATE_CONTROL, color='y')

        fig.legend()
        plt.show()


if __name__ == '__main__':
    draw_analyse('../')
