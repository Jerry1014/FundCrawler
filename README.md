# 天天基金爬虫

## 简介

#### 重要提示

![GitHub license](https://img.shields.io/github/license/tindy2013/subconverter.svg)

        购买基金前，请务必在官方网站上确认爬取的数据无误！
        爬虫仅供学习交流使用，请不要对目标网站造成负担，并在心里默默感谢网站提供的免费数据
        推荐书籍《解读基金：我的投资观与实践》、《聪明的投资者》、《投资最重要的事》
        推荐网站 晨星中国：www.morningstar.cn
        我利用基金数据挑选基金的策略包括 1 在本基金任职超过三年，且三年夏普表现优异的经理 2 在本基金任职超过十年，年均回报率表现优异的经理

- 基金类型,资产规模,基金管理人,基金净值,基金经理(最近连续最长任职),基金经理的上任时间,近三年标准差,近三年夏普,近三年涨幅,近五年涨幅
- 爬取全部数据需要26min左右（20619个基金），取决于网络环境，瓶颈为网站的反爬策略
  ![Image text](docs/img/result_2.png)

# 食用方法

- Python3.13
- 安装依赖 pip install -r requirements.txt
- 运行run.py 爬取基金数据
- 杂七杂八
  - 只想爬一点点数据看下效果 test_process_manager.SmokeTestTaskManager.test_run
  - 爬了很多我不需要的数据，很慢 module.crawling_data.async_crawling_data.AsyncCrawlingData.__init__
  - 爬取过程中的日志文件 process_manager.TaskManager.\_\_init__
  - 爬取结果文件 module.save_result.save_result_2_file.SaveResult2File.\_\_init__
  - 爬取结果分析 (通过堆，取三年夏普最高的前几个基金)utils.result_analyse.analyse
  - 想爬取更多的数据  
1 看下现有的爬取网页上是否有对应的信息  
module.crawling_data.data_mining.data_mining_type.PageType
有的话，直接在对应的策略上，通过正则或其他的方式将信息提取出来  
没有的话，新增一个策略，爬取新的网页，以及进行对应的清洗

# 技术相关
![Image text](docs/img/overview.png)
- 因为数据清洗和 http下载分别是计算密集和IO密集的，为了避免GIL和频繁的线程切换影响效率。
AsyncHttpRequestDownloader起了一个新进程，在子进程内通过线程池进行http的爬取，通过队列来交换爬取任务和结果，通过事件来感知爬取结束
- 目前的爬取瓶颈是网站的反爬策略，可以通过utils.downloader.rate_control.rate_control_analyse.draw_analyse来分析当前网络环境下
所能支持的最高并发任务数，爬虫本身也会尝试寻找一个合适的并发数（并发的变化率随迭代数的增加而降低）
![Image text](docs/img/rate_control.png)

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=Jerry1014/FundCrawler&type=Date)](https://star-history.com/#Jerry1014/FundCrawler&Date)

## 未来更新计划

- 继续优化爬取速率，精进爬取窗口的动态变化算法
- 健壮性。增加数据匹配时的错误发现机制
