# 天天基金爬虫
爬取天天基金网上的所有基金，辅助对基金投资的选择
        
## 功能特性
- 爬取基金的近1、3、6月，近1、3年及成立来的收益率，当前基金经理及其任职时间、任职来的收益率及总的任职时间
- 模仿tcp的拥塞避免的线程数量控制，慢开始，当出现错误时，线程最大值减半，成功则线程最大值+1
- 爬取全部数据需要136s，瓶颈为网站的反爬策略

- 结果展示
    ![Image text](./image/result-2.png)
    2019/11/26 共有7571个基金

## 食用方法
- 下载所有脚本文件（除MonkeyTest外）
- 爬取基金数据
    - 运行CrawlingFund.py并等待
- ~~筛选基金~~ 还没做
- ~~一些分析~~ 也没做

## 自定义
- 虚假UA获取
        
        自定义类，并提供一个实例，可以通过 ins.random来获得一个UA（str）
        替换所有的fake_ua

- 指定爬取的基金列表或获取基金列表的方式

        在ProvideTheListOfFund.py继承GetFundList类并实现_set_fund_list_generator方法
        或者通过其他方法修改属性_fund_list_generator的值
        在CrawlingFund.py中将自定义的类传入crawling_fund方法
        已实现的类：
        GetFundListByWeb 从天天基金网站下载全部基金列表
        GetFundListFromList 传入要爬取的基金列表
        GetFundListFromFile 从文件中获得基金列表  （这个还没做）
        
- 指定爬取方法
        
        继承CrawlingWebpage.py中的GetPage类
        在CrawlingFund.py中的crawling_fund方法里，将爬取核心类替换为自定义的类
        已实现的类：
        GetPageByWebWithAnotherProcessAndMultiThreading 子进程，通过多线程的方式获取页面，模拟tcp的拥塞控制来管理线程数量
        
        

## 以后的更新
- 数据清洗，按照要求筛选基金
- 数据可视化 （计划做个简单的晴雨表？显示这个月多少基金赚钱，多少赔钱）
- 代理？（好像这个需求不大，可能不做）
