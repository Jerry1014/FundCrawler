"""
解析基金网页的内容
"""
import re


class ParseBase:
    def get_parse_fund_info(self):
        """
        返回用于解析基金信息的迭代器或其他（需实现通过send方法接收html文本）
        """
        raise NotImplementedError()

    def get_parse_fund_manger(self):
        """
        返回用于解析基金经理信息的迭代器或其他（需实现通过send方法接收html文本）
        """
        raise NotImplementedError()

    def get_after_parsing_fund_info(self, first_crawling):
        """
        返回用于完成基金信息的解析后的处理，如保存文件
        :param first_crawling:是否是第一次爬取（还是对爬取失败的部分做的重复爬取），用于保存文件时，防止第一次爬取的文件被删除
        """
        raise NotImplementedError()


class ParseDefault(ParseBase):
    """
    解析的默认实现，针对天天基金网（http://fund.eastmoney.com/）2019/11
    """
    # 基金类型的分类
    result_dir = './results/'
    fund_kind_belong_to_index = ['股票型', '混合型', '债券型', '定开债券', '股票指数', '联接基金', 'QDII-指数', 'QDII',
                                 '混合-FOF', '货币型', '理财型', '分级杠杆', 'ETF-场内', '债券指数']
    fund_kind_belong_to_guaranteed = ['保本型']
    fund_kind_belong_to_closed_period = ['固定收益']
    # 不同类型基金的解析顺序定义
    parse_index_for_index_fund = ['近1月', '近1年', '近3月', '近3年', '近6月', '成立来']
    parse_index_for_guaranteed_fund = ['保本期收益', '近6月', '近1月', '近1年', '近3月', '近3年']
    parse_index_for_capital_preservation_fund = ['最近约定年化收益率']

    def get_parse_fund_info(self):
        fund_web_page_parse = self._parse_fund_info()
        next(fund_web_page_parse)
        return fund_web_page_parse

    def get_parse_fund_manger(self):
        manager_web_page_parse = self._parse_manager_info()
        next(manager_web_page_parse)
        return manager_web_page_parse

    def get_after_parsing_fund_info(self, first_crawling):
        write_file = self._write_to_file(first_crawling)
        next(write_file)
        return write_file

    @classmethod
    def _parse_fund_info(cls):
        """
        对基金信息界面进行解析 通过send(page_context,fund_info)来获得解析
        未来将整合进fund_info类中
        :return: 迭代器 FundInfo
        """
        page_context, fund_info = yield

        while True:
            # 获取基金类型和规模
            fund_info.fund_kind = re.search(r'基金类型：(?:<a.*?>|)(.*?)[<&]', page_context)
            fund_info.fund_kind = fund_info.fund_kind.group(1) if fund_info.fund_kind is not None else "解析基金类型失败"
            fund_info.set_fund_info('基金规模',
                                    re.search(r'基金规模</a>：((?:\d+(?:\.\d{2}|)|--)亿元.*?)<', page_context).group(1))

            # 按照基金类型分类并获取其收益数据
            if fund_info.fund_kind in ParseDefault.fund_kind_belong_to_index:
                achievement_re = re.search(
                    r'：.*?((?:-?\d+\.\d{2}%)|--).*?'.join(ParseDefault.parse_index_for_index_fund + ['基金类型']),
                    page_context)
            elif fund_info.fund_kind in ParseDefault.fund_kind_belong_to_guaranteed:
                achievement_re = re.search(
                    r'(?:：|).*?((?:-?\d+\.\d{2}%)|--).*?'.join(ParseDefault.parse_index_for_guaranteed_fund + ['基金类型']),
                    page_context)
            elif fund_info.fund_kind in ParseDefault.fund_kind_belong_to_closed_period:
                achievement_re = re.search(r'最近约定年化收益率(?:<.*?>)(-?\d+\.\d{2}%)<', page_context)
            else:
                print(f'出现无解析方法的基金种类 {fund_info}')
                achievement_re = None

            if achievement_re is not None:
                # 清洗基金收益率
                if fund_info.fund_kind in ParseDefault.fund_kind_belong_to_index:
                    tem_header = ParseDefault.parse_index_for_index_fund
                elif fund_info.fund_kind in ParseDefault.fund_kind_belong_to_guaranteed:
                    tem_header = ParseDefault.parse_index_for_guaranteed_fund
                else:
                    tem_header = ParseDefault.parse_index_for_capital_preservation_fund
                for header, value in zip(tem_header, achievement_re.groups()):
                    fund_info.set_fund_info(header, value)
                fund_info.next_step = 'parsing_manager'
                # 清洗 基金经理在本基金的任职时间和收益率 和基金经理信息及其主页链接
                fund_manager_detail = re.search(
                    r'</td> {2}<td class="td03">(.+?|-)</td> {2}<td class="td04 bold (?:ui-colo'
                    r'r-(?:red|green)|)">(-?\d+\.\d{2}%|--)</td></tr>', page_context)
                if fund_manager_detail is not None:
                    fund_info.set_fund_info('任职时间', fund_manager_detail.group(1))
                    fund_info.set_fund_info('任期收益', fund_manager_detail.group(2))
                    fund_managers = re.findall(r'(?:<a href="(.*?)">(.+?)</a>&nbsp;&nbsp;)',
                                               re.search(r'<td class="td02">(?:<a href="(.*?)">(.+?)</a>&nbsp;&nbsp;)+',
                                                         page_context).group(0))
                    fund_info.manager_need_process_list = fund_managers
                else:
                    print(f'出现无法解析基金经理的基金 {fund_info}')
                    fund_info.next_step = 'writing_file'
            else:
                print(f'出现无法解析收益的基金 {fund_info}')
                fund_info.next_step = 'writing_file'

            page_context, fund_info = yield fund_info

    @classmethod
    def _parse_manager_info(cls):
        """
        对基金经理的信息进行解析 通过send(page_context,fund_info)来获得解析
        """
        # 挖坑 下次重构获取基金经理名称，与爬取部分做解耦
        page_context, fund_info = yield
        while True:
            manager_info = re.search('<span>累计任职时间：</span>(.*?)<br />', page_context)
            fund_info.set_manager_info(fund_info.manager_need_process_list.pop()[1], manager_info.group(1))
            if len(fund_info.manager_need_process_list) == 0:
                fund_info.next_step = 'writing_file'
            page_context, fund_info = yield fund_info

    @classmethod
    def _write_to_file(cls, first_crawling):
        """
        将爬取到的信息逐行保存到文件 保存内容通过send()发送 (FundInfo)
        当基金类型为None时，保存文件过程结束，释放所有句柄，并抛出StopIteration
        :param first_crawling: 是否是第一次爬取，这决定了是否会重新写保存文件（清空并写入列索引）
        """
        open_mode = 'w' if first_crawling else 'a'
        filename_handle = dict()
        from os.path import exists
        if not exists(ParseDefault.result_dir):
            from os import makedirs
            makedirs(ParseDefault.result_dir)
        # 保存文件的第一行（列索引）
        write_format_of_index = ['基金名称', '基金代码', '基金规模', '近1月', '近3月', '近6月', '近1年', '近3年', '成立来', '基金经理', '任职时间',
                                 '任期收益',
                                 '总任职时间']
        write_format_of_guaranteed = ['基金名称', '基金代码', '基金规模', '保本期收益', '近1月', '近3月', '近6月', '近1年', '近3年', '基金经理',
                                      '任职时间',
                                      '任期收益', '总任职时间']
        write_format_of_capital_preservation = ['基金名称', '基金代码', '基金规模', '最近约定年化收益率', '基金经理', '任职时间', '任期收益', '总任职时间']

        fund_info = yield
        while fund_info is not None:
            if fund_info.fund_kind not in filename_handle.keys():
                # 此基金类型的文件尚未打开过
                f = open(ParseDefault.result_dir + fund_info.fund_kind + '.csv', open_mode)
                filename_handle[fund_info.fund_kind] = f
                if fund_info.fund_kind in ParseDefault.fund_kind_belong_to_index:
                    header = ','.join(write_format_of_index) + '\n'
                elif fund_info.fund_kind in ParseDefault.fund_kind_belong_to_guaranteed:
                    header = ','.join(write_format_of_guaranteed) + '\n'
                else:
                    header = ','.join(write_format_of_capital_preservation) + '\n'
                f.write(header)
            else:
                f = filename_handle[fund_info.fund_kind]

            # 按照列索引，取出基金数据并写入文件
            if fund_info.fund_kind in ParseDefault.fund_kind_belong_to_index:
                index = write_format_of_index
            elif fund_info.fund_kind in ParseDefault.fund_kind_belong_to_guaranteed:
                index = write_format_of_guaranteed
            else:
                index = write_format_of_capital_preservation
            f.write(fund_info.get_info(index))
            f.write('\n')
            fund_info = yield

        for i in filename_handle.values():
            i.close()
