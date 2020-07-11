# -*-coding:utf-8-*-
"""
定义了程序中用到的数据结果
"""


class FundInfo:
    """
    基金信息
    """

    def __init__(self):
        # 基金类型 基金信息字典 基金经理信息字典 当前基金信息类状态（下一步） 需要解析的基金经理列表
        self.fund_kind = 'Unknown'
        self._fund_info = dict()
        self._manager_info = dict()
        self.next_step = 'parsing_fund'
        self.manager_need_process_list = list()

    def get_info(self, index: list = None, missing: str = '??'):
        """
        获取基金信息
        :param index: 基金信息的列索引，若无，则按照保存信息的字典给出的哈希顺序
        :param missing: 列索引无对应值的填充
        :return: str 按照给定的列索引返回基金信息，信息之间以 , 分割
        """
        if index is None:
            return ','.join(list(self._fund_info.values()) + ['/'.join(self._manager_info.keys()),
                                                              '/'.join(self._manager_info.values())])
        else:
            return ','.join(self._get_info(i, missing) for i in index)

    def _get_info(self, index: str, missing: str):
        """
        内部的获取基金信息的方法
        :param index: 要获取的基金信息索引（key）
        :param missing: 列索引无对应值的填充
        :return: str 对应的基金信息
        """
        if index in self._fund_info.keys():
            return self._fund_info[index]
        elif index == '基金经理' or index == '总任职时间':
            return '/'.join(self._manager_info.keys()) if index == '基金经理' else '/'.join(self._manager_info.values())
        else:
            return str(missing)

    def set_fund_info(self, key: str, value: str):
        """
        设置基金信息
        :param key: 基金信息索引
        :param value: 基金信息
        """
        self._fund_info[key] = str(value)

    def set_manager_info(self, key, value):
        """
        设置基金经理信息
        :param key: 基金经理姓名
        :param value: 基金经理信息（目前为str 基金经理的总任职时长）
        """
        self._manager_info[key] = value

    def get_fund_basic_info(self):
        """
        获取基金的基本信息，代码名称
        :return: (基金名称，基金代码）
        """
        name = self._fund_info.get('基金名称', '无')
        code = self._fund_info.get('基金代码', '无')
        return name, code

    def __repr__(self):
        return self.get_info()
