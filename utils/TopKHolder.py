from collections import defaultdict
from heapq import heappush, heappushpop
from typing import List, Dict


class TopKHolder:
    """
    基于堆做的，只保留top k元素的容器
    """

    def __init__(self, key_function, k: int):
        self._k: int = k
        self._key_function = key_function
        self._heap: List = list()
        self._element_dict: Dict = defaultdict(list)

    def put(self, element) -> None:
        key = self._key_function(element)

        if len(self._heap) < self._k:
            # 容器未满直接push
            heappush(self._heap, key)
            self._element_dict[key].append(element)
        else:
            # 容器满了，push and pop一下
            min_key = heappushpop(self._heap, key)
            if min_key == key:
                return
            self._element_dict[min_key].pop()
            self._element_dict[key].append(element)

    def cur_k(self) -> List:
        result_list = []
        for value in self._element_dict.values():
            if value:
                result_list.extend(value)
        return result_list
