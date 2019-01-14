#!/usr/bin/env python3
# -*-coding:utf-8-*-
"""Copyright: Copyright (c) 2018 Asiainfo
    @Description: ***监控目录文件模块***
    @fileName: monitor.py
    @version: v1.0.0
    @author:  mazhe
    @created on: 2018/11/16 4:09 PM 17:29
"""

import os
import time
from log import logger
import common


def get_dir_files(dir_path):
    """
    根据dir_path，将目录下的文件存入result_list中
    :param dir_path: 文件目录
    :return: 无
    """
    if not os.path.isdir(dir_path):
        logger.error("dir_path: {0} is not exist!".format(dir_path))
        raise TypeError
    result_list = list()
    tmp_list = os.listdir(dir_path)
    for item in tmp_list:
        _path = os.path.join(dir_path, item)
        if not os.path.isdir(_path):
            file_time = os.path.getmtime(_path)
            result_list.append(file_time)
    return result_list


def get_check_time():
    """
    :return: 返回当前时间，时间戳
    """
    return time.mktime(time.localtime())


def get_min_file_time(file_time_list):
    """
    根据传入的时间戳列表，获取列表中最早的时间，并格式化该时间戳，当列表为空时，即目录下
    没文件，最早时间显示当前检查时间
    :param file_time_list: 一组时间戳组成的列表
    :return: 该列表中最早的时间，格式 YYYY-MM-DD HH:MI:SS
    """
    if len(file_time_list) == 0:
        min_time = get_check_time()
    else:
        min_time = min(file_time_list)
    file_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(min_time))
    return file_time


class Monitor:
    """
    负责收集指定集群的目录文件信息
    """
    interval = 5    # 监控轮询时间，default = 5s

    def __init__(self, dir_list):
        self.__dir_list = dir_list
        if not isinstance(self.__dir_list, list):
            logger.fatal("args dir_list :{0} is not a list "
                         .format(self.__dir_list))
            raise TypeError

    def run(self):
        """
        具体监控逻辑，将结果存储在result_dict字典中，
        :return:
        """
        result_dict = {}
        old_time = 0
        while True:
            this_time = get_check_time()
            for path in self.__dir_list:
                increase_num = 0
                deal_num = 0
                deal_speed = 0
                his_num = 0
                tmp_list = get_dir_files(path)
                min_time = get_min_file_time(tmp_list)
                this_num = len(tmp_list)
                if old_time != 0:
                    # 第一次统计，无增加量，处理量，处理速度
                    # 非第一统计，开始计算增加量，处理量，处理速度
                    for file_time in tmp_list:
                        if file_time >= old_time:  # 大于上次统计时间，为新增
                            increase_num += 1
                        else:  # 小于上次统计时间，为历史量
                            his_num += 1

                    deal_num = int(result_dict[path][0]) - his_num
                    deal_speed = int(deal_num / self.interval * 60)

                result_dict[path] = [this_num, min_time, deal_num, increase_num,
                                     deal_speed, this_time]
            # 展示
            for item in result_dict.items():
                print(item)

            old_time = this_time  # 捕获上次统计时间
            time.sleep(self.interval)


def main():
    """
    程序启动函数
    :return:
    """
    dir_list = common.get_monitor_dirs()
    print(type(dir_list))
    print(dir_list)
    monitor = Monitor(dir_list)
    monitor.run()
