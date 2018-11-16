#!/usr/bin/env python3
# -*-coding:utf-8-*-
"""Copyright: Copyright (c) 2018 Asiainfo
    @Description: ***新建，填写用途***
    @fileName: run.py
    @version: v1.0.0
    @author:  mazhe
    @created on: 2018/11/15 2:20 PM 17:29
"""

import time
import monitor_dir_files


def scan():

    test = monitor_dir_files.MonitorFtp("jfrun", "Ngtst!234", "10.4.86.26", 30021)
    file_list = ["/data/offline/rpling/gsm/in"]

    result_dict = {}
    old_time = 0
    scan_time = 5

    while True:
        this_time = monitor_dir_files.__get_check_time()
        result = test.get_remote_file(file_list)
        for path, value in result.items():
            increase_num = 0
            deal_num = 0
            deal_speed = 0
            his_num = 0
            time_list = monitor_dir_files._get_file_time(value)
            min_time = monitor_dir_files._get_min_file_time(time_list)
            this_num = len(time_list)
            print("old_time={},time_list={}".format(old_time, time_list))
            if old_time != 0:
                # 第一次统计，无增加量，处理量，处理速度
                # 非第一统计，开始计算增加量，处理量，处理速度
                for item in time_list:
                    if item >= old_time:    # 大于上次统计时间，为新增
                        increase_num += 1
                    else:   # 小于上次统计时间，为历史量
                        his_num += 1

                    deal_num = int(result_dict[path][0]) - his_num
                    deal_speed = int(deal_num / scan_time * 60)

            result_dict[path] = [this_num, min_time, deal_num, increase_num,
                                 deal_speed]

        old_time = this_time    # 捕获上次统计时间
        # 格式化输出
        for i in result_dict.items():
            print(i)
        time.sleep(scan_time)


if __name__ == "__main__":
    scan()
