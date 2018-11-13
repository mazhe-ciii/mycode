#!/usr/bin/env python3
# -*-coding:utf-8-*-
"""Copyright: Copyright (c) 2018 Asiainfo
    @Description: ***获取目录积压情况主要类***
    @fileName: monitor_dir_files.py
    @version: v1.0.0
    @author:  mazhe
    @created on: 2018/11/1 2:29 PM 17:29

    最早时间：时间中最小值
    新增量：本次统计量中时间大于上次统计时间的量
    处理量：本次统计时间内的文件数中，时间小于上次统计时间的数量
    处理速度： 处理量/轮训时间
"""


import ftplib
import time
from log import logger


def _get_file_time(file_list):
    """获取ftp获取远端的文件详细列表中的时间，转换为时间戳
    -rw-rw-rw-    1 604  700  345 Oct 20 16:00 E910_63917502_20181020000002.gz
    :param: file_list 文件详细列表信息
    :return: 返回所有文件的时间戳列表
    """
    result = []
    for item in file_list:
        try:
            tmp_list = item.split()
            month = tmp_list[5]  # Oct
            day = tmp_list[6]  # 21
            clock = tmp_list[7]  # 16:00
            time_string = "2018-{}-{} {}:00".format(month, day, clock)
            format_file_time = time.strptime(time_string, "%Y-%b-%d %H:%M:%S")
            file_time = time.mktime(format_file_time)
            result.append(file_time)
        except Exception as errmsg:
            logger.error(str(errmsg))
            continue
    return result


def _get_min_file_time(file_time_list):
    """
    根据传入的时间戳列表，获取列表中最早的时间，并格式化该时间戳
    :param file_time_list: 一组时间戳组成的列表
    :return: 该列表中最早的时间，格式 YYYY-MM-DD HH:MI:SS
    """
    min_time = min(file_time_list)
    file_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(min_time))
    # min_time.strftime("%Y-%m-%d %H:%M:%S")
    return file_time


class MonitorFtp:
    """
    监控远端目录积压的ftp登陆类，监控一个或者一组远端目录内的目录积压情况
    """
    relogin_time = 5  # 登陆失败重试时间

    def __init__(self, user, password, remote_ip, port):
        self.__user = str(user)
        self.__password = str(password)
        self.__remote_ip = str(remote_ip)
        self.__port = int(port)

    def __login_remote(self):
        """
        ftp登陆方法
        :return:无
        """
        self.__ftp = ftplib.FTP()
        self.__ftp.set_debuglevel(0)
        while True:
            try:
                self.__ftp.connect(self.__remote_ip, self.__port, 300)
                self.__ftp.login(self.__user, self.__password)
                logger.info("Host {0},port: {1} login success".format(
                    self.__remote_ip, self.__port))
                break
            except Exception as errmsg:
                logger.error("Login {0}:{1} error,login in 10s later,reason:"
                             "{2}".format(self.__remote_ip, self.__port,
                                          str(errmsg)))
                time.sleep(self.relogin_time)  # 10s后重试

    def get_remote_file(self, *args):
        """
        根据传入的绝对路径返回当前目录下的文件数量
        :return:
        """
        result = {}
        self.__login_remote()
        for remote_dir in args:
            file_list = list()  # 每个目录下的文件列表
            try:
                self.__ftp.cwd(remote_dir)
            except Exception as errmsg:
                logger.error("Remote dir :{} is not exist!error message:{}"
                             .format(remote_dir, str(errmsg)))
                continue
            self.__ftp.dir('.', file_list.append)
            result[remote_dir] = file_list
        self.__ftp.quit()
        return result


class FormatOutput:
    """
    格式化输出监控信息，入参为以目录为key，目录下文件列表为value的dict
    """
    def __init__(self, base_dict):
        self.__base_dict = base_dict
        if not isinstance(self.__base_dict, dict):
            logger.error("Args:base_dict type error,not a dict!")
            exit(1)

    def __get_check_tiem(self):
        return time.mktime(time.localtime())




