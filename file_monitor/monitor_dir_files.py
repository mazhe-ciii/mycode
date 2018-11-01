#!/usr/bin/env python3
# -*-coding:utf-8-*-
"""Copyright: Copyright (c) 2018 Asiainfo
    @Description: ***获取目录积压情况主要类***
    @fileName: monitor_dir_files.py
    @version: v1.0.0
    @author:  mazhe
    @created on: 2018/11/1 2:29 PM 17:29
"""


import ftplib
import time
from log import logger


def get_file_time(file_list):
    """获取ftp获取远端的文件详细列表中的时间，转换为时间戳
    -rw-rw-rw-    1 604  700  345 Oct 20 16:00 E910_63917502_20181020000002.gz
    :param: file_list 文件详细列表信息
    :return: 返回所有文件的时间戳列表
    """
    for item in file_list:
        tmp_list = item.split()
        month = tmp_list[5]  # Oct
        day = tmp_list[6]  # 21
        time = tmp_list[7]  # 16:00

    pass

class MonitorFtp:
    """
    监控远端目录积压的ftp登陆类，监控一个或者一组远端目录内的目录积压情况
    """
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
                self.__ftp.login(self.__user, self.__passwd)
                logger.info("Host {0},port: {1} login success".format(
                    self.__remote_ip, self.__port))
                break
            except Exception as errmsg:
                logger.error("Login {0}:{1} error,login in 10s later,reason:"
                             "{2}".format(self.__remote_ip, self.__port,
                                          str(errmsg)))
                time.sleep(10)  # 10s后重试

    def get_remote_file(self, *args):
        """
        根据传入的绝对路径返回当前目录下的文件数量
        :return:
        """
        for remote_dir in args:
            file_list = list()  # 每个目录下的文件列表
            try:
                self.__ftp.cwd(remote_dir)
            except Exception as errmsg:
                logger.error("Remote dir :{} is not exist!error message:{}"
                             .format(remote_dir, str(errmsg)))
                continue
            self.__ftp.dir('.', file_list.append)





