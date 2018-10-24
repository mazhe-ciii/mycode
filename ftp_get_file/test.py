#!/usr/bin/env python3
# -*-coding:utf-8-*-
"""Copyright: Copyright (c) 2018 Asiainfo
    @Description: ***新建，填写用途***
    @fileName: test.py.py
    @version: v1.0.0
    @author:  mazhe
    @created on: 2018/7/7 14:02
"""

import ftpFunc
from multiprocessing import Pool
import os


if __name__ == '__main__':
    user = 'fsrun'
    passwd = 'diaBso3_wV'
    ip = '10.7.6.28'
    port = 30021
    local_path = '/app/mazhe/files'
    remote_path = '/data/ori_xdr_backup/gprs/test'
    process_num = 10
    result_dict = ftpFunc.ftp_get_local_file(local_path, process_num)
    p = Pool(process_num)
    for i in range(process_num):
        key = 'key' + str(i)
        file_list = result_dict[key]
        p.apply_async(ftpFunc.ftp_upload_file, args=(user, passwd, ip, port, remote_path, file_list))
    p.close()
    p.join()

