#!/usr/bin/env python3
# -*-coding:utf-8-*-
"""Copyright: Copyright (c) 2018 Asiainfo
    @Description: ***新建，填写用途***
    @fileName: log.py
    @version: v1.0.0
    @author:  mazhe
    @created on: 2018/10/24 3:23 PM 17:29
"""

import os
import logging
from datetime import datetime

start_time = datetime.now()

# 日志路径
# log_path = '/hadoop/data1/aidemp/log/sync_taxpayer_info/'
log_path = '/Users/mazhe/mycode/sync_taxpayer_info'
log_name = 'sync_taxpayer_info.{}.log'.format(start_time.strftime('%Y%m%d'))
log_file = os.path.join(log_path, log_name)

logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s %(processName)s pid:%(process)s  '
                              '[%(levelname)s]: %(message)s')
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)
