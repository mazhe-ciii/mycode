#!/usr/bin/env python3
#-*-coding:utf-8-*-
"""Copyright: Copyright (c) 2018 Asiainfo
    @Description: ***新建，填写用途***
    @fileName: xfer_log.py
    @version: v1.0.0
    @author:  mazhe
    @created on: 2018/9/4 15:59
"""


import logging
from logging.handlers import TimedRotatingFileHandler
import os
import configparser
from datetime import datetime
from main import config_file


cfg = configparser.ConfigParser()
cfg.read(config_file, encoding='utf-8')
log_path = cfg.get('common', 'log_path')
log_prefix = cfg.get('common', 'log_prefix')
log_time = datetime.now().strftime('%Y%m%d')
log_name = '%s.log' % (log_prefix, )
log_file = os.path.join(log_path, log_name)

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
# fh = logging.FileHandler(log_file)
fh = TimedRotatingFileHandler(log_file, when='d', interval=1)
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(processName)s pid:%(process)s  [%(levelname)s]: %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)


