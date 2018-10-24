#!/usr/bin/env python3
#-*-coding:utf-8-*-
"""Copyright: Copyright (c) 2018 Asiainfo
    @Description: ***新建，填写用途***
    @fileName: main.py
    @version: v1.0.0
    @author:  mazhe
    @created on: 2018/9/5 17:29
"""


config_file = r'/Users/mazhe/PycharmProjects/untitled/own_code/ftpGetFIle/config/test_config.ini'

if __name__ == '__main__':

    import xfer_main
    task = xfer_main.FtpXferInit(config_file)
    task.main()
