#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
# author : mazhe
# Date   : 2018/6/20 22:49
# File   : ftp4BackupFile.py
# Desp   : ftp程序用来备份话单到远端主机
"""

import ftplib
import os
import time
import logging
import shutil
import random
import string
from multiprocessing import Pool
from datetime import datetime, timedelta
import ConfigParser
import sys


def ftp_login(user, passwd, ip, port):
    """
    :param user: ftp用户名
    :param passwd: ftp密码
    :param ip: 对端ip地址
    :param port: 对端ftp端口，默认30021
    :return: 返回登录成功后ftp对象
    """
    ftp = ftplib.FTP()
    ftp.set_debuglevel(0)
    while True:
        try:
            ftp.connect(ip, port, 300)
            ftp.login(user, passwd)
            break
        except Exception as errmsg:
            logger.error('Login error,relogin in 10s later,reason:' + str(errmsg))
            time.sleep(10)  # 10s后重试
    return ftp


def ftp_get_remote_file(ftp, remote_path, size_mod_num=0, mod=0):
    """
    :param ftp: ftpclass对象，由ftp_login返回
    :param remote_path: 远端下载目录
    :param size_mod_num: 文件大小取模数，用于多进程
    :param mod: 按size_mod_num取模后的值
    :return: 返回待传输文件列表，元素为文件详细信息
    """
    file_list = []   # 初始化获取文件列表
    resul_tlist = []
    try:
        ftp.cwd(remote_path)
        ftp.dir('.', file_list.append)
    except Exception as errmsg:
        logger.error('Get file list error,reason:' + str(errmsg))
        return
    # 判断是否是文件，只传输文件，忽略文件夹,d开头为文件夹不下载
    for item in file_list:
        if item.startswith('d'):
            continue
        # 需要取模数和模值都不为null，只保留按size_mod_num取模后相应mod模值的文件
        if size_mod_num != 0:
            szie = ftp.size(item.split(' ')[-1])
            if (szie % size_mod_num) == mod:
                resul_tlist.append(item)
        else:
            resul_tlist.append(item)
    return resul_tlist


def ftp_get_local_file(local_path, size_mod_num=1, mod=0):
    """
    :param local_path: 需要上传文件的目录
    :param size_mod_num: 文件大小取模数，默认值为1，即不分组
    :param mod: 按size_mod_num取模后数值
    :return: 待传输文件列表,绝对路径
    """
    result_list = []
    file_list = os.listdir(local_path)
    logger.info('Get %s full file list done.' % (local_path, ))
    for item in file_list:
        file_path = os.path.join(local_path, item)
        # 只上传文件，localpath下的目录剔除
        if os.path.isfile(file_path):
            # 需要将文件列表按大小取模分组
            if size_mod_num != 1:
                size = os.path.getsize(file_path)
                if (size % size_mod_num) == int(mod):
                    result_list.append(file_path)
            # 不需要将文件列表按大小取模分组
            else:
                result_list.append(file_path)
    logger.info('Get deal file list done.')
    return result_list


def ftp_download_file(ftp, file_list, local_path):
    """
    :param ftp: ftp对象
    :param file_list: 文件列表，由ftp.dir()返回
    :param local_path: 本地下载目录
    :return: 无
    """
    buffersize = 1024
    file_num = len(file_list)
    if file_num == 0:
        logger.info('This time has no file to deal,ftp quit .')
        return
    else:
        logger.info('This time download file number: %s.' % (file_num, ))
    # 检查临时目录
    local_tmp = os.path.join(local_path, 'tmp')
    if not os.path.exists(local_tmp):
        os.makedirs(local_tmp)
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    for item in file_list:
        src_file = str(item.split(' ')[-1])
        # 开始下载
        try:
            tmp_dest_file = os.path.join(local_tmp, get_tmp_name(src_file))
            logger.info('Start download %s to %s' % (src_file, tmp_dest_file))
            filehandle = open(tmp_dest_file, 'wb').write
            ftp.retrbinary('RETR ' + src_file, filehandle, buffersize)   # 下载文件到临时目录
            shutil.move(tmp_dest_file, local_path) # 移动到正式目录
            logger.info('Move tmpfile %s to %s succed' % (tmp_dest_file, local_path))
            ftp.delete(src_file)
            logger.info('Delete srcfile %s succed' % (src_file, ))
            logger.info('Download %s succed' % (src_file,))
        except Exception as errmsg:
            logger.error("Download %s fail,reason:%s", (src_file, errmsg))
            continue
        ftp.quit()


def ftp_upload_file(user, passwd, ip, port, remote_path, local_path, size_mod_num, mod):
    """
    :param user: ftp用户名
    :param passwd: ftp密码
    :param ip: 对端ip地址
    :param port: 对端ftp端口，默认30021
    :return: 返回登录成功后ftp对象
    :param remote_path: 上传的远端目录
    :param local_path: 本地待传输目录
    :param size_mod_num: 文件大小取模数
    :param mod: 取模后数值
    :return:无
    """
    buffersize = 1024
    file_list = ftp_get_local_file(local_path, size_mod_num, mod)
    ftp = ftp_login(user, passwd, ip, port)
    file_num = len(file_list)
    if file_num == 0:
        logger.info('This time has no file to deal,ftp quit .')
        return
    else:
        logger.info('This time upload file number: %s.' % (file_num, ))
    remote_tmp = os.path.join(remote_path, 'tmp')
    done_num = 1
    for src_file in file_list:
        # 开始上传
        try:
            tmp_dest_file = os.path.join(remote_tmp, get_tmp_name(os.path.basename(src_file)))
            dest_file = os.path.join(remote_path, os.path.basename(src_file))
            logger.info('[%s/%s] Start upload %s to %s' % (done_num, file_num, src_file, tmp_dest_file))
            with open(src_file, 'rb') as filehandle:
                ftp.storbinary('STOR ' + tmp_dest_file, filehandle, buffersize)
            ftp.rename(tmp_dest_file, dest_file)
            logger.info('[%s/%s] Rename tmpfile %s to %s succed' % (done_num, file_num, tmp_dest_file, dest_file))
            os.remove(src_file)
            logger.info('[%s/%s] Delete srcfile %s succed' % (done_num, file_num, src_file))
            logger.info('[%s/%s] Upload  %s succed' % (done_num, file_num, src_file))
            done_num += 1
        except Exception as errmsg:
            logger.error('[%s/%s] Upload  %s fail,reason:%s' % (done_num, file_num, src_file, errmsg))
            done_num += 1
            continue
    ftp.quit()


def ftp_check_remote_dir(user, passwd, ip, port, remote_path):
    """
    :param user: ftp用户名
    :param passwd: ftp密码
    :param ip: 远端ip
    :param port: ftp端口
    :param remote_path: 远端路径
    :return: NONE
    """
    ftp = ftp_login(user, passwd, ip, port)
    # 检查上传临时目录及目标目录是否存在
    remote_tmp = os.path.join(remote_path, 'tmp')
    try:
        ftp.cwd(remote_path)
    except Exception as errmsg:
        logger.error('remote_path directory %s is not exists,create it.reason: %s' % (remote_path, errmsg))
        ftp.mkd(remote_path)
    try:
        ftp.cwd(remote_tmp)
    except Exception as errmsg:
        logger.error('remote_tmp directory %s is not exists,create it.reason: %s' % (remote_tmp, errmsg))
        ftp.mkd(remote_tmp)
    ftp.quit()



def get_tmp_name(name):
    """
    :param name: 文件名
    :return: 返回增加随机8位英文字符的文件名
    """
    # 8位随机英文字符包含大小写
    ran_seq = ''.join(random.sample(string.ascii_letters, 8))
    tmp_name = ran_seq + name
    return tmp_name


def check_local_dir(local_path):
    """
    :param local_path: 检查目录
    :return: True目录无文件，False目录有文件
    """
    file_list = os.listdir(local_path)
    file_num = len(file_list)
    if file_num == 0:
        logger.info('Local path %s is empty' % (local_path, ))
        return True
    else:
        logger.info('Local path still has %s files' % (file_num, ))
        return False

# 开始上传文件
if __name__ == '__main__':
    # 读取配置中配置项
    cfg = ConfigParser.ConfigParser()
    # 获取当前执行程序的父目录
    file_path = os.path.split(os.path.abspath(__file__))[0]
    cfg_path = os.path.join(file_path, 'config', 'config_ori_xdr.ini')
    cfg.read(cfg_path)
    deal_type = str(sys.argv[1])
    log_prefix = cfg.get('common', 'log_prefix')
    log_path = cfg.get('common', 'log_path')
    user = cfg.get('common', 'user')
    passwd = cfg.get('common', 'passwd')
    port = int(cfg.get('common', 'port'))
    process_num = int(cfg.get(deal_type, 'process_num'))
    remote_ip = cfg.get(deal_type, 'remote_ip')
    remote_path = cfg.get(deal_type, 'remote_path')
    local_path = cfg.get(deal_type, 'local_path')
    backup_day = cfg.get(deal_type, 'backup_day')
    # deal_day = (datetime.now() + timedelta(days=int(backup_day))).strftime('%Y%m%d')
    deal_day = '20180717'
    # local_path 与 deal_day 拼接为处理目录， remote_path 与 deal_day 拼接为远端目录
    local_path = os.path.join(local_path, deal_day)
    remote_path = os.path.join(remote_path, deal_day)
    # local_path = '/data01/bak/split/ggprs/bakfiles/20180703'
    # remote_path = '/data/ori_xdr_backup/ggprs/20180703'
    # log_name = '%s_%s_%s.log' % (log_prefix, deal_type, datetime.now().strftime('%Y%m%d'))
    log_name = '%s_%s_%s.log' % (log_prefix, deal_type, deal_day)
    # 设置日志文件输出
    log_name = os.path.join(log_path, log_name)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(processName)s pid:%(process)s  [%(levelname)s]: %(message)s')
    filehandle = logging.FileHandler(log_name)
    filehandle.setLevel(logging.INFO)
    filehandle.setFormatter(formatter)
    logger.addHandler(filehandle)

    # 检查远端目录
    ftp_check_remote_dir(user, passwd, remote_ip, port, remote_path)
    # 防止因报错导致部分文件没有上传，循环执行，当local_path不再有文件，程序退出
    while True:
        # 一次获取待处理文件列表，按照preocess_num 分组
        p = Pool(process_num)
        for i in range(process_num):
            p.apply_async(ftp_upload_file, args=(user, passwd, remote_ip, port, remote_path, local_path, process_num, i))
        p.close()
        p.join()
        if check_local_dir(local_path):
            break
    logger.info('Delete local path ' + local_path)
    os.rmdir(local_path)
    logger.info('Upload %s to %s is done.' % (local_path, remote_path))

