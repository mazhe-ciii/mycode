#!/usr/bin/env python3
# -*-coding:utf-8-*-
"""Copyright: Copyright (c) 2018 Asiainfo
    @Description: ***新建，填写用途***
    @fileName: xfer.py
    @version: v1.0.0
    @author:  mazhe
    @created on: 2018/9/5 16:12
"""

import ftplib
import configparser
import os
import time
from xfer_log import logger
import random
import string


class FtpXferInit:
    """
    ftp 初始化类，包括读取配置文件及启动程序
    """

    def __init__(self, config_file):
        self._config_file = config_file
        if not os.path.exists(self._config_file):
            raise FileExistsError(self._config_file + ' not exist ')

    def _get_conf(self):
        param_list = ['trans_type', 'remote_ip', 'user', 'passwd', 'port', 'remote_path', 'local_path', 'match_exp',
                      'process_num']
        param_result = {}
        cfg = configparser.ConfigParser()
        cfg.read(self._config_file, encoding='utf-8')

        for key in param_list:
            value = cfg.get('job', key)
            param_result[key] = value
        return param_result

    def main(self):
        param = self._get_conf()
        logger.info(param)
        task = xfer(param['trans_type'], param['remote_ip'], param['user'], param['passwd'], param['port'],
                    param['remote_path'], param['local_path'], param['match_exp'], param['process_num'])


def get_tmp_name(name):
    # 8位随机英文字符包含大小写
    ran_seq = ''.join(random.sample(string.ascii_letters, 8))
    tmp_name = ran_seq + name
    return tmp_name


class xfer:
    """
    ftp类
    """

    def __init__(self, trans_type, remote_ip, user, passwd, port, remote_path, local_path, match_exp, process_num):
        self._trans_type = trans_type
        self._remote_ip = remote_ip
        self._user = user
        self._passwd = passwd
        self._port = port
        self._remote_path = remote_path
        self._local_path = local_path
        self._match_exp = match_exp
        self._process_num = process_num

    def _ftp_login(self):
        ftp = ftplib.FTP()
        ftp.set_debuglevel(0)
        while True:
            try:
                ftp.connect(self._remote_ip, self._port, 300)
                ftp.login(self._user, self._passwd)
                break
            except Exception as errmsg:
                logger.error('Login error,relogin in 10s later,reason:' + str(errmsg))
                time.sleep(10)  # 10s后重试
        return ftp

    def _get_local_file(self):
        result_list = []
        file_list = os.listdir(self._local_path)
        logger.info('Get %s full file list done.' % (self._local_path,))
        for item in file_list:
            file_path = os.path.join(self._local_path, item)
            # 只上传文件，localpath下的目录剔除
            if os.path.isfile(file_path):
                result_list.append(file_path)
        logger.info('Get deal file list done.')
        return result_list

    def _get_remote_file(self):
        file_list = []  # 初始化获取文件列表
        result_list = []
        ftp = self._ftp_login()
        try:
            ftp.cwd(self._remote_path)
            ftp.dir('.', file_list.append)
        except Exception as errmsg:
            logger.error('Get file list error,reason:' + str(errmsg))
            return
        # 判断是否是文件，只传输文件，忽略文件夹,d开头为文件夹不下载
        for item in file_list:
            if item.startswith('d'):
                continue
            result_list.append(item)
        return result_list

    def download_file(self):
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



