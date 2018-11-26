#!/usr/bin/env python3
# -*-coding:utf-8-*-
"""Copyright: Copyright (c) 2018 Asiainfo
    @Description: ***公共功能，获取监控列表，根据主机名称获取监控集群名称等***
    @fileName: common.py
    @version: v1.0.0
    @author:  mazhe
    @created on: 2018/11/19 11:38 AM 17:29
"""

import socket
import cx_Oracle
from ora_sql_exec import sql_dml, sql_select, many_insert
from log import logger


class GetClusterError(BaseException): pass


# 集群列表，包含主机名，主机ip，集群名称
__cluster = [
    {"host_name": "xbal1102", "ip": "10.4.86.11", "cluster": "JM1"},
    {"host_name": "xbal1402", "ip": "10.4.86.13", "cluster": "JM1"},
    {"host_name": "xbal1302", "ip": "10.4.86.12", "cluster": "JM1"},
    {"host_name": "xbal1502", "ip": "10.4.86.14", "cluster": "JM1"},
    {"host_name": "xbal1103", "ip": "10.4.86.15", "cluster": "JM2"},
    {"host_name": "xbal1303", "ip": "10.4.86.16", "cluster": "JM2"},
    {"host_name": "xbal1403", "ip": "10.4.86.17", "cluster": "JM2"},
    {"host_name": "xbal1503", "ip": "10.4.86.18", "cluster": "JM2"},
    {"host_name": "xbal1104", "ip": "10.4.86.19", "cluster": "JM3"},
    {"host_name": "xbal1304", "ip": "10.4.86.20", "cluster": "JM3"},
    {"host_name": "xbal1404", "ip": "10.4.86.21", "cluster": "JM3"},
    {"host_name": "xbal1504", "ip": "10.4.86.22", "cluster": "JM3"},
    {"host_name": "xbal0603", "ip": "10.4.86.135", "cluster": "JM4"},
    {"host_name": "xbal0803", "ip": "10.4.86.136", "cluster": "JM4"},
    {"host_name": "xbal0903", "ip": "10.4.86.137", "cluster": "JM4"},
    {"host_name": "xbal1003", "ip": "10.4.86.138", "cluster": "JM4"},
    {"host_name": "xbaj1101", "ip": "10.4.86.23", "cluster": "JF1"},
    {"host_name": "xbaj1301", "ip": "10.4.86.24", "cluster": "JF1"},
    {"host_name": "xbaj1401", "ip": "10.4.86.25", "cluster": "JF1"},
    {"host_name": "xbaj1501", "ip": "10.4.86.26", "cluster": "JF1"},
    {"host_name": "xbaj1102", "ip": "10.4.86.27", "cluster": "JF2"},
    {"host_name": "xbaj1302", "ip": "10.4.86.28", "cluster": "JF2"},
    {"host_name": "xbaj1402", "ip": "10.4.86.29", "cluster": "JF2"},
    {"host_name": "xbaj1502", "ip": "10.4.86.30", "cluster": "JF2"},
    {"host_name": "xbaj1103", "ip": "10.4.86.31", "cluster": "JF3"},
    {"host_name": "xbaj1403", "ip": "10.4.86.32", "cluster": "JF3"},
    {"host_name": "xbaj1303", "ip": "10.4.86.33", "cluster": "JF3"},
    {"host_name": "xbaj1503", "ip": "10.4.86.34", "cluster": "JF3"},
    {"host_name": "xbaj1104", "ip": "10.4.86.35", "cluster": "JF4"},
    {"host_name": "xbaj1304", "ip": "10.4.86.36", "cluster": "JF4"},
    {"host_name": "xbaj1404", "ip": "10.4.86.37", "cluster": "JF4"},
    {"host_name": "xbaj1504", "ip": "10.4.86.38", "cluster": "JF4"},
    {"host_name": "xbaj1602", "ip": "10.4.86.39", "cluster": "JF5"},
    {"host_name": "xbaj1802", "ip": "10.4.86.40", "cluster": "JF5"},
    {"host_name": "xbaj1902", "ip": "10.4.86.41", "cluster": "JF5"},
    {"host_name": "xbaj2002", "ip": "10.4.86.42", "cluster": "JF5"},
    {"host_name": "xbaj1603", "ip": "10.4.86.43", "cluster": "JF6"},
    {"host_name": "xbaj1803", "ip": "10.4.86.44", "cluster": "JF6"},
    {"host_name": "xbaj1903", "ip": "10.4.86.45", "cluster": "JF6"},
    {"host_name": "xbaj2003", "ip": "10.4.86.46", "cluster": "JF6"},
    {"host_name": "xbaj1604", "ip": "10.4.86.47", "cluster": "JF7"},
    {"host_name": "xbaj1804", "ip": "10.4.86.48", "cluster": "JF7"},
    {"host_name": "xbaj1904", "ip": "10.4.86.49", "cluster": "JF7"},
    {"host_name": "xbaj2004", "ip": "10.4.86.50", "cluster": "JF7"},
    {"host_name": "xbak0101", "ip": "10.4.86.51", "cluster": "JF8"},
    {"host_name": "xbak0201", "ip": "10.4.86.52", "cluster": "JF8"},
    {"host_name": "xbak0401", "ip": "10.4.86.53", "cluster": "JF8"},
    {"host_name": "xbak0501", "ip": "10.4.86.54", "cluster": "JF8"},
    {"host_name": "xbak0102", "ip": "10.4.86.55", "cluster": "JF9"},
    {"host_name": "xbak0202", "ip": "10.4.86.56", "cluster": "JF9"},
    {"host_name": "xbak0402", "ip": "10.4.86.57", "cluster": "JF9"},
    {"host_name": "xbak0502", "ip": "10.4.86.58", "cluster": "JF9"},
    {"host_name": "xbak0103", "ip": "10.4.86.59", "cluster": "JF10"},
    {"host_name": "xbak0203", "ip": "10.4.86.60", "cluster": "JF10"},
    {"host_name": "xbak0403", "ip": "10.4.86.61", "cluster": "JF10"},
    {"host_name": "xbak0503", "ip": "10.4.86.62", "cluster": "JF10"},
    {"host_name": "xbak0104", "ip": "10.4.86.63", "cluster": "JF11"},
    {"host_name": "xbak0204", "ip": "10.4.86.64", "cluster": "JF11"},
    {"host_name": "xbak0404", "ip": "10.4.86.65", "cluster": "JF11"},
    {"host_name": "xbak0504", "ip": "10.4.86.66", "cluster": "JF11"},
    {"host_name": "xbak0601", "ip": "10.4.86.67", "cluster": "JF12"},
    {"host_name": "xbak0701", "ip": "10.4.86.68", "cluster": "JF12"},
    {"host_name": "xbak0901", "ip": "10.4.86.69", "cluster": "JF12"},
    {"host_name": "xbak1001", "ip": "10.4.86.70", "cluster": "JF12"},
    {"host_name": "xbak0602", "ip": "10.4.86.71", "cluster": "JF13"},
    {"host_name": "xbak0702", "ip": "10.4.86.72", "cluster": "JF13"},
    {"host_name": "xbak0902", "ip": "10.4.86.73", "cluster": "JF13"},
    {"host_name": "xbak1002", "ip": "10.4.86.74", "cluster": "JF13"},
    {"host_name": "xbak0603", "ip": "10.4.86.75", "cluster": "JF14"},
    {"host_name": "xbak0703", "ip": "10.4.86.76", "cluster": "JF14"},
    {"host_name": "xbak0903", "ip": "10.4.86.77", "cluster": "JF14"},
    {"host_name": "xbak1003", "ip": "10.4.86.78", "cluster": "JF14"},
    {"host_name": "xbak0604", "ip": "10.4.86.79", "cluster": "JF15"},
    {"host_name": "xbak0704", "ip": "10.4.86.80", "cluster": "JF15"},
    {"host_name": "xbak0904", "ip": "10.4.86.81", "cluster": "JF15"},
    {"host_name": "xbak1004", "ip": "10.4.86.82", "cluster": "JF15"},
    {"host_name": "xbak1101", "ip": "10.4.86.83", "cluster": "JF16"},
    {"host_name": "xbak1201", "ip": "10.4.86.84", "cluster": "JF16"},
    {"host_name": "xbak1401", "ip": "10.4.86.85", "cluster": "JF16"},
    {"host_name": "xbak1501", "ip": "10.4.86.86", "cluster": "JF16"},
    {"host_name": "xbak1102", "ip": "10.4.86.87", "cluster": "JF17"},
    {"host_name": "xbak1202", "ip": "10.4.86.88", "cluster": "JF17"},
    {"host_name": "xbak1402", "ip": "10.4.86.89", "cluster": "JF17"},
    {"host_name": "xbak1502", "ip": "10.4.86.90", "cluster": "JF17"},
    {"host_name": "xbak1103", "ip": "10.4.86.91", "cluster": "JF18"},
    {"host_name": "xbak1203", "ip": "10.4.86.92", "cluster": "JF18"},
    {"host_name": "xbak1403", "ip": "10.4.86.93", "cluster": "JF18"},
    {"host_name": "xbak1503", "ip": "10.4.86.94", "cluster": "JF18"},
    {"host_name": "xbak1104", "ip": "10.4.86.95", "cluster": "JF19"},
    {"host_name": "xbak1204", "ip": "10.4.86.96", "cluster": "JF19"},
    {"host_name": "xbak1404", "ip": "10.4.86.97", "cluster": "JF19"},
    {"host_name": "xbak1504", "ip": "10.4.86.98", "cluster": "JF19"},
    {"host_name": "xbak1602", "ip": "10.4.86.99", "cluster": "JF20"},
    {"host_name": "xbak1702", "ip": "10.4.86.100", "cluster": "JF20"},
    {"host_name": "xbak1902", "ip": "10.4.86.101", "cluster": "JF20"},
    {"host_name": "xbak2002", "ip": "10.4.86.102", "cluster": "JF20"}
]
zgdb_str = r"aiop1/ng3,aiop@srv_zw3"


def get_monitor_cluster():
    """
    根据执行的主机名成，在__cluster列表中，获取集群名称。
    :return: string ,集群名称
    """
    hostname = socket.gethostname()
    for item in __cluster:
        if item["host_name"] == hostname:
            cluster = item["cluster"]
            break

    # 判断是否获取到集群名称
    if "cluster" in locals().keys():
        return cluster
    else:
        logger.error("Get cluster name error , host : {0} not in list"
                     .format(hostname))
        raise GetClusterError


def get_monitor_dirs():
    """
    从oracle配置表中，获取此集群的监控目录
    :return:
    """

    cluster = get_monitor_cluster()

    query_dir_sql = ("select monitor_dir from monitor_file_amount_change where "
                     "cluster = '{0}'".format(cluster))
    zgdb_conn = cx_Oracle.connect(zgdb_str)
    dirs = sql_select(query_dir_sql, zgdb_conn)[0]
    zgdb_conn.close()
    return dirs
