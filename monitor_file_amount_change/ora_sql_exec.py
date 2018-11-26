#!/usr/bin/env python3
# -*-coding:utf-8-*-
"""Copyright: Copyright (c) 2018 Asiainfo
    @Description: ***oracle数据库的基本面操作***
    @fileName: ora_sql_exec.py
    @version: v1.0.0
    @author:  mazhe
    @created on: 2018/11/19 11:57 AM 17:29
"""


def sql_dml(sql, db):
    """
    @Function: sql_dml
    @Description: 数据增删改操作
    @param:sql 查询sql，db oracle连接
    @return：返回DML row counts
    """
    cursor = db.cursor()
    cursor.execute(sql)
    # 获取更改条数
    # effect_num = cursor.getarraydmlrowcounts()
    db.commit()
    cursor.close()
    # return effect_num


def many_insert(table_name, values, db):
    """
    :param table_name: 插入数据表
    :param values: 带插入数据列表，list
    :param db: 数据库连接
    :return: 无
    """
    if len(values) > 0:
        cursor = db.cursor()
        param = list()
        # 根据查询出数据的字段数，生成绑定变量字符串
        for i in range(0, len(values[0])):
            str_append = ":" + str(i + 1)
            param.append(str_append)
        bind_str = ",".join(param)
        sql = "insert into {0} values ({1})".format(table_name, bind_str)
        cursor.prepare(sql)
        cursor.executemany(None, values)
        db.commit()
        cursor.close()


def sql_select(sql, db):
    """
    @Function: sql_select
    @Description: 通过数据连接查询sql结果
    @param:sql 查询sql，db oracle连接
    @return：返回sql匹配所有结果集
    """
    cursor = db.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    return result
