#!/usr/bin/env python3
# -*-coding:utf-8-*-
"""Copyright: Copyright (c) 2018 Asiainfo
    @Description: ***纳税人资质表zg.cm_taxpayer_info修复方法***
    @fileName: sync_data.py
    @version: v1.0.0
    @author:  mazhe
    @created on: 2018/10/24 11:24 AM 17:29
生成结果表名称 aidemp.NSR_info_${day_id}_end(crm库)
针对A单边情况：
检验crm库中存在的A单边,将依然存在的数据拿到boss库，生成结果crm_d
select * from party.cm_taxpayer_info a where  exists
(select 1 from NSR_info_${day_id}_end where a.tax_id=tax_id and type='A');
下面sql将A单边数据插入boss库中
insert into zg.cm_taxpayer_info select * from crm_d  a where not exists
(select 1 from zg.cm_taxpayer_info where a.tax_Id=tax_id);

针对B单边情况：
在crm执行，删除在crm中的已有数据,将剩余结果拿回boss库中执行,生成结果 boss_d
select * from NSR_info_${day_id}_end a where not exists
(select 1 from party.cm_taxpayer_info where a.tax_id=tax_id) and a.type = 'B';
删除boss表对应的单边数据，执行下面SQL
delete from zg.cm_taxpayer_info a where
exists( select 1 from aidemp.boss_d where a.tax_id=tax_id );

针对D单边情况：
修复boss，crm不一致数据，将结果集拿回crm侧查询全字段，执行下面sql，得到结果集crm_boss_d
select a.* from party.cm_taxpayer_info a, NSR_info_${day_id}_end b where
a.tax_id=b.tax_id and b.type='D';
修改boss侧数据。将state改成crm一致
update zg.cm_taxpayer_info a set
state=(select state from crm_boss_d where a.tax_id=tax_id )
where exists
(select 1 from crm_boss_d where a.tax_id=tax_id and a.state <> state);
修改boss侧数据，将tax_work改成和crm一致、
update zg.cm_taxpayer_info a set
tax_work=(select tax_work from crm_boss_d where a.tax_id=tax_id )
where exists
(select 1 from crm_boss_d where a.tax_id=tax_id and  a.tax_work <> tax_work );
"""


from datetime import datetime
import cx_Oracle
from log import logger


now = datetime.now()
today = now.strftime("%Y%m%d")
dbstr_boss = r"ng3upd/boss4,dml@srv_zw2"   # boss用户数据库连接串
dbstr_crm = r"aidemp/padi_5678@yydb1"   # crm用户


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
    effect_num = cursor.getarraydmlrowcounts()
    db.commit()
    cursor.close()
    return effect_num


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


def check_tmp_table():
    """
    A单边数据临时表：taxpayer_crm_d，B单边数据临时表：taxpayer_boss_d,都在账务库
    如果表存在，truncate临时表，如果表不存在，则依照zg.cm_taxpayer_info新建
    """
    table_list = ["taxpayer_crm_d", "taxpayer_boss_d", "taxpayer_crm_boss_d"]
    zgdb_conn = cx_Oracle.connect(dbstr_boss)
    for table in table_list:
        check_sql = ("select count(1) from dba_tables where "
                     "table_name = '{}'".format(table))
        result = sql_select(check_sql, zgdb_conn)
        count = int(result[0][0])
        if count == 1:
            # truncate table
            truncate_sql = "truncate table {}".format(table)
            logger.info("Table {} is exist,truncate table,exec sql : {}"
                        .format(table, truncate_sql))
            sql_dml(truncate_sql, zgdb_conn)
        else:
            # create new table
            create_sql = ("create table {} as select * from zg.cm_taxpayer_info"
                          " where 1 = 2").format(table)
            logger.info("Table {} is not exist,create it,exec sql:{}"
                        .format(table, create_sql))
            sql_dml(create_sql, zgdb_conn)
    zgdb_conn.close()


def backup_table_taxpayer():
    """
    备份zg.cm_taxpayer_info 
    """
    backup_name = "cm_taxpayer_info_" + today
    zgdb_conn = cx_Oracle.connect(dbstr_boss)
    check_sql = ("select count(1) from dba_tables where "
                 "table_name=upper'{}' and owner='NG3UPD'".format(backup_name))
    result = sql_select(check_sql, zgdb_conn)
    count = int(result[0][0])
    if count == 1:
        # truncate table
        drop_sql = "drop table {}".format(backup_name)
        logger.info("Table {} is exist,drop table,exec sql : {}"
                    .format(backup_name, drop_sql))
        sql_dml(drop_sql, zgdb_conn)
    # create new table
    create_sql = ("create table {} as select * from zg.cm_taxpayer_"
                  "info").format(backup_name)
    logger.info("Backup table zg.cm_taxpayer_info,exec sql: {}"
                .format(create_sql))
    try:
        sql_dml(create_sql, zgdb_conn)
    except Exception as errmsg:
        logger.error("Create table error,error massage:" + str(errmsg))
    zgdb_conn.close()


# 定义一组获取从 aidemp.nsr_info_yyyymmdd_end 获取异常数据，并且同步异常数据sql
# get_crm_d,sync_crm_d 为获取A单边数据，并同步A单边缺失数据至BOSS账务库sql
# get_boss_d,sync_boss_d 为获取B单边数据，并在BOSS库删除单边数据sql
# get_crm_boss_d,sync_state,sync_state 为获取BOSS侧与CRM侧数据状态不一致数据，并将
# BOSS侧数据按照CRM状态同步sql
get_crm_d = ("select * from party.cm_taxpayer_info a where exists"
             "(select 1 from aidemp.nsr_info_{}_end where a.tax_id=tax_id  and"
             " type='A')").format(today)
get_boss_d = ("select * from aidemp.nsr_info_{}_end a where not exists"
              "(select 1 from party.cm_taxpayer_info where a.tax_id=to_char(tax_id))"
              " and a.type = 'B'").format(today)
get_crm_boss_d = ("select a.* from party.cm_taxpayer_info a,"
                  "aidemp.nsr_info_{}_end b where to_char(a.tax_id)=b.tax_id and "
                  "b.type='D'").format(today)
# sync_crm_d = ("insert into zg.cm_taxpayer_info select * from "
#               "taxpayer_crm_d a where not exists(select 1 from "
#               "zg.cm_taxpayer_info where a.tax_id=tax_id)")
# sync_boss_d = ("delete from zg.cm_taxpayer_info a where exists"
#                "( select 1 from aidemp.taxpayer_boss_d where a.tax_id=tax_id )")
# sync_state = ("update zg.cm_taxpayer_info a set state=(select state from "
#               "taxpayer_crm_boss_d where a.tax_id=tax_id) where exists"
#               "(select 1 from taxpayer_crm_boss_d where a.tax_id=tax_id and "
#               "a.state <> state)")
# sync_taxwork = ("update zg.cm_taxpayer_info a set tax_work=(select tax_work "
#                 "from taxpayer_crm_boss_d where a.tax_id=tax_id) where exists"
#                 "(select 1 from taxpayer_crm_boss_d where a.tax_id=tax_id and  "
#                 "a.tax_work <> tax_work )")
# for test
sync_crm_d = ("insert into zg.cm_taxpayer_info_mztest select * from "
              "taxpayer_crm_d a where not exists(select 1 from "
              "zg.cm_taxpayer_info where a.tax_id=tax_id)")
sync_boss_d = ("delete from zg.cm_taxpayer_info_mztest a where exists"
               "( select 1 from aidemp.taxpayer_boss_d where a.tax_id=tax_id )")
sync_state = ("update zg.cm_taxpayer_info_mztest a set state=(select state from "
              "taxpayer_crm_boss_d where a.tax_id=tax_id) where exists(select 1 from "
              "taxpayer_crm_boss_d where a.tax_id=tax_id and a.state <> state)")
sync_taxwork = ('update zg.cm_taxpayer_info_mztest a set tax_work=(select '
                'tax_work from taxpayer_crm_boss_d where a.tax_id=tax_id) where '
                'exists(select 1 from taxpayer_crm_boss_d where a.tax_id=tax_id '
                'and  a.tax_work <> tax_work )')


def sync_data_crm_d():
    """
    同步A单边数据处理函数：
    1、从结果表 nsr_INFO_yyyymmd_end 中获取A单边数据，同步至账务库taxpayer_crm_d表
    2、根据taxpayer_crm_d表中数据，同步BOSS侧缺失数据
    :return:
    """
    # 获取A单边数据
    crmdb_conn = cx_Oracle.connect(dbstr_crm)
    result_a = sql_select(get_crm_d, crmdb_conn)
    crmdb_conn.close()
    line_count = len(result_a)
    # 开始向BOSS账务库同步不一致数据
    # 同步A单边数据

    if line_count > 0:
        zgdb_conn = cx_Oracle.connect(dbstr_boss)
        logger.info("开始同步A单边数据，共{}条".format(line_count))
        many_insert("taxpayer_crm_d", result_a, zgdb_conn)
        sql_dml(sync_crm_d, zgdb_conn)
        logger.info("A单边数据同步完成")
        zgdb_conn.close()
    else:
        logger.info("本次没有需要同步的A单边数据")
    return line_count


def sync_data_boss_d():
    """
    同步B单边数据处理函数：
    1、从结果表 NSR_INFO_yyyymmd_end中获取B单边数据，同步至账务库taxpayer_boss_d表
    2、根据 taxpayer_boss_d 表中数据，删除BOSS侧多余数据
    :return:
    """
    # 获取B单边数据
    crmdb_conn = cx_Oracle.connect(dbstr_crm)
    result_b = sql_select(get_boss_d, crmdb_conn)
    line_count = len(result_b)
    crmdb_conn.close()
    # 开始向BOSS账务库同步不一致数据
    # 同步B单边数据

    if line_count > 0:
        zgdb_conn = cx_Oracle.connect(dbstr_boss)
        logger.info("开始同步B单边数据，共{}条".format(line_count))
        many_insert("taxpayer_boss_d", result_b, zgdb_conn)
        sql_dml(sync_boss_d, zgdb_conn)
        logger.info("B单边数据同步完成")
        zgdb_conn.close()
    else:
        logger.info("本次没有需要删除的B单边数据")

    return line_count


def sync_data_crm_boss_d():
    """
    同步B单边数据处理函数：
    1、从结果表 NSR_INFO_yyyymmd_end中获取BOSS侧与CRM侧状态不一致的数据，同步至
       taxpayer_crm_boss_d 表
    2、根据 taxpayer_crm_boss_d 表中数据，同步BOSS侧与CRM侧状态不一致的数据
    :return:
    """
    crmdb_conn = cx_Oracle.connect(dbstr_crm)
    result_d = sql_select(get_crm_boss_d, crmdb_conn)
    line_count = len(result_d)

    # 开始向BOSS账务库同步不一致数据
    # 同步状态不一致数据

    if line_count > 0:
        zgdb_conn = cx_Oracle.connect(dbstr_boss)
        logger.info("开始同步状态不一致数据，共{}条".format(line_count))
        many_insert("taxpayer_crm_boss_d", zgdb_conn)
        sql_dml(sync_state, zgdb_conn)
        sql_dml(sync_taxwork, zgdb_conn)
        logger.info("状态不一致数据同步完成")
        zgdb_conn.close()
    else:
        logger.info("本次没有需要同步状态不知数据")
    return line_count


def main():
    logger.info("Start sync cm_taxpayer_info")
    check_tmp_table()  # 检查临时表
    # backup_table_taxpayer()  # 备份BOSS生产表

    crm_d_count = sync_data_crm_d()
    boss_d_count = sync_data_boss_d()
    crm_boss_d_count = sync_data_crm_boss_d()

    logger.info("CRM单边： %s 条，BOSS单边： %s 条，不一致： %s 条。"
                .format(crm_d_count, boss_d_count, crm_boss_d_count))
