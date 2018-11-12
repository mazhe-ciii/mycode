#!/usr/bin/env python3
# -*-coding:utf-8-*-
"""Copyright: Copyright (c) 2018 Asiainfo
    @Description: ***纳税人资质表zg.cm_taxpayer_info修复方法***
    @fileName: sync_data.py
    @version: v1.0.0
    @author:  mazhe
    @created on: 2018/10/24 11:24 AM 17:29
纳税人资质修复info表需求说明：
1、生产表 zg.cm_taxpayer_info 需要备份
2、日志中显示各种类型数据数量

生成结果表名称aidemp.nsr_info_yyyymmdd_end(crm库)
针对A单边情况：
检验crm库中存在的A单边,将依然存在的数据拿到boss库，生成结果taxpayer_crm_d
select * from party.cm_taxpayer_info a where exists
(select 1 from aidemp.nsr_info_yyyymmdd_end where a.tax_id=tax_id and type='A')
将A单边数据插入boss库中
insert into zg.cm_taxpayer_info select * from taxpayer_crm_d  a where not
exists (select 1 from zg.cm_taxpayer_info where a.tax_id=tax_id)

针对B单边情况：
在crm执行，删除在crm中的已有数据,将剩余结果拿回boss库中执行,生成结果taxpayer_boss_d
select * from nsr_info_yyyymmdd_end a where not exists
(select 1 from party.cm_taxpayer_info where a.tax_id=tax_id) and a.type = 'B'
删除boss表对应的单边数据，执行下面SQL
delete from zg.cm_taxpayer_info a where
exists(select 1 from taxpayer_boss_d where a.tax_id=tax_id)

针对D单边情况：
修复boss，crm不一致数据，将结果集拿回crm侧查询全字段，执行下面sql，
得到结果taxpayer_crm_boss_d
select a.* from party.cm_taxpayer_info a,nsr_info_yyyymmdd_end b
where a.tax_id=b.tax_id and b.type='D';
修改boss侧数据。将state改成crm一致
update zg.cm_taxpayer_info a
set
state=(select state from taxpayer_crm_boss_d where a.tax_id=tax_id )
where
exists
(select 1 from taxpayer_crm_boss_d where a.tax_id=tax_id and a.state <> state)
修改boss侧数据，将tax_work改成和crm一致、
update zg.cm_taxpayer_info a
set
tax_work=(select tax_work from taxpayer_crm_boss_d where a.tax_id=tax_id )
where
exists(select 1 from taxpayer_crm_boss_d where a.tax_id=tax_id and
a.tax_work <> tax_work)
"""


from datetime import datetime
import cx_Oracle
from log import logger
import sql_list


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


def check_tmp_table():
    """
    A单边数据临时表：taxpayer_crm_d，B单边数据临时表：taxpayer_boss_d,都在账务库
    如果表存在，truncate临时表，如果表不存在，则依照zg.cm_taxpayer_info新建
    """
    table_list = ["taxpayer_crm_d", "taxpayer_boss_d", "taxpayer_crm_boss_d"]
    zgdb_conn = cx_Oracle.connect(dbstr_boss)
    for table in table_list:
        check_sql = ("select count(1) from dba_tables where table_name = "
                     "upper('{}') and owner = 'NG3UPD'").format(table)
        result = sql_select(check_sql, zgdb_conn)
        count = int(result[0][0])
        logger.debug("Tmp table {} count:{}".format(table, count))
        if count == 1:
            # 删除掉临时表，防止表结构变化
            drop_sql = "drop table {}".format(table)
            logger.info("删除已经存在的临时表: {},exec sql : {}"
                        .format(table, drop_sql))
            sql_dml(drop_sql, zgdb_conn)

        # 新建临时表
        if table == "taxpayer_boss_d":
            create_sql = ("create table {} tablespace zg_acct as select "
                          "tax_id,state,tax_work from zg.cm_taxpayer_info "
                          "where 1 = 2").format(table)
        else:
            create_sql = ("create table {} tablespace zg_acct as "
                          "select * from zg.cm_taxpayer_info "
                          "where 1 = 2").format(table)
        logger.info("新建临时表{},exec sql:{}".format(table, create_sql))
        sql_dml(create_sql, zgdb_conn)
        alter_sql = ("alter table {} add (operation varchar2(100))".format(table))
        logger.info("增加标识字段 is_sync")
        sql_dml(alter_sql, zgdb_conn)
    zgdb_conn.close()


def backup_table_taxpayer(src_table_name):
    """
    :param src_table_name : 备份源表名称
    :return : 无
    """
    backup_name = src_table_name + today
    zgdb_conn = cx_Oracle.connect(dbstr_boss)
    check_sql = ("select count(1) from dba_tables where "
                 "table_name=upper('{}') and owner='NG3UPD'").format(backup_name)
    result = sql_select(check_sql, zgdb_conn)
    count = int(result[0][0])
    logger.debug("Backup table count : {}".format(count))
    if count == 1:
        # 备份表存在，则删除
        drop_sql = "drop table {}".format(backup_name)
        logger.info("备份表 {} 存在，删除表,exec sql : {}"
                    .format(backup_name, drop_sql))
        sql_dml(drop_sql, zgdb_conn)
    # create new table
    create_sql = ("create table {} as select * from zg.cm_taxpayer_"
                  "info").format(backup_name)
    logger.info("备份表 {} ,exec sql: {}".format(src_table_name, create_sql))
    try:
        sql_dml(create_sql, zgdb_conn)
    except Exception as errmsg:
        logger.error("Create table error,error massage:" + str(errmsg))
    zgdb_conn.close()


def add_column(src_list):
    """
    给orcle查询的结果集list中每行记录最后追加一个空字段
    :param src_list: 需要追加空字段的oracle结果list
    :return:
    """
    result_list = []
    length = len(src_list)
    for i in range(length):
        tmp_list = list(src_list[i])
        tmp_list.append("")
        result_list.append(tuple(tmp_list))
        del tmp_list

    return result_list


def sync_data_crm_d():
    """
    同步A单边数据处理函数：
    1、从结果表 nsr_INFO_yyyymmd_end 中获取A单边数据，同步至账务库taxpayer_crm_d表
    2、根据taxpayer_crm_d表中数据，同步BOSS侧缺失数据
    :return:
    """
    # 获取A单边数据
    crmdb_conn = cx_Oracle.connect(dbstr_crm)
    logger.info("从CRM库获取A单边数据, exec sql : {}".format(sql_list.get_crm_d))
    result_a = sql_select(sql_list.get_crm_d, crmdb_conn)
    crmdb_conn.close()
    line_count = len(result_a)
    # 开始向BOSS账务库同步不一致数据
    # 同步A单边数据

    if line_count > 0:
        # 增加result_a 中，每行数据最后增加一个空字段
        result_a = add_column(result_a)
        # print(result_a)
        zgdb_conn = cx_Oracle.connect(dbstr_boss)
        logger.info("本次共获取A单边数据共{}条".format(line_count))
        many_insert("taxpayer_crm_d", result_a, zgdb_conn)
        logger.info("标记需要插入BOSS表的A单边数据,exec sql : {}"
                    .format(sql_list.mark_crm_d))
        sql_dml(sql_list.mark_crm_d, zgdb_conn)
        # 获取标记的需要同步的数据量
        mark_result = sql_select(sql_list.get_crm_d_count, zgdb_conn)
        mark_count = len(mark_result)
        if mark_count > 0:
            logger.info("本次需要插入BOSS表的A单边数据共 {} 条,tax_id： {}"
                        .format(mark_count, str(mark_result)))
            sql_dml(sql_list.sync_crm_d, zgdb_conn)
        else:
            logger.info("本次没有需要插入BOSS表的A单边数据")
        logger.info("A单边数据同步完成")
        zgdb_conn.close()
    else:
        logger.info("本次没有需要同步的A单边数据")
        mark_count = 0

    return mark_count


def sync_data_boss_d():
    """
    同步B单边数据处理函数：
    1、从结果表 NSR_INFO_yyyymmd_end中获取B单边数据，同步至账务库taxpayer_boss_d表
    2、根据 taxpayer_boss_d 表中数据，删除BOSS侧多余数据
    :return:
    """
    # 获取B单边数据
    crmdb_conn = cx_Oracle.connect(dbstr_crm)
    logger.info("从CRM库获取B单边数据, exec sql : {}".format(sql_list.get_boss_d))
    result_b = sql_select(sql_list.get_boss_d, crmdb_conn)
    line_count = len(result_b)
    crmdb_conn.close()

    # 开始向BOSS账务库同步不一致数据
    # 同步B单边数据
    if line_count > 0:
        # 增加result_b 中，每行数据最后增加一个空字段
        result_b = add_column(result_b)
        # print(result_b)
        zgdb_conn = cx_Oracle.connect(dbstr_boss)
        logger.info("本次共获取B单边数据共，共{}条".format(line_count))
        many_insert("taxpayer_boss_d", result_b, zgdb_conn)
        logger.info("标记需要删除BOSS表的B单边数据,exec sql : {}"
                    .format(sql_list.mark_crm_d))
        sql_dml(sql_list.mark_boss_d, zgdb_conn)
        # 标记需要删除的B单边数据
        mark_result = sql_select(sql_list.get_boss_d_count, zgdb_conn)
        mark_count = len(mark_result)
        if mark_count > 0:
            logger.info("本次需要删除BOSS表的B单边数据共 {} 条,tax_id： {}"
                        .format(mark_count, str(mark_result)))
            sql_dml(sql_list.sync_boss_d, zgdb_conn)
        else:
            logger.info("本次没有需要从BOSS表删除的B单边数据")
        logger.info("B单边数据同步完成")
        zgdb_conn.close()
    else:
        logger.info("本次没有需要删除的B单边数据")
        mark_count = 0

    return mark_count


def sync_data_crm_boss_d():
    """
    同步B单边数据处理函数：
    1、从结果表 NSR_INFO_yyyymmd_end中获取BOSS侧与CRM侧状态不一致的数据，同步至
       taxpayer_crm_boss_d 表
    2、根据 taxpayer_crm_boss_d 表中数据，同步BOSS侧与CRM侧状态不一致的数据
    :return:
    """
    crmdb_conn = cx_Oracle.connect(dbstr_crm)
    logger.info("从CRM库获取状态不一致数据, exec sql : {}"
                .format(sql_list.get_crm_boss_d))
    result_d = sql_select(sql_list.get_crm_boss_d, crmdb_conn)
    line_count = len(result_d)

    # 开始向BOSS账务库同步不一致数据
    # 同步状态不一致数据
    if line_count > 0:
        # 增加result_d 中，每行数据最后增加一个空字段
        result_d = add_column(result_d)
        # print(result_d)
        logger.info("本次共从获取状态不一致数据共 {} 条".format(line_count))
        zgdb_conn = cx_Oracle.connect(dbstr_boss)
        many_insert("taxpayer_crm_boss_d", result_d, zgdb_conn)
        logger.info("标记需要更新的state不一致数据,exec sql : {}"
                    .format(sql_list.mark_update_state))
        sql_dml(sql_list.mark_update_state, zgdb_conn)
        logger.info("标记需要更新的tax_work不一致数据,exec sql : {}"
                    .format(sql_list.mark_update_taxwork))
        sql_dml(sql_list.mark_update_taxwork, zgdb_conn)

        mark_state_result = sql_select(sql_list.get_update_state_count,
                                       zgdb_conn)
        mark_state_count = len(mark_state_result)
        mark_taxwork_result = sql_select(sql_list.get_update_taxwork_count,
                                         zgdb_conn)
        mark_taxwork_count = len(mark_taxwork_result)
        if mark_state_count > 0:
            logger.info("本次需要更新state字段的数据共 {} 条,tax_id : {}"
                        .format(mark_state_count, str(mark_state_result)))
            sql_dml(sql_list.sync_state, zgdb_conn)
        elif mark_taxwork_count > 0:
            logger.info("本次需要更新tax_work字段数据共 {} 条,tax_id : {}"
                        .format(mark_taxwork_count, str(mark_taxwork_result)))
            sql_dml(sql_list.sync_taxwork, zgdb_conn)
        else:
            logger.info("本次没有需要同步state和tax_work字段的数据")
        zgdb_conn.close()
    else:
        logger.info("本次没有需要同步状态不一致数据")
        mark_state_count = 0
        mark_taxwork_count = 0
    return mark_state_count, mark_taxwork_count


def main():
    logger.info("[数据一致性] 开始同步BOSS库cm_taxpayer_info表数据")
    check_tmp_table()  # 检查临时表
    backup_table_taxpayer("cm_taxpayer_info")  # 备份BOSS生产表

    crm_d_count = sync_data_crm_d()
    boss_d_count = sync_data_boss_d()
    state_count, taxwork_count = sync_data_crm_boss_d()

    logger.info("CRM单边：{}条,BOSS单边：{}条,state不一致：{}条,taxwork不一致:{}条"
                .format(crm_d_count, boss_d_count, state_count, taxwork_count))
