#!/usr/bin/env python3
#-*-coding:utf-8-*-
"""Copyright: Copyright (c) 2018 Asiainfo
    @Description: ***纳税人资质contact表自动修复***
    @fileName: nashuiren.py
    @version: v1.0.0
    @author:  houlf3
    @created on: 2018/9/13 15:07
    """
import cx_Oracle
import os
import sys
import logging
from datetime import datetime

start_time = datetime.now()  # 获取开始时间
mouth_id = start_time.strftime('%Y%m%d')
if len(sys.argv) ==2:
    mouth_id = sys.argv[1]
##############参数设置开始#####################
logpath = '/hadoop/data1/aidemp/log/纳税人资质/%s/'%mouth_id # 日志路径
logname = '纳税人资质修复.%s.log' %(start_time.strftime('%Y%m%d'), )
##############参数设置结束######################

if not os.path.exists(logpath):
    try:
        os.system("mkdir -p %s" % logpath)
    except Exception as e:
        print(e)
logfile = os.path.join(logpath,logname)

# 定义日志输出
logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s %(module)s %(levelname)-8s: %(message)s')
file_handler = logging.FileHandler(logfile)
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

def sql_dml(sql, db):
    """
    @Function: sql_dml
    @Description: 数据增删改操作
    @param:sql 查询sql，db oracle连接
    @return：无
    """
    logger.info("执行语句：%s"%sql)
    cursor = db.cursor()
    cursor.execute(sql)
    db.commit()

def many_insert(table_name,values,db):
    logger.info("插入数据：表%s，条数：%s" %(table_name,len(values)))
    if len(values)>0:
        cursor = db.cursor()
        param = ''
        for i in range(0, len(values[0])):
            param = param + ':%s,'%(i+1)

        cursor.prepare('insert into %s values (%s)'%(table_name,param[0:-1]))
        cursor.executemany(None, values)
        db.commit()

def sql_select(sql, db):
    """
    @Function: sql_select
    @Description: 通过数据连接查询sql结果
    @param:sql 查询sql，db oracle连接
    @return：返回sql匹配所有结果集
    """
    logger.info("执行语句：%s" % sql)
    cursor = db.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    db.close()
    return result



logger.info("开始执行修复...")

dbstr_boss = r'ng3upd/boss4,dml@srv_zw1'  # boss用户数据库连接串
#dbstr_boss = r'aidemp/padi_5678@aidemp'  # boss用户数据库连接串
dbstr_crm = r"aidemp/padi_5678@yydb1" #crm用户

db_conn = cx_Oracle.connect(dbstr_boss)
cursor = db_conn.cursor()
cursor.execute('truncate table boss_d')
cursor.execute('truncate table crm_d')
cursor.close()
db_conn.commit()
logger.info("清理完成...")

logger.info("单边数据获取中...")
db_conn = cx_Oracle.connect(dbstr_crm)

cursor = db_conn.cursor()
cursor.execute('select a.* from cm_and_tax_contact a,NSR_CONTACT_%s_END b  where a.contact_id=to_char(b.contact_id) and a.tax_id=to_char(b.tax_id) and b.type=\'A\''%mouth_id)
result_A = cursor.fetchall()
cursor.execute('delete from NSR_CONTACT_%s_END a where exists (select 1 from cm_and_tax_contact b where to_char(a.contact_id)=to_char(b.contact_id) and to_char(a.tax_id)=to_char(b.tax_id) and a.type=\'B\')'%mouth_id)
db_conn.commit()
cursor.execute('select * from NSR_CONTACT_%s_END a where a.type=\'B\''%mouth_id)
result_B = cursor.fetchall()
cursor.execute('select a.* from cm_and_tax_contact a, NSR_CONTACT_%s_END b  where a.contact_id=to_char(b.contact_id) and a.tax_id=to_char(b.tax_id) and b.type=\'D\''%mouth_id)
result_D = cursor.fetchall()
cursor.close()
db_conn.close()
logger.info("CRM单边：%s条，BOSS单边：%s条，不一致：%s条。"%(len(result_A),len(result_B),len(result_D)))
if len(result_A) >0:
    db_conn = cx_Oracle.connect(dbstr_boss)
    many_insert('crm_d',result_A,db_conn)

    db_conn = cx_Oracle.connect(dbstr_boss)
    sql_dml('insert into cm_and_tax_contact select * from crm_d a where not exists(select  1 from cm_and_tax_contact  b where to_char(a.contact_id)=b.contact_id and to_char(a.tax_id)=b.tax_id and a.cm_service_id=b.cm_service_id)',db_conn)
    logger.info("A单边数据处理完成")
if len(result_B) >0:
    db_conn = cx_Oracle.connect(dbstr_boss)
    many_insert('boss_d',result_B,db_conn)

    db_conn = cx_Oracle.connect(dbstr_boss)
    sql_dml('delete from cm_and_tax_contact a where exists(select  1 from  boss_d  b where to_char(a.contact_id)=to_char(b.b_contact_id) and to_char(a.tax_id)=to_char(b.b_tax_id) and a.cm_service_id=b.b_cm_service_id)',db_conn)
    logger.info("B单边数据处理完成")
if len(result_D) >0:
    db_conn = cx_Oracle.connect(dbstr_boss)
    sql_dml('truncate table crm_d',db_conn)

    db_conn = cx_Oracle.connect(dbstr_boss)
    many_insert('crm_d',result_D,db_conn)

    db_conn = cx_Oracle.connect(dbstr_boss)
    sql_dml('update cm_and_tax_contact a set a.state=(select b.state from crm_d  b  where to_char(a.contact_id)=to_char(b.contact_id) and to_char(a.tax_id)=to_char(b.tax_id)) where exists(select 1 from crm_d b where a.contact_id=b.contact_id and a.tax_id=b.tax_id and a.state<>b.state)',db_conn)
    logger.info("不一致数据处理完成")

logger.info("执行完成.")


os.system("python3 /hadoop/data1/aidemp/scripts/python/sendMsg.py \"%s\" \"%s\""%("13501233302",
        "纳税人修复提醒\n数据处理完成，共处理CRM单边：%s条，BOSS单边：%s条，不一致：%s条。"%(len(result_A),len(result_B),len(result_D))))
