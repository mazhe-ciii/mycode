#!/usr/bin/env python3
# -*-coding:utf-8-*-
"""Copyright: Copyright (c) 2018 Asiainfo
    @Description: ***sql配置文件，执行程序中需要的sql***
    @fileName: sql_list.py
    @version: v1.0.0
    @author:  mazhe
    @created on: 2018/11/7 10:39 AM 17:29
"""
from datetime import datetime

now = datetime.now()
today = now.strftime("%Y%m%d")
# 定义一组获取从 aidemp.nsr_info_yyyymmdd_end 获取异常数据，并且同步异常数据sql
# get_crm_d,sync_crm_d 为获取A单边数据，并同步A单边缺失数据至BOSS账务库sql
# get_boss_d,sync_boss_d 为获取B单边数据，并在BOSS库删除单边数据sql
# get_crm_boss_d,sync_state,sync_state 为获取BOSS侧与CRM侧数据状态不一致数据，并将
# BOSS侧数据按照CRM状态同步sql

# 生产表
dest_table="zg.cm_taxpayer_info"

get_crm_d = ("select * from party.cm_taxpayer_info a where exists"
             "(select 1 from aidemp.nsr_info_{}_end where a.tax_id=tax_id  and"
             " type='A') and a.tax_id != 88888888").format(today)
get_boss_d = ("select to_number(a.b_tax_id),a.b_state,to_number(a.b_tax_work)"
              " from aidemp.nsr_info_{}_end a where not exists"
              "(select 1 from party.cm_taxpayer_info where a.tax_id="
              "to_char(tax_id)) and a.type = 'B' and a.b_tax_id != '88888888'"
              .format(today))
get_crm_boss_d = ("select a.* from party.cm_taxpayer_info a,"
                  "aidemp.nsr_info_{}_end b where to_char(a.tax_id)=b.tax_id "
                  "and b.type='D' and a.tax_id != 88888888").format(today)

# 更新临时表operation字段，标记操作类型
mark_crm_d = ("update taxpayer_crm_d a set a.operation = 'insert' where not "
              "exists(select 1 from {} where a.tax_id=tax_id)"
              .format(dest_table))
mark_boss_d = ("update taxpayer_boss_d a set a.operation = 'delete' where "
               "exists (select 1 from {} where a.tax_id=tax_id)"
               .format(dest_table))
mark_update_state = ("update taxpayer_crm_boss_d a set a.operation = "
                     "'update state' where exists (select 1 from {} where "
                     "a.tax_id=tax_id and a.state <> state)".format(dest_table))
mark_update_taxwork = ("update taxpayer_crm_boss_d a set a.operation = "
                       "'update tax_work' where exists (select 1 from {} where "
                       "a.tax_id=tax_id and a.tax_work <> tax_work)"
                       .format(dest_table))

# 获取3个临时表中需要同步数据的量
get_crm_d_count = "select tax_id from taxpayer_crm_d where operation = 'insert'"
get_boss_d_count = ("select tax_id from taxpayer_boss_d where "
                    "operation = 'delete'")
get_update_state_count = ("select tax_id from taxpayer_crm_boss_d where "
                          "operation='update state'")
get_update_taxwork_count = ("select tax_id from taxpayer_crm_boss_d where "
                            "operation='update tax_work'")


# zg.cm_taxpayer_info 表字段
table_field = ("TAX_ID,BUSI_LICENSE_NUM,NATIONAL_TAX_NUM,ENTERPRISE_NAME,"
               "ADD_TAX_VIN_NUM,ADDRESS,CONTACT_PHONE,PAYMENT_ACCOUNT_NAME,"
               "PAYMENT_ACCOUNT,IS_INVOICE_NEED,INVOICE_TYPE,CONTACT_PERSON,"
               "PROPERTY,EXIT_PIC_TAXF_SEC,EXIT_PIC_TAXF,EXIT_PIC_APPLY_SEC,"
               "EXIT_PIC_ADD_TAX_SEC,EXIT_PIC_APPLY,EXIT_PIC_ADD_TAX,"
               "EXIT_PIC_LICENSE_SEC,EXIT_PIC_LICENSE,EXIT_PIC_OTH_SEC,"
               "EXIT_PIC_OTH,TAX_STATE,INTELL_EFFECTIVE_DATE,INTELL_EXPIRE"
               "_DATE,TAX_WORK,STATE,FIRST_CHECK_ID,SED_CHECK_GROUP,"
               "START_OP_ID,CREATE_OP_ID,CREATE_ORG_ID,OP_ID,ORG_ID,DONE_CODE,"
               "CREATE_DATE,DONE_DATE,EFFECTIVE_DATE,EXPIRE_DATE,REGION_ID,"
               "REMARKS,EXT1,EXT2,EXT3,EXT4,EXT5,EXT6,AGENT_CONTACT_PHONE,"
               "REMIND_PHONE,MOT_DATE,MET_EFFECTIVE_DATE,MET_EXPIRE_DATE,"
               "THIRD_PARTY_NAME,EXIT_THIRD_PARTY_SEQ,EXIT_THIRD_PARTY,"
               "CONTACT_TAX_ID,PIC_ADDRESS,PIC_ADDRESS_SEQ,PIC_BANK,"
               "PIC_BANK_SEQ")

# 同步临时表数据到正式表

sync_crm_d = ("insert into {} select {} from taxpayer_crm_d a where "
              "a.operation='insert'".format(dest_table, table_field))
sync_boss_d = ("delete from {} a where exists"
               "(select 1 from taxpayer_boss_d where a.tax_id=tax_id and "
               "operation='delete')".format(dest_table))
sync_state = ("update {} a set state=(select state from taxpayer_crm_boss_d "
              "where a.tax_id=tax_id and operation='update state') where exists "
              "(select 1 from taxpayer_crm_boss_d where a.tax_id = tax_id)"
              .format(dest_table))
sync_taxwork = ("update {} a set tax_work=(select tax_work from "
                "taxpayer_crm_boss_d where a.tax_id=tax_id and "
                "operation='update tax_work') where exists "
                "(select 1 from taxpayer_crm_boss_d where a.tax_id = tax_id)"
                .format(dest_table))

if __name__ == "__main__":
    print(get_crm_d)
    print(get_boss_d)
    print(get_crm_boss_d)
    print(mark_crm_d)
    print(mark_boss_d)
    print(mark_update_state)
    print(mark_update_taxwork)
    print(sync_crm_d)
    print(sync_boss_d)
    print(sync_taxwork)
    print(sync_state)
