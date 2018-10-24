#!/usr/bin/env python3
# -*-coding:utf-8-*-
"""Copyright: Copyright (c) 2018 Asiainfo
    @Description: ***纳税人资质表zg.cm_taxpayer_info修复方法***
    @fileName: sql.py
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
提出crm生产表中存在的B单边数据，然后删除BOSS侧生产表对应tax_id的数据
select a.tax_id from NSR_info_${day_id}_end a where not exists
(select 1 from party.cm_taxpayer_info where a.tax_id=tax_id) and a.type = 'B';
删除boss表对应的单边数据，执行下面SQL
delete from zg.cm_taxpayer_info a where tax_id = :tax_id

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


now = datetime.now()
# A单边处理sql
get_crm_d = """select * from party.cm_taxpayer_info a where  exists
(select 1 from NSR_info_{}_end where a.tax_id=tax_id and type='A');
""".format(now.strftime('%Y%m%d'))

sync_crm_d = """insert into zg.cm_taxpayer_info select * from crm_d a where 
not exists(select 1 from zg.cm_taxpayer_info where a.tax_Id=tax_id);
"""
