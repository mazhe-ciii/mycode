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
exists(select 1 from taxpayer_crm_boss_d where a.tax_id=tax_id and a.state <> state)
修改boss侧数据，将tax_work改成和crm一致、
update zg.cm_taxpayer_info a 
set 
tax_work=(select tax_work from taxpayer_crm_boss_d where a.tax_id=tax_id )
where 
exists(select 1 from taxpayer_crm_boss_d where a.tax_id=tax_id and
a.tax_work <> tax_work)