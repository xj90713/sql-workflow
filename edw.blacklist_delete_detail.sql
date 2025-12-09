-- drop view edw.blacklist_delete_detail
create view edw.blacklist_delete_detail as
with sc as (
select distinct user_id,su.staff_code,sb.staff_name
from odshk.sys_user_leader_relation su
left join odsyy.staff_basic_info sb
on su.staff_code=sb.staff_code
where su.staff_code is not null and principal_id is not null)
select t0.*,if(LENGTH(follower_name)>0,'未删微','已删微') as delete_wx_status,
COALESCE(fbl.is_delete,'是') as is_delete,fbl.restore_reason,fbl.restore_datetime
-- ,if(t0.fill_datetime<=COALESCE(rph.create_time,'3000-01-01 00:00:00'),t0.fill_datetime,rph.create_time) as fill_datetime_new
,if(t0.fill_datetime>=COALESCE(rph.create_time,'1000-01-01 00:00:00'),t0.fill_datetime,rph.create_time) as fill_datetime_new
,rph.delwx_reason
from(
select 
scb.name,scb.cid,fb.user_number,scb.wx_nick,zy.unionid,fb.data_resource,fb.fill_datetime,
if(risk.risk_no is null,'否','是') as is_risk,risk.is_malice_protect,risk.is_out_complaint,risk.is_blacklist,risk.blacklist_reason,risk.is_refund,risk.risk_no,
max(zy.avatar) as avatar,
GROUP_CONCAT(distinct zy.follower_name) as follower_name,GROUP_CONCAT(distinct zy.follower_staff_code) as follower_staff_code,
GROUP_CONCAT(distinct zy.executive_name) as executive_name,
GROUP_CONCAT(distinct zy.majordomo_name) as majordomo_name,
GROUP_CONCAT(distinct (case when zy.principal_id=154 then '推广一区'
when zy.principal_id=155 then '推广二区'
when zy.principal_id=1308 then '创新业务部'
else '' end)) as region
from (
select user_number,min(fill_datetime) as fill_datetime,GROUP_CONCAT(data_resource) as data_resource
from (
select user_number,max(fill_datetime) as fill_datetime,'导入' as data_resource
from report.fill_blacklist_resource
where user_number is not null
group by 1
union ALL
select customer_num as user_number,'1000-01-01 00:00:00' as fill_datetime,'新增' as data_resource from odshk.sys_customer_base
where customer_num is not null and in_del_wx=1
)fb0
group by 1
)fb
left join (
select * from odshk.sys_customer_base
where customer_num is not null)scb
on scb.customer_num=fb.user_number 
left join(
select w.*,fsc.staff_code as follower_staff_code,scb.cid as cid0
from odshk.we_customer w
left join odshk.sys_customer_base scb
on w.unionid=scb.union_id and scb.union_id is not null
left join odshk.we_employee we
on w.add_user_id=we.we_user_id
left join sc psc
on w.principal_id=psc.user_id
left join sc msc
on w.majordomo_id=msc.user_id
left join sc esc
on w.executive_id=esc.user_id
left join sc fsc
on w.follower_id=fsc.user_id
where w.del_flag=0 and w.principal_id+w.majordomo_id+w.executive_id+w.follower_id>0
and we.del_flag=0 and we.use_status=1
)zy
on scb.cid=zy.cid0
left join(
select * from(
select ro.risk_no,ro.customer_num,rpp.is_malice_protect,rpp.is_out_complaint,rpp.is_blacklist,rpp.blacklist_reason,rpp.is_refund,rph.is_delwx,rph.delwx_reason,rpp.process_time,
ROW_NUMBER()OVER (PARTITION by ro.customer_num ORDER BY rpp.is_out_complaint desc,rph.is_delwx desc,rpp.process_time desc) as order_num
from (
select risk_no,customer_num from odsfk.risk_order
where customer_num is not null and length(customer_num)>0)ro
left join (
select risk_no
,if(max(malice_protect_power)=1,'是','否') as is_malice_protect
,if(max(is_out_complaint)=1,'是','否') as is_out_complaint
,if(max(is_blacklist)=1,'是','否') as is_blacklist
,GROUP_CONCAT(distinct blacklist_reason) as  blacklist_reason
,if(max(is_refund)=1,'是','否') as is_refund
,max(process_time) as process_time
from odsfk.risk_process_plan
where is_draft =0
group by 1)rpp
on ro.risk_no=rpp.risk_no
left join (
select risk_no,delwx_reason,is_delwx from (
select risk_no,delwx_reason,is_delwx,ROW_NUMBER()OVER (PARTITION by risk_no ORDER BY create_time desc) as order_num
from odsfk.risk_process_history
where is_delwx=1)rph0
where order_num=1)rph
on ro.risk_no=rph.risk_no
)risk0
where order_num=1)risk
on fb.user_number=risk.customer_num
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)t0
left join report.fill_blacklist_log fbl
on t0.cid=fbl.cid and t0.user_number=fbl.user_number
left join(
select * from (
select risk_no,create_time,
delwx_reason,
ROW_NUMBER()OVER (PARTITION by risk_no ORDER BY create_time desc) as risk_num
from odsfk.risk_process_history
where is_delwx=1
)rph0
where risk_num=1
)rph
on t0.risk_no=rph.risk_no