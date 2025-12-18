----hz|token|crontab|告警内容
select
    distinct
    os.customer_cid
from
    hydrus_admin.order_statistic os
        join link_scrm.sys_customer_extension sce
             on os.customer_cid = sce.cid
where
    os.order_status = 2
  and os.create_time > '2025-01-01 00:00:00'
  and os.offline_order_id is not null
  and os.order_amount <= 300000
  and sce.\`10090\` in
      (
       10148,
       10147,
       10145,
       10144,
       10143,
       10142,
       10141,
       10140
          );