package com.xiaoxj.sqlworkflow.service;

import io.github.reata.sqllineage4j.common.model.Table;
import io.github.reata.sqllineage4j.core.LineageRunner;

import java.util.List;

public class SqllineTests {
    public static void main(String[] args) {
        String sqlContent = """
                -- ########################################################
                -- 输出表：cdm.st_app_user_active_ds
                -- 输入表：pdw.fact_cnt_stock_login_log, cdm.dwd_gzg_log_app_heartbeat_di, cdm.dim_gzg_pub_date, cdm.t_product_privilege_detail_tmp
                -- 操作者：wangdongdong
                -- 创建时间：2021-05-20
                -- 作业方式：每日全量
                -- ########################################################
                -- 修改时间 2022-10-10 15:10
                with t1 as
                         (select p1.hp_stat_date
                               , p1.guid
                               , p1.is_privilege
                               , p2.open_number_later_2000
                               , p2.is_open
                               , case when p3.guid is not null then 1 else 0 end as is_new
                          from (select p1.hp_stat_date
                                     , p1.guid
                                     , max(case when p2.user_id is not null then 1 else 0 end) as is_privilege
                                from (select dt as hp_stat_date
                                           , guid
                                           , user_id
                                      from cdm.dws_gzg_log_app_heartbeat_stat_di
                                      where dt <= date_sub(${imp_pt_day}, 1)
                                        and dt >= date_sub(${imp_pt_day}, 90)
                                        and guid <> ''
                                      group by dt
                                             , guid
                                             , user_id
                                     ) p1
                                         left join
                                     (select hp_stat_date
                                           , user_id
                                      from cdm.dwd_gzg_trd_product_privilege_mid
                                      where hp_stat_date <= date_sub(${imp_pt_day}, 1)
                                        and hp_stat_date >= date_sub(${imp_pt_day}, 90)
                                      group by hp_stat_date
                                             , user_id
                                     ) p2
                                     on p1.hp_stat_date = p2.hp_stat_date
                                         and p1.user_id = p2.user_id
                                group by p1.hp_stat_date
                                       , p1.guid
                               ) p1
                                   left join
                               (select day_short_desc
                                     , is_open
                                     , open_number_later_2000
                                from cdm.dim_gzg_pub_date
                                where day_short_desc >= date_sub(${imp_pt_day}, 90)
                                  and day_short_desc <= date_sub(${imp_pt_day}, 1)
                               ) p2
                               on p1.hp_stat_date = p2.day_short_desc
                                   left join
                               (select dt as hp_stat_date
                                     , guid
                                from cdm.dws_gzg_usr_user_login_di
                                where dt <= date_sub(${imp_pt_day}, 1)
                                  and dt >= date_sub(${imp_pt_day}, 90)
                                  and guid <> ''
                                  and is_new = 1
                                group by dt
                                       , guid
                               ) p3
                               on p1.hp_stat_date = p3.hp_stat_date
                                   and p1.guid = p3.guid
                         )
                insert overwrite table cdm.dws_gzg_usr_user_active
                select p1.hp_stat_date
                     , p3.is_trade_day
                     , cast(p1.guid_cnt as int) as guid_cnt
                     , cast(p1.new_guid_cnt as int) as new_guid_cnt
                     , p1.start_times
                     , p1.total_online_time
                     , cast(p4.privilege_guid_cnt as int) as privilege_guid_cnt
                     , p2.remain_per
                     , p2.new_remain_per
                     , p2.privilege_remain_per
                     , p2.no_privilege_remain_per
                from (select dt as hp_stat_date
                           , count(distinct guid)                                                                          as guid_cnt
                           , count(distinct case when is_new = 1 then guid end)                                            as new_guid_cnt
                           , case
                                 when count(distinct guid) > 0
                                     then round(sum(start_times) / count(distinct guid), 2) end                            as start_times
                           , case
                                 when count(distinct guid) > 0
                                     then round(sum(total_online_time) / (count(distinct guid) * 60), 2) end               as total_online_time
                      from cdm.dws_gzg_usr_user_login_di
                      where dt <= date_sub(${imp_pt_day}, 1)
                        and dt >= date_sub(${imp_pt_day}, 90)
                        and guid <> ''
                      group by dt
                     ) p1
                         left join
                     (select p1.hp_stat_date
                           , p1.remain_per
                           , p1.new_remain_per
                           , p1.privilege_remain_per
                           , p1.no_privilege_remain_per
                      from (select p1.hp_stat_date
                                 , case when p1.guid_cnt > 0 then round(p1.last_guid_cnt / p1.guid_cnt, 4) end             as remain_per
                                 , case
                                       when p1.new_guid_cnt > 0
                                           then round(p1.last_new_guid_cnt / p1.new_guid_cnt, 4) end                       as new_remain_per
                                 , case
                                       when p1.privilege_guid_cnt > 0
                                           then round(p1.last_privilege_guid_cnt / p1.privilege_guid_cnt, 4) end           as privilege_remain_per
                                 , case
                                       when p1.no_privilege_guid_cnt > 0
                                           then round(p1.last_no_privilege_guid_cnt / p1.no_privilege_guid_cnt, 4) end     as no_privilege_remain_per
                                 , row_number() over (order by p1.hp_stat_date desc)                                       as num
                            from (select p1.hp_stat_date
                                       , count(distinct p1.guid)                                        as guid_cnt
                                       , count(distinct p2.guid)                                        as last_guid_cnt
                                       , count(distinct case when p1.is_new = 1 then p1.guid end)       as new_guid_cnt
                                       , count(distinct case when p1.is_new = 1 then p2.guid end)       as last_new_guid_cnt
                                       , count(distinct case when p1.is_privilege = 1 then p1.guid end) as privilege_guid_cnt
                                       , count(distinct case when p1.is_privilege = 1 then p2.guid end) as last_privilege_guid_cnt
                                       , count(distinct case when p1.is_privilege = 0 then p1.guid end) as no_privilege_guid_cnt
                                       , count(distinct case when p1.is_privilege = 0 then p2.guid end) as last_no_privilege_guid_cnt
                                  from (select hp_stat_date
                                             , guid
                                             , open_number_later_2000
                                             , is_privilege
                                             , is_new
                                        from t1
                                        where is_open = 1
                                       ) p1
                                           left join
                                       (select hp_stat_date
                                             , guid
                                             , open_number_later_2000
                                             , is_privilege
                                        from t1
                                        where is_open = 1
                                       ) p2
                                       on cast(p1.open_number_later_2000 as bigint) + 1 = cast(p2.open_number_later_2000 as bigint)
                                           and p1.guid = p2.guid
                                  group by p1.hp_stat_date
                                 ) p1
                           ) p1
                      where p1.num > 1
                     ) p2
                     on p1.hp_stat_date = p2.hp_stat_date
                         left join
                     (select day_short_desc
                           , case when is_open = 1 then '是' else '否' end as is_trade_day
                      from cdm.dim_gzg_pub_date
                      where day_short_desc >= date_sub(${imp_pt_day}, 90)
                        and day_short_desc <= date_sub(${imp_pt_day}, 1)
                     ) p3
                     on p1.hp_stat_date = p3.day_short_desc
                         left join
                     (select hp_stat_date
                           , count(distinct guid) as privilege_guid_cnt
                      from t1
                      where is_privilege = 1
                      group by hp_stat_date
                     ) p4
                     on p1.hp_stat_date = p4.hp_stat_date;
                """;
        String sqlContent1 = "insert into test values(1);";
        LineageRunner runner = LineageRunner.builder(sqlContent1).build();
        List<Table> sources = runner.sourceTables();
        List<Table> targets = runner.targetTables();
        System.out.println( "sources:" + sources);
        System.out.println( "targets:" + targets);
    }
}
