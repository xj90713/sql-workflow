package com.xiaoxj.sqlworkflow.service;

import io.github.reata.sqllineage4j.common.model.Table;
import io.github.reata.sqllineage4j.core.LineageRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SqllineTests {
    public static void main(String[] args) {
        String sqlContent = """
                #!/bin/bash
                #客户动态-视频回放
                source /data/apps/SparkRuntimeTempDir/readmqconfig.sh source4
                source /data/apps/pushSomeInfo/bin/impala2mq.sh
                exchangeName=customer-event
                routingKey=azt-event-key
                api=azt-event-queue
                isFilter=0
                sql="
                select customer_id                            as accountId_out
                     , 1                                      as journeyType_out
                     , 1                                      as businessType_out
                     , '客户动态-视频回放'                    as title_out
                     , concat_ws('_', circle_name, cast(circle_id as string)) as `圈子名称`
                     , video_title                            as `视频标题`
                     , last_review_time                       as `当天观看最晚时间`
                     , visitor_cnt                            as `当天观看次数`
                     , live_operator                          as `视频讲师`
                     , live_time                              as `视频直播时间`
                from cdm.dwd_scrm_customer_video_review_change_hd
                order by customer_id,last_review_time;
                "
                impala2mq 2
                """;
        String strings = extractSqlToString(sqlContent);
        LineageRunner runner = LineageRunner.builder(strings).build();
        System.out.println("sql:" + strings);
        List<Table> sources = runner.sourceTables();
        List<Table> targets = runner.targetTables();
        System.out.println( "sources:" + sources);
        System.out.println( "targets:" + targets);
    }

    /**
     * 从脚本中提取所有被 sql="..." 包裹的内容
     * @param content 脚本全文
     * @return 提取出的 SQL 列表
     */
    public static List<String> extractSql(String content) {
        List<String> sqlList = new ArrayList<>();

        String regex = "sql\\s*=\\s*\"(.*?)\"";
        Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
        Matcher matcher = pattern.matcher(content);

        while (matcher.find()) {
            // 获取第一个括号内的匹配内容
            String rawSql = matcher.group(1);
            if (rawSql != null) {
                sqlList.add(rawSql.trim());
            }
        }
        return sqlList;
    }
    /**
     * 提取脚本中的 SQL 并合并为一个带分号换行的字符串
     */
    public static String extractSqlToString(String content) {
        List<String> sqlList = new ArrayList<>();

        // 匹配 sql="..." 格式，(?s) 相当于 Pattern.DOTALL
        String regex = "sql\\s*=\\s*\"(.*?)\"";
        Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
        Matcher matcher = pattern.matcher(content);

        while (matcher.find()) {
            String sql = matcher.group(1).trim();
            if (!sql.isEmpty()) {
                // 确保 SQL 以分号结尾（如果没有的话）
                if (!sql.endsWith(";")) {
                    sql += ";";
                }
                sqlList.add(sql);
            }
        }

        // 使用 Java 8 的流将 List 合并为 String，每个元素后接换行
        return sqlList.stream().collect(Collectors.joining("\n"));
    }
}
