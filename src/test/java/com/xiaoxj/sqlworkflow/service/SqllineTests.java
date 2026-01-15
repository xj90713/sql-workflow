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
---doris---
CALL EXECUTE_STMT("c_hz_middle", "DELETE t1 FROM phpmanager.t_user_tags_test as t1 LEFT JOIN phpmanager.t_user_tags_tmp_test as t2 ON  t1.tag_id=t2.tag_id and t1.user_id=t2.user_id WHERE t2.tag_id IS NULL and t1.tag_id not rlike 'fz_';");
insert into c_hz_middle.phpmanager.t_user_tags_tmp_test select * from hive.db_tag.dwd_user_tag_to_interface_dd;
           """;
        System.out.println("extractSql:" + sqlContent);
        String strings = extractSqlToString(sqlContent);
        LineageRunner runner = LineageRunner.builder(sqlContent).build();
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
                sql = sql.replaceAll("\\\\", "");
                sqlList.add(sql);
            }
        }

        // 使用 Java 8 的流将 List 合并为 String，每个元素后接换行
        return sqlList.stream().collect(Collectors.joining("\n"));
    }
}
