package com.xiaoxj.sqlworkflow;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;

public class SqlParserExample {
    public static void main(String[] args) throws Exception {
        String sql = "SELECT id as id, name FROM users WHERE age > 20";
        Statement statement = CCJSqlParserUtil.parse(sql);

        if (statement instanceof Select select) {
            //            System.out.println("解析的SQL: " + select.getSelectBody().get);
            System.out.println("解析的表名: " +
                    ((net.sf.jsqlparser.statement.select.PlainSelect)select.getSelectBody()).getFromItem());
        }
    }
}