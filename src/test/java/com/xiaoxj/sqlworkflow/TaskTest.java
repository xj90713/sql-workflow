package com.xiaoxj.sqlworkflow;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TaskTest {
    public static void main(String[] args) {
        String sql = """
                ---hive---
                insert into test select * from
                test2
                join select * from test1；
                ---shell---
                echo "test" ;
                ---spark---
                spark submit;
                """;
        String sql1 = "insert into test select * from test_hive;";
        System.out.println("countTasks: " + taskTriples(sql));

    }
    public static List<Map<String, String>> taskTriples(String sql) {
        ArrayList<Integer> sepStarts = new ArrayList<>();
        ArrayList<Integer> sepEnds = new ArrayList<>();
        ArrayList<String> types = new ArrayList<>();
        Pattern p = Pattern.compile("(?m)^\\s*---\\s*([a-zA-Z0-9_]+)\\s*---\\s*$");
        Matcher m = p.matcher(sql);
        while (m.find()) {
            sepStarts.add(m.start());
            sepEnds.add(m.end());
            types.add(m.group(1));
        }
        List<Map<String, String>> res = new ArrayList<>();
        if (types.isEmpty()) {
            Map<String, String> t = new LinkedHashMap<>();
            t.put("task_name", "task_1");
            t.put("task_type", "default");
            t.put("task_content", sql.trim());
            res.add(t);
            return res;
        }
        for (int i = 0; i < types.size(); i++) {
            int contentStart = sepEnds.get(i);
            int contentEnd = (i + 1 < sepStarts.size()) ? sepStarts.get(i + 1) : sql.length();
            String content = sql.substring(contentStart, contentEnd).trim();
            Map<String, String> t = new LinkedHashMap<>();
            t.put("task_name", "task_" + (i + 1));
            t.put("task_type", types.get(i) == null ? "default" : types.get(i).toLowerCase());
            t.put("task_content", content);
            res.add(t);
        }
        return res;
    }
}
