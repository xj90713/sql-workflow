package com.xiaoxj.sqlworkflow.common.utils;

import com.xiaoxj.sqlworkflow.dolphinscheduler.task.TaskDefinition;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WorkflowDefineParam;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class TextUtils {
    public static List<String> parseFirstLine(String input) {
        if (input == null || input.isEmpty()) {
            return new ArrayList<>();
        }

        // 1. 获取第一行内容

        String firstLine = input.replace("--","").split("\n")[0];

        // 2. 根据 "|" 分隔符拆分，并对每个元素进行去空格处理
        return Arrays.stream(firstLine.split("\\|"))
                .map(String::trim)
                .collect(Collectors.toList());
    }

    /**
     * 提取字符串中花括号 {} 内的内容
     * @param text 输入字符串
     * @return 花括号内的内容列表
     */
    public static List<String> extractFromBraces(String text) {
        List<String> result = new ArrayList<>();

        if (text == null || text.isEmpty()) {
            return result;
        }
        Pattern pattern = Pattern.compile("\\$\\{([^}]+)\\}");
        Matcher matcher = pattern.matcher(text);

        while (matcher.find()) {
            String content = matcher.group(1);
            if (content != null && !content.trim().isEmpty()) {
                result.add(content.trim());
            }
        }

        return result;
    }

    /**
     * 解析 Shell 文本中的 target_tables 列表
     * @param shellContent 完整的 Shell 脚本内容
     * @return 提取到的表名列表
     */
    public static List<String> extractTargetTables(String shellContent) {
        List<String> tables = new ArrayList<>();
        if (shellContent == null || shellContent.isEmpty()) {
            return tables;
        }

        String marker = "##target_tables##";
        int index = shellContent.indexOf(marker);
        if (index == -1) {
            return tables;
        }

        String subContent = shellContent.substring(index + marker.length());

        // 匹配以 # 开头，后面跟表名（允许字母、数字、下划线和点），整行可有前后空格
        Pattern pattern = Pattern.compile("(?m)^#\\s*([A-Za-z0-9_.]+)\\s*$");
        Matcher matcher = pattern.matcher(subContent);

        while (matcher.find()) {
            String table = matcher.group(1).trim();
            if (!table.isEmpty() && !tables.contains(table)) {
                tables.add(table);
            }
        }
        return tables;
    }

    public static String getAlertShell(String alertTemplate, String token, String mentionedUsers) {
        List<String> strings = extractFromBraces(alertTemplate);
        String first = strings.getFirst();
        String shellTemplate = """
        #!/bin/bash
        set -ex
        if [ -n "${%s}" ]; then
            # 发送企业微信 Webhook
            curl 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=%s' \\
                 -H 'Content-Type: application/json' \\
                 -d "{
                    \\"msgtype\\": \\"text\\",
                    \\"text\\": {
                        \\"content\\": \\"%s\\",
                        \\"mentioned_mobile_list\\": [%s]
                    }
                 }"
            exit 0
        fi
        """;
        if (mentionedUsers == null || mentionedUsers.isEmpty()) {
            shellTemplate = """
                    #!/bin/bash
                    set -ex
                    if [ -n "${%s}" ] && [ "${%s}" != "0" ] && [ "${%s}" != "null" ]; then
                        # 发送企业微信 Webhook
                        curl 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=%s' \\
                             -H 'Content-Type: application/json' \\
                             -d "{
                                \\"msgtype\\": \\"markdown\\",
                                \\"markdown\\": {
                                    \\"content\\": \\"【%s】\\"
                                }
                             }"
                        exit 0
                    fi
                    """;
            return String.format(shellTemplate, first, first, first, token, alertTemplate);
        }
        return String.format(shellTemplate, first, token, alertTemplate, mentionedUsers);
    }

    public static String getMentionedUsers(String mentionedUsers) {
        return Arrays.stream(mentionedUsers.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(s -> "\\\"" + s + "\\\"")
                .collect(Collectors.joining(","));
    }
    public static String removeDashLines(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        // 替换以 ---- 开头的行（包括换行符）
        return text.replaceAll("(?m)^----.*\\r?\\n?", "");
    }

    public static String md5(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) { throw new RuntimeException(e); }
    }
    public static  String inferTargetFromFilename(String filename) {
        int dot = filename.lastIndexOf('.');
        return dot > 0 ? filename.substring(0, dot) : filename;
    }

    public static String extractComments(String sql) {
        String[] lines = sql.split("\n");
        StringBuilder comments = new StringBuilder();
        boolean startExtracting = false;
        for (String line : lines) {
            if (line.trim().startsWith("--")) {
                startExtracting = true;
            }
            if (startExtracting && !line.trim().startsWith("--")) {
                break;
            }
            if (startExtracting && line.trim().startsWith("--")) {
                String comment = line.replaceAll("^--", "").replaceAll("#", "").trim();
                comments.append(comment).append("\n");
            }
        }
        return comments.toString().trim();
    }


    public static List<Map<String, String>> workflowTriples(String scriptContent, String workflowName, String filePath) {
        ArrayList<Integer> sepStarts = new ArrayList<>();
        ArrayList<Integer> sepEnds = new ArrayList<>();
        ArrayList<String> types = new ArrayList<>();
        Pattern p = Pattern.compile("(?m)^\\s*---\\s*([a-zA-Z0-9_]+)\\s*---\\s*$");
        Matcher m = p.matcher(scriptContent);
        while (m.find()) {
            sepStarts.add(m.start());
            sepEnds.add(m.end());
            types.add(m.group(1));
        }
        List<Map<String, String>> res = new ArrayList<>();
        if (types.isEmpty()) {
            Map<String, String> t = new LinkedHashMap<>();
            t.put("task_name", workflowName);
            if (filePath.contains("shell") || filePath.contains("sh")) {
                t.put("task_type", "shell");
            } else {
                t.put("task_type", "hive");
            }
            t.put("task_content", scriptContent.trim());
            res.add(t);

            if (scriptContent.contains("target_tables")) {
                List<String> targetTableList = TextUtils.extractTargetTables(scriptContent);
                String targetTables = targetTableList.stream()
                        .map( table -> "'" + table.replace("'", "''") + "'")
                        .collect(Collectors.joining(","));
                Map<String, String> task = new LinkedHashMap<>();
                task.put("task_name", workflowName + "-callback");
                task.put("task_type", "sql");
                task.put("task_content", "update workflow_deploy set status = 'Y' where workflow_name in (" + targetTables + ")");
                res.add(task);
            }

            return res;
        }
        for (int i = 0; i < types.size(); i++) {
            int contentStart = sepEnds.get(i);
            int contentEnd = (i + 1 < sepStarts.size()) ? sepStarts.get(i + 1) : scriptContent.length();
            String content = scriptContent.substring(contentStart, contentEnd).trim();
            Map<String, String> t = new LinkedHashMap<>();
            t.put("task_name", workflowName + (i + 1));
            t.put("task_type", types.get(i) == null ? "hive" : types.get(i).toLowerCase());
            t.put("task_content", content);
            res.add(t);
        }
        return res;
    }

    public static String replaceSqlContent(String sqlContent) {

        sqlContent = sqlContent.replace("${pt_day}", "'2025-01-01'")
                .replace("${imp_pt_day}", "'2025-01-01'");
        return sqlContent;
    }

    public static String getTaskCodes(WorkflowDefineParam param) {
        if (param == null || param.getTaskDefinitionJson() == null) return "";
        StringBuilder sb = new StringBuilder();
        List<TaskDefinition> list = param.getTaskDefinitionJson();
        for (int i = 0; i < list.size(); i++) {
            Long code = list.get(i).getCode();
            if (code == null) continue;
            if (sb.length() > 0) sb.append(',');
            sb.append(code);
        }
        return sb.toString();
    }

    public static boolean checkDescriptionLength(String description) {
        log.info("checkDescriptionLength description length:{}", description.length());
        return description.codePointCount(0, description.length()) > 255;
    }

    public static String base64Decode(String content) {
        byte[] decodedBytes = Base64.getDecoder().decode(content);
        return new String(decodedBytes, StandardCharsets.UTF_8);
    }

    public static String extractSql(String content) {
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
