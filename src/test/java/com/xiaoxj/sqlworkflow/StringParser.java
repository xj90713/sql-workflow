package com.xiaoxj.sqlworkflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class StringParser {

    /**
     * 解析首行内容并返回 List
     * @param input 完整的原始字符串
     * @return 分隔后的字符串列表
     */
    public static List<String> parseFirstLine(String input) {
        if (input == null || input.isEmpty()) {
            return new ArrayList<>();
        }

        // 1. 获取第一行内容
        String firstLine = input.split("\n")[0];

        // 2. 根据 "|" 分隔符拆分，并对每个元素进行去空格处理
        return Arrays.stream(firstLine.split("\\|"))
                .map(String::trim)
                .collect(Collectors.toList());
    }

    public static void main(String[] args) {
        String rawData = "----hz|e363807e-4b18-4e6d-886d-320d66c3953a|0 0 * * * ? *|告警内容\n" +
                "select ..."; // 这里省略后面的 SQL 部分

        List<String> result = parseFirstLine(rawData);

        // 输出结果验证
        System.out.println(result);
    }
}