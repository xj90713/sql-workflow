import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ShellParser {

    /**
     * 解析 Shell 文本中的 target_tables 列表
     * @param shellContent 完整的 Shell 脚本内容
     * @return 提取到的表名列表
     */
    public static List<String> extractTargetTables(String shellContent) {
        List<String> tables = new ArrayList<>();
        String marker = "##target_tables##";
        int index = shellContent.indexOf(marker);

        if (index == -1) {
            return tables;
        }

        // 2. 截取标识符之后的内容
        String subContent = shellContent.substring(index + marker.length());

        // 3. 使用正则匹配 # 后面紧跟的表名
        // ^#\\s*(\\w+) 匹配行首的 #，忽略可能的空格，捕获单词字符
        Pattern pattern = Pattern.compile("^#\\s*([a-zA-Z0-9_]+)", Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(subContent);

        while (matcher.find()) {
            tables.add(matcher.group(1));
        }
        return tables;
    }

    public static Set<String> getTablesOrDependencies(String shellContent, String type) {
        Set<String> tables = new LinkedHashSet<>();
        if (shellContent == null || shellContent.trim().isEmpty() || type == null || type.trim().isEmpty()) {
            return tables;
        }

        String marker = String.format("##%s##", type.trim());
        int start = shellContent.indexOf(marker);
        if (start == -1) {
            return tables;
        }

        // 从当前 marker 后开始截取
        start += marker.length();

        // 找下一个 ##...## marker，避免把后续 block 也解析进来
        Pattern nextMarkerPattern = Pattern.compile("(?m)^##[A-Za-z0-9_]+##\\s*$");
        Matcher nextMarkerMatcher = nextMarkerPattern.matcher(shellContent);
        int end = shellContent.length();

        while (nextMarkerMatcher.find()) {
            if (nextMarkerMatcher.start() >= start) {
                end = nextMarkerMatcher.start();
                break;
            }
        }

        String blockContent = shellContent.substring(start, end);

        // 匹配以 # 开头的表名/依赖名
        Pattern tablePattern = Pattern.compile("(?m)^#\\s*([A-Za-z0-9_.-]+)\\s*$");
        Matcher tableMatcher = tablePattern.matcher(blockContent);

        while (tableMatcher.find()) {
            String table = tableMatcher.group(1).trim();
            if (!table.isEmpty()) {
                tables.add(table);
            }
        }

        return tables;
    }


    public static void main(String[] args) {
        String shellScript = """
                python3 /root/python/user_action_tag_model.py
                ##target_tables##
                #db_tag.dwd_user_action_tag_dd
                ##source_tables##
                #db_tag.dwd_user_action_summary_30d
                """;

        List<String> tableList = getTablesOrDependencies(shellScript,"source_tables").stream().toList();

        // 打印结果
        tableList.forEach(System.out::println);
    }
}