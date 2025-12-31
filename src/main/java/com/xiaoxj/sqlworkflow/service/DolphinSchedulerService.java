package com.xiaoxj.sqlworkflow.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.xiaoxj.sqlworkflow.core.DolphinClient;
import com.xiaoxj.sqlworkflow.dolphinscheduler.instance.WorkflowInstanceQueryResp;
import com.xiaoxj.sqlworkflow.dolphinscheduler.schedule.ScheduleDefineParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.schedule.ScheduleInfoResp;
import com.xiaoxj.sqlworkflow.dolphinscheduler.task.*;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.Parameter;
import com.xiaoxj.sqlworkflow.enums.*;
import com.xiaoxj.sqlworkflow.dolphinscheduler.instance.WorkflowInstanceCreateParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.instance.WorkflowInstanceCreateParams;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WorkflowDefineParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WorkflowDefineResp;
import com.xiaoxj.sqlworkflow.remote.HttpMethod;
import com.xiaoxj.sqlworkflow.remote.HttpRestResult;
import com.xiaoxj.sqlworkflow.util.TaskDefinitionUtils;
import com.xiaoxj.sqlworkflow.util.TaskLocationUtils;
import com.xiaoxj.sqlworkflow.util.TaskRelationUtils;
import com.xiaoxj.sqlworkflow.util.TaskUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DolphinSchedulerService {
    private final DolphinClient dolphinClient;
    private final DataSource dataSource;


    @Value("${dolphin.tenant.code}")
    private String tenantCode;


    public DolphinSchedulerService(DolphinClient dolphinClient, DataSource dataSource) {
        this.dolphinClient = dolphinClient;
        this.dataSource = dataSource;
    }

    public List<Long> generateTaskCodes(Long projectCode, int count) {
        return dolphinClient.opsForWorkflow().generateTaskCode(projectCode, count);
    }

    public List<WorkflowDefineResp> listWorkflows(Long projectCode, Integer pageNo, Integer pageSize, String searchVal) {
        return dolphinClient.opsForWorkflow().page(projectCode, pageNo, pageSize, searchVal);
    }

    public WorkflowDefineResp createWorkflow(Long projectCode, WorkflowDefineParam param) {
        return dolphinClient.opsForWorkflow().create(projectCode, param);
    }

    public WorkflowDefineResp updateWorkflow(Long projectCode, Long workflowCode, WorkflowDefineParam param) {
        return dolphinClient.opsForWorkflow().update(projectCode, param, workflowCode);
    }

    public boolean deleteWorkflow(Long projectCode, Long workflowCode) {
        return dolphinClient.opsForWorkflow().delete(projectCode, workflowCode);
    }
    public boolean onlineWorkflow(Long projectCode, Long workflowCode) {
        return dolphinClient.opsForWorkflow().online(projectCode, workflowCode);
    }

    public boolean offlineWorkflow(Long projectCode, Long workflowCode) {
        return dolphinClient.opsForWorkflow().offline(projectCode, workflowCode);
    }

    public HttpRestResult<JsonNode> startWorkflow(Long projectCode, Long workflowCode) {
        WorkflowInstanceCreateParam workflowInstanceCreateParam = createWorkflowInstanceCreateParam(workflowCode);
        return dolphinClient.opsForWorkflowInst().start(projectCode, workflowInstanceCreateParam);
    }


    public List<HttpRestResult<JsonNode>> startWorkflows(Long projectCode, String workflowCodes) {
        List<Long> workflowDefinitionCodeList = Arrays.stream(workflowCodes.split(","))
                .map(Long::parseLong)
                .toList();
        List<HttpRestResult<JsonNode>> result = new ArrayList<>();
        for (Long workflowDefinitionCode : workflowDefinitionCodeList) {
            HttpRestResult<JsonNode> jsonNodeHttpRestResult = startWorkflow(projectCode, workflowDefinitionCode);
            result.add(jsonNodeHttpRestResult);
        }
        return result;
    }

    public List<WorkflowInstanceQueryResp> listWorkflowInstances(Long projectCode, Long workflowCode) {
        return dolphinClient.opsForWorkflowInst().page(null, null, projectCode, workflowCode);
    }

    public WorkflowInstanceCreateParam createWorkflowInstanceCreateParam(Long workflowCode) {
        WorkflowInstanceCreateParam startParam = new WorkflowInstanceCreateParam();
        startParam
                .setWorkflowDefinitionCode(workflowCode)
                .setScheduleTime("")
                .setFailureStrategy(FailureStrategy.CONTINUE.toString())
                .setWarningType(WarningType.NONE.toString())
                .setWarningGroupId(0L)
                .setExecType("")
                .setStartNodeList("")
                .setTaskDependType(TaskDependType.TASK_POST.toString())
                .setRunMode(RunMode.RUN_MODE_SERIAL.toString())
                .setWorkflowInstancePriority(Priority.MEDIUM.toString())
                .setWorkerGroup("default")
                .setEnvironmentCode("")
                .setTenantCode(tenantCode)
                .setStartParams("")
                .setExpectedParallelismNumber("")
                .setDryRun(0);
        return startParam;
    }

    public WorkflowInstanceCreateParams createWorkflowInstanceCreateParams(String workflowCodes) {
        WorkflowInstanceCreateParams startParams = new WorkflowInstanceCreateParams();
        startParams
                .setWorkflowDefinitionCodes(workflowCodes)
                .setScheduleTime("")
                .setFailureStrategy(FailureStrategy.CONTINUE.toString())
                .setWarningType(WarningType.NONE.toString())
                .setWarningGroupId(0L)
                .setExecType("")
                .setStartNodeList("")
                .setTaskDependType(TaskDependType.TASK_POST.toString())
                .setRunMode(RunMode.RUN_MODE_SERIAL.toString())
                .setWorkflowInstancePriority(Priority.MEDIUM.toString())
                .setWorkerGroup("default")
                .setEnvironmentCode("")
                .setStartParams("")
                .setExpectedParallelismNumber("")
                .setDryRun(0);
        return startParams;
    }

    public WorkflowDefineParam createWorkDefinition(List<Map<String, String>> tasks, Long projectCode, String workflowName, String describe) {
        if (checkDescriptionLength(describe)) {
            log.warn("Parameter description is too long.");
            describe = describe.substring(0, 250);
        }
        List<Long> taskCodes = dolphinClient.opsForWorkflow().generateTaskCode(projectCode, tasks.size());
        List<TaskDefinition> defs = new ArrayList<>();
        for (int i = 0; i < tasks.size(); i++) {
            Map<String, String> t = tasks.get(i);
            String type = String.valueOf(t.getOrDefault("task_type", "hive")).toLowerCase();
            String content = String.valueOf(t.getOrDefault("task_content", ""));
            if ("hive".equals(type)) {
                HivecliTask hive = new HivecliTask();
                hive.setHiveSqlScript(content);
                defs.add(TaskDefinitionUtils.createDefaultTaskDefinition(t.get("task_name"),taskCodes.get(i), hive));
            } else if ("shell".equals(type)) {
                ShellTask sh = new ShellTask();
                sh.setRawScript(content);
                defs.add(TaskDefinitionUtils.createDefaultTaskDefinition(t.get("task_name"),taskCodes.get(i), sh));
            } else if ("sql".equals(type) || "doris".equals(type)) {
                SqlTask sqlTask = new SqlTask();
                if ("doris".equals(type)) {
                    sqlTask.setType("DORIS");
                    sqlTask.setDatasource(9);
                } else {
                    sqlTask.setType("MYSQL");
                    sqlTask.setDatasource(4);
                }
                sqlTask
                        .setSql(content)
                        .setSqlType("1")
                        .setSendEmail(false)
                        .setDisplayRows(10)
                        .setTitle("")
                        .setGroupId(null)
//                        .setConnParams("null")
                        .setConditionResult(TaskUtils.createEmptyConditionResult());
                defs.add(TaskDefinitionUtils.createDefaultTaskDefinition(t.get("task_name"),taskCodes.get(i), sqlTask));
            }else if ("http".equals(type)) {
                HttpTask http = new HttpTask();
                http
                        .setUrl(content)
                        .setHttpMethod(HttpMethod.GET.toString())
                        .setHttpCheckCondition(HttpCheckCondition.STATUS_CODE_DEFAULT.toString())
                        .setCondition("")
                        .setConditionResult(TaskUtils.createEmptyConditionResult());
                defs.add(TaskDefinitionUtils.createDefaultTaskDefinition(t.get("task_name"), taskCodes.get(i), http));
            } else {
                ShellTask sh = new ShellTask();
                sh.setRawScript(content);
                defs.add(TaskDefinitionUtils.createDefaultTaskDefinition(t.get("task_name"), taskCodes.get(i), sh));
            }
        }
        WorkflowDefineParam pcr = new WorkflowDefineParam();
        pcr.setName(workflowName)
                .setLocations(TaskLocationUtils.horizontalLocation(taskCodes.toArray(new Long[0])))
                .setDescription(describe)
                .setTenantCode(tenantCode)
                .setTimeout("0")
                .setExecutionType(WorkflowDefineParam.EXECUTION_TYPE_PARALLEL)
                .setTaskDefinitionJson(defs)
                .setTaskRelationJson(TaskRelationUtils.oneLineRelation(taskCodes.toArray(new Long[0])))
                .setGlobalParams(null);

        return pcr;
    }

    public WorkflowDefineParam createAlertWorkDefinition(Long projectCode, String workflowName, String sqlContent,  String dnName, String token) {

        List<Long> taskCodes = dolphinClient.opsForWorkflow().generateTaskCode(projectCode, 2);
        Integer datasourceId = dolphinClient.opsForDataSource().getDatasource(dnName).getId();
        List<String> strings = parseFirstLine(sqlContent);
        String alertTemplate = strings.get(3);
        String mentionedUsers = getMentionedUsers(strings.get(4));
        String sql = removeDashLines(sqlContent);
        List<String> alertParamsList = extractFromBraces(alertTemplate);
        List<Parameter> outTaskParamList = new ArrayList<>();
        List<Parameter> outLocalParams = new ArrayList<>();
        List<Parameter> inTaskParamList = new ArrayList<>();
        List<Parameter> inLocalParams = new ArrayList<>();
        Map<String, String> TaskParamMap = new HashMap<>();
        alertParamsList.forEach(param -> {
            inTaskParamList.add(new Parameter(param, "", "IN", "LIST"));
            inLocalParams.add(new Parameter(param, "", "IN", "LIST"));
            outTaskParamList.add(new Parameter(param, "", "OUT", "LIST"));
            outLocalParams.add(new Parameter(param, "", "OUT", "LIST"));
            TaskParamMap.put(param, "");
        });
        List<TaskDefinition> defs = new ArrayList<>();
        SqlTask sqlTask = new SqlTask();
        sqlTask
                .setType("MYSQL")
                .setDatasource(datasourceId)
                .setSql(sql)
                .setSqlType("0")
                .setSendEmail(false)
                .setDisplayRows(10)
                .setTitle("")
                .setGroupId(null)
                .setConnParams("")
                .setLocalParams(outLocalParams)
                .setTaskParamList(outTaskParamList)
                .setTaskParamMap(TaskParamMap)
                .setConditionResult(TaskUtils.createEmptyConditionResult());

        defs.add(TaskDefinitionUtils.createDefaultTaskDefinition(workflowName, taskCodes.get(0), sqlTask));
        ShellTask sh = new ShellTask();

        String finalScript = getAlertShell(alertTemplate, token, mentionedUsers);
        sh.setRawScript(finalScript);
        sh.setLocalParams(inLocalParams);
        sh.setTaskParamList(inTaskParamList);
        sh.setTaskParamMap(TaskParamMap);
        defs.add(TaskDefinitionUtils.createDefaultTaskDefinition("发送告警通知",taskCodes.get(1), sh));
        log.info("workflowName: {}" , workflowName);
        WorkflowDefineParam pcr = new WorkflowDefineParam();
        pcr.setName(workflowName)
                .setLocations(TaskLocationUtils.horizontalLocation(taskCodes.toArray(new Long[0])))
                .setDescription(workflowName)
                .setTenantCode(tenantCode)
                .setTimeout("0")
                .setExecutionType(WorkflowDefineParam.EXECUTION_TYPE_PARALLEL)
                .setTaskDefinitionJson(defs)
                .setTaskRelationJson(TaskRelationUtils.oneLineRelation(taskCodes.toArray(new Long[0])))
                .setGlobalParams(null);
        return pcr;
    }

    public String getTaskCodesString(WorkflowDefineParam param) {
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

    public String getWorkflowInstanceStatus(Long projectCode, Long workflowInstanceId) {
        return dolphinClient.opsForWorkflowInst().getWorkflowInstanceStatus(projectCode, workflowInstanceId);
    }

    public boolean checkDescriptionLength(String description) {
        log.info("checkDescriptionLength description length:{}", description.length());
        return description.codePointCount(0, description.length()) > 255;
    }

    public boolean onlineSchedule(Long projectCode, Long scheduleId) {
        return dolphinClient.opsForSchedule().online(projectCode, scheduleId);
    }

    public boolean offlineSchedule(Long projectCode, Long scheduleId) {
        return dolphinClient.opsForSchedule().offline(projectCode, scheduleId);
    }

    public ScheduleInfoResp createSchedule(Long projectCode, ScheduleDefineParam scheduleDefineParam) {
        return dolphinClient.opsForSchedule().create(projectCode, scheduleDefineParam);
    }

    public ScheduleInfoResp updateSchedule(Long projectCode,Long scheduleId, ScheduleDefineParam scheduleDefineParam) {
        return dolphinClient.opsForSchedule().update(projectCode,scheduleId, scheduleDefineParam);
    }

    public ScheduleDefineParam createScheduleDefineParam(Long projectCode,Long workflowCode, String schedule) {
        List<Long> taskCodes = dolphinClient.opsForWorkflow().generateTaskCode(projectCode, 2);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        ScheduleDefineParam scheduleDefineParam = new ScheduleDefineParam();
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime end = now.plusYears(100);

        scheduleDefineParam
                .setWorkflowDefinitionCode(workflowCode)
                .setTenantCode(tenantCode)
                .setSchedule(
                        new ScheduleDefineParam.Schedule()
                                .setStartTime(now.format(formatter))
                                .setEndTime(end.format(formatter))
                                .setCrontab(schedule));
        return scheduleDefineParam;
    }
    public List<String> parseFirstLine(String input) {
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
    public List<String> extractTargetTables(String shellContent) {
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

    public String getAlertShell(String alertTemplate, String token, String mentionedUsers) {
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
        return String.format(shellTemplate, first, token, alertTemplate, mentionedUsers);
    }

    public String getMentionedUsers(String mentionedUsers) {
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
}
