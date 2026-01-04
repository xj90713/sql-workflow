package com.xiaoxj.sqlworkflow.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.xiaoxj.sqlworkflow.common.enums.*;
import com.xiaoxj.sqlworkflow.common.utils.*;
import com.xiaoxj.sqlworkflow.core.DolphinClient;
import com.xiaoxj.sqlworkflow.dolphinscheduler.schedule.ScheduleDefineParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.schedule.ScheduleInfoResp;
import com.xiaoxj.sqlworkflow.dolphinscheduler.task.*;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.Parameter;
import com.xiaoxj.sqlworkflow.dolphinscheduler.instance.WorkflowInstanceCreateParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.instance.WorkflowInstanceCreateParams;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WorkflowDefineParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WorkflowDefineResp;
import com.xiaoxj.sqlworkflow.remote.HttpMethod;
import com.xiaoxj.sqlworkflow.remote.HttpRestResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class DolphinSchedulerService {

    private final DolphinClient dolphinClient;


    @Value("${dolphin.tenant.code}")
    private String tenantCode;


    public WorkflowDefineResp createWorkflow(Long projectCode, WorkflowDefineParam param) {
        return dolphinClient.opsForWorkflow().create(projectCode, param);
    }

    public WorkflowDefineResp updateWorkflow(Long projectCode, Long workflowCode, WorkflowDefineParam param) {
        return dolphinClient.opsForWorkflow().update(projectCode, param, workflowCode);
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
        List<String> strings = TextUtils.parseFirstLine(sqlContent);
        String alertTemplate = strings.get(3);
        String mentionedUsers = strings.size() > 4 ? TextUtils.getMentionedUsers(strings.get(4)) : "";
        String sql = TextUtils.removeDashLines(sqlContent);
        List<String> alertParamsList = TextUtils.extractFromBraces(alertTemplate);
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

        String finalScript = TextUtils.getAlertShell(alertTemplate, token, mentionedUsers);
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
}
