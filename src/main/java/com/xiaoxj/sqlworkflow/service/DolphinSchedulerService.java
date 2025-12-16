package com.xiaoxj.sqlworkflow.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.xiaoxj.sqlworkflow.core.DolphinClient;
import com.xiaoxj.sqlworkflow.dolphinscheduler.instance.WorkflowInstanceQueryResp;
import com.xiaoxj.sqlworkflow.dolphinscheduler.task.HivecliTask;
import com.xiaoxj.sqlworkflow.dolphinscheduler.task.HttpTask;
import com.xiaoxj.sqlworkflow.dolphinscheduler.task.ShellTask;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.TaskDefinition;
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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class DolphinSchedulerService {
    private final DolphinClient dolphinClient;


    @Value("${dolphin.tenant.code}")
    private String tenantCode;


    public DolphinSchedulerService(DolphinClient dolphinClient) {
        this.dolphinClient = dolphinClient;
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
        System.out.println("taskCodes:" + taskCodes);
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
            } else if ("http".equals(type)) {
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

}
