package com.xiaoxj.sqlworkflow.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.xiaoxj.sqlworkflow.core.DolphinClient;
import com.xiaoxj.sqlworkflow.dolphinscheduler.instance.WorkflowInstanceQueryResp;
import com.xiaoxj.sqlworkflow.dolphinscheduler.task.HivecliTask;
import com.xiaoxj.sqlworkflow.dolphinscheduler.task.HttpTask;
import com.xiaoxj.sqlworkflow.dolphinscheduler.task.ShellTask;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.TaskDefinition;
import com.xiaoxj.sqlworkflow.domain.WorkflowDeploy;
import com.xiaoxj.sqlworkflow.domain.TaskStatus;
import com.xiaoxj.sqlworkflow.enums.*;
import com.xiaoxj.sqlworkflow.dolphinscheduler.instance.WorkflowInstanceCreateParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.instance.WorkflowInstanceCreateParams;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WorkflowDefineParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WorkflowDefineResp;
import com.xiaoxj.sqlworkflow.remote.HttpMethod;
import com.xiaoxj.sqlworkflow.remote.HttpRestResult;
import com.xiaoxj.sqlworkflow.repo.WorkflowDependencyRepository;
import com.xiaoxj.sqlworkflow.repo.WorkflowDeployRepository;
import com.xiaoxj.sqlworkflow.repo.TaskStatusRepository;
import com.xiaoxj.sqlworkflow.util.TaskDefinitionUtils;
import com.xiaoxj.sqlworkflow.util.TaskLocationUtils;
import com.xiaoxj.sqlworkflow.util.TaskRelationUtils;
import com.xiaoxj.sqlworkflow.util.TaskUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Service
public class DolphinSchedulerService {
    private final DolphinClient dolphinClient;

    private final TaskStatusRepository statusRepo;
    private final WorkflowDependencyRepository depRepo;
    private final WorkflowDeployRepository deployRepo;

    @Value("dolphin.token")
    private String token;

    @Value("#{${dolphin.project.code}}")
    private Long projectCode;

    @Value("${dolphin.tenant.code}")
    private String tenantCode;

    @Value("${dolphin.max.parallelism}")
    private int maxParallelism;

    public DolphinSchedulerService(DolphinClient dolphinClient, TaskStatusRepository statusRepo, WorkflowDependencyRepository depRepo, WorkflowDeployRepository deployRepo) {
        this.dolphinClient = dolphinClient;
        this.statusRepo = statusRepo;
        this.depRepo = depRepo;
        this.deployRepo = deployRepo;
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
        java.util.List<Long> taskCodes = dolphinClient.opsForWorkflow().generateTaskCode(projectCode, tasks.size());
        java.util.List<TaskDefinition> defs = new java.util.ArrayList<>();
        for (int i = 0; i < tasks.size(); i++) {
            java.util.Map<String, String> t = tasks.get(i);
            String type = String.valueOf(t.getOrDefault("task_type", "default")).toLowerCase();
            String content = String.valueOf(t.getOrDefault("task_content", ""));
            if ("hive".equals(type)) {
                HivecliTask hive = new HivecliTask();
                hive.setHiveSqlScript(content);
                defs.add(TaskDefinitionUtils.createDefaultTaskDefinition(taskCodes.get(i), hive));
            } else if ("shell".equals(type)) {
                ShellTask sh = new ShellTask();
                sh.setRawScript(content);
                defs.add(TaskDefinitionUtils.createDefaultTaskDefinition(taskCodes.get(i), sh));
            } else if ("http".equals(type)) {
                HttpTask http = new HttpTask();
                http
                        .setUrl(content)
                        .setHttpMethod(HttpMethod.GET.toString())
                        .setHttpCheckCondition(HttpCheckCondition.STATUS_CODE_DEFAULT.toString())
                        .setCondition("")
                        .setConditionResult(TaskUtils.createEmptyConditionResult());
                defs.add(TaskDefinitionUtils.createDefaultTaskDefinition(taskCodes.get(i), http));
            } else {
                ShellTask sh = new ShellTask();
                sh.setRawScript(content);
                defs.add(TaskDefinitionUtils.createDefaultTaskDefinition(taskCodes.get(i), sh));
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

    public void triggerPending(long projectCode, String startWorkflows) {
        List<TaskStatus> pending = statusRepo.findByCurrentStatus(TaskStatus.Status.PENDING);
        int runningCount = statusRepo.findByCurrentStatus(TaskStatus.Status.RUNNING).size();
        int slots = Math.max(0, maxParallelism - runningCount);
        for (TaskStatus t : pending) {
            if (slots <= 0) break;
            WorkflowDeploy deploy = deployRepo.findByTaskName(t.getTaskName());
            if (deploy == null) continue;
//            boolean started = startWorkflows(projectCode, startWorkflows);
            boolean started = true;
            if (started) {
                t.setCurrentStatus(TaskStatus.Status.RUNNING);
                t.setUpdatedAt(LocalDateTime.now());
                statusRepo.save(t);
                slots--;
            }
        }
    }
}
