package com.xiaoxj.sqlworkflow.service;

import com.xiaoxj.sqlworkflow.core.DolphinClient;
import com.xiaoxj.sqlworkflow.enums.*;
import com.xiaoxj.sqlworkflow.instance.WorkflowInstanceCreateParam;
import com.xiaoxj.sqlworkflow.instance.WorkflowInstanceCreateParams;
import com.xiaoxj.sqlworkflow.workflow.WrokflowDefineParam;
import com.xiaoxj.sqlworkflow.workflow.WrokflowDefineResp;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DolphinSchedulerService {
    private final DolphinClient dolphinClient;

    public DolphinSchedulerService(DolphinClient dolphinClient) {
        this.dolphinClient = dolphinClient;
    }

    public List<Long> generateTaskCodes(Long projectCode, int count) {
        return dolphinClient.opsForWorkflow().generateTaskCode(projectCode, count);
    }

    public List<WrokflowDefineResp> listWorkflows(Long projectCode, Integer pageNo, Integer pageSize, String searchVal) {
        return dolphinClient.opsForWorkflow().page(projectCode, pageNo, pageSize, searchVal);
    }

    public WrokflowDefineResp createWorkflow(Long projectCode, WrokflowDefineParam param) {
        return dolphinClient.opsForWorkflow().create(projectCode, param);
    }

    public WrokflowDefineResp updateWorkflow(Long projectCode, Long workflowCode, WrokflowDefineParam param) {
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

    public boolean startWorkflow(Long projectCode, Long workflowCode) {
        WorkflowInstanceCreateParam workflowInstanceCreateParam = createWorkflowInstanceCreateParam(workflowCode);
        return dolphinClient.opsForWorkflowInst().start(projectCode, workflowInstanceCreateParam);
    }

    public boolean startWorkflows(Long projectCode, String workflowCodes) {
        WorkflowInstanceCreateParams workflowInstanceCreateParams = createWorkflowInstanceCreateParams(workflowCodes);
        return dolphinClient.opsForWorkflowInst().batchStart(projectCode, workflowInstanceCreateParams);
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
}
