package com.xiaoxj.sqlworkflow.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.xiaoxj.sqlworkflow.dolphinscheduler.schedule.ScheduleDefineParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.schedule.ScheduleInfoResp;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WorkflowDefineParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WorkflowDefineResp;
import com.xiaoxj.sqlworkflow.remote.HttpRestResult;
import java.util.*;


public interface DolphinSchedulerService {

    WorkflowDefineResp createWorkflow(Long projectCode, WorkflowDefineParam param);

    WorkflowDefineResp updateWorkflow(Long projectCode, Long workflowCode, WorkflowDefineParam param);

    boolean onlineWorkflow(Long projectCode, Long workflowCode);
    boolean offlineWorkflow(Long projectCode, Long workflowCode);

    HttpRestResult<JsonNode> startWorkflow(Long projectCode, Long workflowCode);


    List<HttpRestResult<JsonNode>> startWorkflows(Long projectCode, String workflowCodes);


    String getWorkflowInstanceStatus(Long projectCode, Long workflowInstanceId);

    boolean onlineSchedule(Long projectCode, Long scheduleId);

    ScheduleInfoResp createSchedule(Long projectCode, ScheduleDefineParam scheduleDefineParam);

    ScheduleInfoResp updateSchedule(Long projectCode,Long scheduleId, ScheduleDefineParam scheduleDefineParam);

    WorkflowDefineResp getWorkflow(Long projectCode, String workflowName);
}
