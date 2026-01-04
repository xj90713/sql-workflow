package com.xiaoxj.sqlworkflow.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.xiaoxj.sqlworkflow.common.utils.WorkflowUtils;
import com.xiaoxj.sqlworkflow.core.DolphinClient;
import com.xiaoxj.sqlworkflow.dolphinscheduler.instance.WorkflowInstanceCreateParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.schedule.ScheduleDefineParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.schedule.ScheduleInfoResp;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WorkflowDefineParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WorkflowDefineResp;
import com.xiaoxj.sqlworkflow.remote.HttpRestResult;
import com.xiaoxj.sqlworkflow.service.DolphinSchedulerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class DolphinSchedulerServiceImpl implements DolphinSchedulerService {

    private final DolphinClient dolphinClient;

    private final WorkflowUtils workflowUtils;

    @Value("${dolphin.tenant.code}")
    private String tenantCode;


    @Override
    public WorkflowDefineResp createWorkflow(Long projectCode, WorkflowDefineParam param) {
        return dolphinClient.opsForWorkflow().create(projectCode, param);
    }

    @Override
    public WorkflowDefineResp updateWorkflow(Long projectCode, Long workflowCode, WorkflowDefineParam param) {
        return dolphinClient.opsForWorkflow().update(projectCode, param, workflowCode);
    }

    @Override
    public boolean onlineWorkflow(Long projectCode, Long workflowCode) {
        return dolphinClient.opsForWorkflow().online(projectCode, workflowCode);
    }

    @Override
    public boolean offlineWorkflow(Long projectCode, Long workflowCode) {
        return dolphinClient.opsForWorkflow().offline(projectCode, workflowCode);
    }

    @Override
    public HttpRestResult<JsonNode> startWorkflow(Long projectCode, Long workflowCode) {
        WorkflowInstanceCreateParam workflowInstanceCreateParam = workflowUtils.createWorkflowInstanceCreateParam(workflowCode);
        return dolphinClient.opsForWorkflowInst().start(projectCode, workflowInstanceCreateParam);
    }


    @Override
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

    @Override
    public String getWorkflowInstanceStatus(Long projectCode, Long workflowInstanceId) {
        return dolphinClient.opsForWorkflowInst().getWorkflowInstanceStatus(projectCode, workflowInstanceId);
    }


    @Override
    public boolean onlineSchedule(Long projectCode, Long scheduleId) {
        return dolphinClient.opsForSchedule().online(projectCode, scheduleId);
    }

    @Override
    public ScheduleInfoResp createSchedule(Long projectCode, ScheduleDefineParam scheduleDefineParam) {
        return dolphinClient.opsForSchedule().create(projectCode, scheduleDefineParam);
    }

    @Override
    public ScheduleInfoResp updateSchedule(Long projectCode,Long scheduleId, ScheduleDefineParam scheduleDefineParam) {
        return dolphinClient.opsForSchedule().update(projectCode,scheduleId, scheduleDefineParam);
    }

}
