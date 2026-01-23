package com.xiaoxj.sqlworkflow.service;

import com.xiaoxj.sqlworkflow.entity.AlertWorkflowDeploy;
import com.xiaoxj.sqlworkflow.entity.WorkflowDeploy;
import java.util.*;


public interface SqlLineageService {
    WorkflowDeploy addWorkflowDeploy(String workflowName, String filePath, String fileName, String sqlContent, String commitUser, long workflowCode, long projectCode, String taskCodes);
    WorkflowDeploy updateWorkflowDeploy(String workflowName, String filePath, String fileName, String sqlContent, String commitUser, long workflowCode, long projectCode, String taskCodes);
    Map<String, String> getTargetAndSourceTablesOrDepedencies(String sqlContent, String fileName);
    AlertWorkflowDeploy addAlertWorkflowDeploy(String workflowName, String filePath, String fileName, String sqlContent, String commitUser, String taskCodes,long scheduleId, long workflowCode, long projectCode, String scheduleTime);
    AlertWorkflowDeploy updateAlertWorkflowDeploy(String workflowName, String filePath, String fileName, String sqlContent, String commitUser, String taskCodes,long scheduleId,long workflowCode, long projectCode, String scheduleTime);
}
