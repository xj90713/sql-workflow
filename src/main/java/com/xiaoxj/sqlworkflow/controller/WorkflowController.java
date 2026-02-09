package com.xiaoxj.sqlworkflow.controller;

import com.xiaoxj.sqlworkflow.common.result.BaseResult;
import com.xiaoxj.sqlworkflow.common.utils.SchedulerUtils;
import com.xiaoxj.sqlworkflow.common.utils.TextUtils;
import com.xiaoxj.sqlworkflow.common.utils.WorkflowUtils;
import com.xiaoxj.sqlworkflow.dolphinscheduler.schedule.ScheduleDefineParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.schedule.ScheduleInfoResp;
import com.xiaoxj.sqlworkflow.entity.AlertWorkflowDeploy;
import com.xiaoxj.sqlworkflow.entity.WorkflowDeploy;
import com.xiaoxj.sqlworkflow.repository.AlertWorkflowDeployRepository;
import com.xiaoxj.sqlworkflow.repository.WorkflowDeployRepository;
import com.xiaoxj.sqlworkflow.service.DolphinSchedulerService;
import com.xiaoxj.sqlworkflow.service.SqlLineageService;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WorkflowDefineParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WorkflowDefineResp;
import com.xiaoxj.sqlworkflow.service.WorkflowQueueService;
import jakarta.annotation.Resource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/dependencies")
@RequiredArgsConstructor
@Slf4j
public class WorkflowController {
    @Resource
    private SqlLineageService lineageService;

    @Resource
    private DolphinSchedulerService dolphinSchedulerService;

    @Resource
    private WorkflowUtils workflowUtils;

    @Resource
    private SchedulerUtils schedulerUtils;

    @Value("#{${dolphin.project.code}}")
    private Long projectCode;

    @Value("#{${dolphin.alertProject.code}}")
    private Long alertProjectCode;

    @Resource
    private WorkflowDeployRepository deployRepo;

    @Resource
    private AlertWorkflowDeployRepository alertDeployRepo;

    @Resource
    private WorkflowQueueService queueService;

    @PostMapping(value = "/addWorkflow", consumes = MediaType.APPLICATION_JSON_VALUE)
    public BaseResult<WorkflowDeploy> addWorkflow(@RequestBody Map<String, String> payload) {
        String filePath = payload.get("file_path");
        String workflowName = payload.get("file_path").substring(filePath.lastIndexOf("/") + 1, filePath.lastIndexOf("."));
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        String content = payload.get("content");
        String sqlContent = TextUtils.base64Decode(content);
        String user = payload.getOrDefault("commit_user", "system");
        List<Map<String, String>> taskTriples = TextUtils.workflowTriples(sqlContent,workflowName, filePath);
        String describe = TextUtils.extractComments(sqlContent);
        WorkflowDefineParam workDefinition = workflowUtils.createWorkDefinition(taskTriples, projectCode, workflowName,describe);
        WorkflowDefineResp workflowDefineResp = dolphinSchedulerService.createWorkflow(projectCode, workDefinition);
        String taskCodesString = TextUtils.getTaskCodes(workDefinition);
        long workflowCode = workflowDefineResp.getCode();
        // 创建任务流之后 需要上线该任务W
        dolphinSchedulerService.onlineWorkflow(projectCode, workflowCode);
        long projectCode = workflowDefineResp.getProjectCode();
        Set<String> targetTables = TextUtils.getTablesOrDependencies(sqlContent,"target_tables");
        if (!targetTables.isEmpty()) {
            targetTables.forEach(targetTable ->
                    lineageService.addWorkflowDeploy(targetTable, filePath, fileName, "insert into " + targetTable + " values(1);", user, 0, 0, null));
            WorkflowDeploy workflowDeploy = lineageService.addWorkflowDeploy(workflowName, filePath, fileName, sqlContent, user, workflowCode, projectCode, taskCodesString);
            return BaseResult.success(workflowDeploy);

        }
        WorkflowDeploy workflowDeploy = lineageService.addWorkflowDeploy(workflowName, filePath, fileName, sqlContent, user, workflowCode, projectCode, taskCodesString);
        return BaseResult.success(workflowDeploy);
    }

    @PostMapping(value = "/updateWorkflow", consumes = MediaType.APPLICATION_JSON_VALUE)
    public BaseResult<WorkflowDeploy> updateWorkflow(@RequestBody Map<String, String> payload) {
        String filePath = payload.get("file_path");
        String workflowName = payload.get("file_path").substring(filePath.lastIndexOf("/") + 1, filePath.lastIndexOf("."));
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        String content = payload.get("content");
        String sqlContent = TextUtils.base64Decode(content);
        String user = payload.getOrDefault("commit_user", "system");
        WorkflowDeploy workflowDeploy = deployRepo.findByWorkflowName(workflowName);
        if (workflowDeploy == null) {
            log.info("工作流不存在，创建工作流");
            WorkflowDefineResp workflow = dolphinSchedulerService.getWorkflow(projectCode, workflowName);
            if (workflow != null) {
                String taskCodes = TextUtils.getTaskCodes(workflow);
                long workflowCode = workflow.getCode();
                WorkflowDeploy workflowDeployResp = lineageService.addWorkflowDeploy(workflowName, filePath, fileName, sqlContent, user, workflowCode, projectCode, taskCodes);
                return BaseResult.success(workflowDeployResp);
            }
            return addWorkflow(payload);
        }
        long workflowCode = workflowDeploy.getWorkflowCode();
        long projectCode = workflowDeploy.getProjectCode();
        Set<String> targetTables = TextUtils.getTablesOrDependencies(sqlContent,"target_tables");
        if (!targetTables.isEmpty()) {
            targetTables.forEach(targetTable ->  {
                        List<WorkflowDeploy> byTargetTable = deployRepo.findByTargetTableAndStatus(targetTable);
                Optional<WorkflowDeploy> deploy = byTargetTable.stream().filter(w -> w.getWorkflowName().equals(targetTable)).findFirst();
                if (deploy.isEmpty()) {
                            lineageService.addWorkflowDeploy(targetTable, filePath, fileName, "insert into " + targetTable + " values(1);", user, 0, 0, null);
                        }
                        lineageService.updateWorkflowDeploy(targetTable, filePath, fileName, "insert into " + targetTable + " values(1);", user, 0, 0,null);
                    });
        }
        String describe = TextUtils.extractComments(sqlContent);
        // 更新工作流之前，必须要下线改任务
        dolphinSchedulerService.offlineWorkflow(projectCode, workflowCode);
        List<Map<String, String>> taskTriples = TextUtils.workflowTriples(sqlContent, workflowName, filePath);
        WorkflowDefineParam workDefinition = workflowUtils.createWorkDefinition(taskTriples, projectCode, workflowName, describe);
        String taskCodesString = TextUtils.getTaskCodes(workDefinition);
        WorkflowDefineResp workflowDefineResp = dolphinSchedulerService.updateWorkflow(projectCode, workflowCode, workDefinition);
        // 更新工作流之后，在上线工作流
        dolphinSchedulerService.onlineWorkflow(projectCode, workflowCode);
        WorkflowDeploy updateWorkflowDeploy = lineageService.updateWorkflowDeploy(workflowName, filePath, fileName, sqlContent, user, workflowCode, projectCode, taskCodesString);
        return BaseResult.success(updateWorkflowDeploy);
    }

    @PostMapping("/addWorkflowAndScheduler")
    public BaseResult<ScheduleInfoResp>  addWorkflowScheduler(@RequestBody Map<String, String> payload) {
        String content = payload.get("content");
        String filePath = payload.get("file_path");
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        String user = payload.getOrDefault("commit_user", "system");
        String workflowName = payload.get("file_path").substring(filePath.lastIndexOf("/") + 1, filePath.lastIndexOf("."));
        log.info("fileName={}, content={}, workflowName={}", fileName, content, workflowName);
        String sqlContent = TextUtils.base64Decode(content);
        List<String> alertScheduler = TextUtils.parseFirstLine(sqlContent);
        String dbName = alertScheduler.get(0);
        String token = alertScheduler.get(1);
        String scheduleTime = alertScheduler.get(2);
        String alertTemplate = alertScheduler.get(3);
        log.info("dbName={}, token={}, scheduleTime={}, alertTemplate={}", dbName, token,scheduleTime, alertTemplate);
        WorkflowDefineParam workDefinition = workflowUtils.createAlertWorkDefinition(alertProjectCode, workflowName,sqlContent, dbName, token);
        log.info("workDefinition={}", workDefinition);
        WorkflowDefineResp workflowDefineResp = dolphinSchedulerService.createWorkflow(alertProjectCode, workDefinition);
        log.info("workflowDefineResp={}", workflowDefineResp);
        String taskCodesString = TextUtils.getTaskCodes(workDefinition);
        long workflowCode = workflowDefineResp.getCode();
        dolphinSchedulerService.onlineWorkflow(alertProjectCode, workflowCode);
        ScheduleDefineParam scheduleDefineParam = schedulerUtils.createScheduleDefineParam(alertProjectCode, workflowCode, scheduleTime);
        ScheduleInfoResp schedule = dolphinSchedulerService.createSchedule(alertProjectCode, scheduleDefineParam);
        dolphinSchedulerService.onlineSchedule(alertProjectCode, schedule.getId());
        lineageService.addAlertWorkflowDeploy(workflowName, filePath, fileName, sqlContent, user, taskCodesString, schedule.getId(), workflowCode, alertProjectCode, scheduleTime);
        return BaseResult.success(schedule);
    }

    @PostMapping("/updateWorkflowAndScheduler")
    public BaseResult<ScheduleInfoResp>  updateWorkflowScheduler(@RequestBody Map<String, String> payload) {
        String filelName = payload.get("file_name");
        String content = payload.get("content");
        String filePath = payload.get("file_path");
        String user = payload.getOrDefault("commit_user", "system");
        String workflowName = payload.get("file_path").substring(filePath.lastIndexOf("/") + 1, filePath.lastIndexOf("."));
        String sqlContent = TextUtils.base64Decode(content);
        List<String> alertScheduler = TextUtils.parseFirstLine(sqlContent);
        String dbName = alertScheduler.get(0);
        String token = alertScheduler.get(1);
        String scheduleTime = alertScheduler.get(2);
        String alertTemplate = alertScheduler.get(3);
        log.info("dbName={}, token={}, scheduleTime={}, alertTemplate={}", dbName, token,scheduleTime, alertTemplate);
        log.info("workflowName={}", workflowName);
        AlertWorkflowDeploy alertWorkflowDeploy = alertDeployRepo.findByWorkflowName(workflowName);
        log.info("alertWorkflowDeploy={}", alertWorkflowDeploy);
        if (alertWorkflowDeploy == null) {
            log.info("工作流不存在，创建工作流");
            return addWorkflowScheduler(payload);
        }
        long workflowCode = alertWorkflowDeploy.getWorkflowCode();
        long alertProjectCode = alertWorkflowDeploy.getProjectCode();
        long schedulerId = alertWorkflowDeploy.getScheduleId();
        dolphinSchedulerService.offlineWorkflow(alertProjectCode, workflowCode);
        WorkflowDefineParam workDefinition = workflowUtils.createAlertWorkDefinition(alertProjectCode, workflowName,sqlContent, dbName, token);
        dolphinSchedulerService.updateWorkflow(alertProjectCode, workflowCode, workDefinition);
        ScheduleDefineParam scheduleDefineParam = schedulerUtils.createScheduleDefineParam(alertProjectCode, workflowCode, scheduleTime);
        dolphinSchedulerService.onlineWorkflow(alertProjectCode, workflowCode);
        ScheduleInfoResp schedule = dolphinSchedulerService.updateSchedule(alertProjectCode,schedulerId,scheduleDefineParam);
        String taskCodesString = TextUtils.getTaskCodes(workDefinition);
        dolphinSchedulerService.onlineSchedule(alertProjectCode, schedule.getId());
        lineageService.updateAlertWorkflowDeploy(workflowName, filePath, filelName, sqlContent, user, taskCodesString, schedulerId, workflowCode, alertProjectCode, scheduleTime);
        return BaseResult.success(schedule);
    }

    @PostMapping("/updateWorkflowStatus")
    @Transactional
    public BaseResult<String> updateWorkflowSchedulerStatus(@RequestBody String tableName) {
        List<WorkflowDeploy> workflowDeployList = deployRepo.findByStatusAndScheduleTypeAndIsDelete('Y', 1,0);
        Map<String, String> map = new HashMap<>();
        workflowDeployList.forEach(deploy -> {
            map.put(deploy.getTargetTable(), deploy.getSourceTables());
        });
        Set<String> affectedTables = queueService.getAffectedTables(tableName, map);
        int num = deployRepo.updateStatusByTargetTable(affectedTables);
        log.info("Affected tables size: {}", num);
        return BaseResult.success("Updated rows: " + affectedTables.size());
    }
}
