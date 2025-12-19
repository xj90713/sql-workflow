package com.xiaoxj.sqlworkflow.controller;

import com.xiaoxj.sqlworkflow.dolphinscheduler.schedule.ScheduleDefineParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.schedule.ScheduleInfoResp;
import com.xiaoxj.sqlworkflow.domain.WorkflowDeploy;
import com.xiaoxj.sqlworkflow.repo.WorkflowDeployRepository;
import com.xiaoxj.sqlworkflow.service.DolphinSchedulerService;
import com.xiaoxj.sqlworkflow.service.SqlLineageService;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WorkflowDefineParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WorkflowDefineResp;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/dependencies")
@Slf4j
public class WorkflowController {
    @Autowired
    private SqlLineageService lineageService;

    @Autowired
    private DolphinSchedulerService dolphinSchedulerService;

    @Value("#{${dolphin.project.code}}")
    private Long projectCode;

    @Autowired
    private WorkflowDeployRepository deployRepo;
    @PostMapping(value = "/addWorkflow", consumes = MediaType.APPLICATION_JSON_VALUE)
    public WorkflowDeploy addWorkflow(@RequestBody Map<String, String> payload) {
        String filePath = payload.get("file_path");
        String workflowName = payload.get("file_path").substring(filePath.lastIndexOf("/") + 1, filePath.lastIndexOf("."));
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        String content = payload.get("content");
        byte[] decodedBytes = Base64.getDecoder().decode(content);
        String sqlContent = new String(decodedBytes, StandardCharsets.UTF_8);
        String user = payload.getOrDefault("commit_user", "system");
        List<Map<String, String>> taskTriples = lineageService.workflowTriples(sqlContent,workflowName);
        String describe = lineageService.extractComments(sqlContent);
        WorkflowDefineParam workDefinition = dolphinSchedulerService.createWorkDefinition(taskTriples, projectCode, workflowName,describe);
        WorkflowDefineResp workflowDefineResp = dolphinSchedulerService.createWorkflow(projectCode, workDefinition);
        String taskCodesString = dolphinSchedulerService.getTaskCodesString(workDefinition);
        long workflowCode = workflowDefineResp.getCode();
        // 创建任务流之后 需要上线该任务
        dolphinSchedulerService.onlineWorkflow(projectCode, workflowCode);
        long projectCode = workflowDefineResp.getProjectCode();
        WorkflowDeploy workflowDeploy = lineageService.addWorkflow(workflowName, filePath, fileName, sqlContent, user, workflowCode, projectCode, taskCodesString);
        return workflowDeploy;
    }


    @PostMapping(value = "/updateWorkflow", consumes = MediaType.APPLICATION_JSON_VALUE)
    public WorkflowDeploy updateWorkflow(@RequestBody Map<String, String> payload) {
        String filePath = payload.get("file_path");
        String workflowName = payload.get("file_path").substring(filePath.lastIndexOf("/") + 1, filePath.lastIndexOf("."));
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        String content = payload.get("content");
        byte[] decodedBytes = Base64.getDecoder().decode(content);
        String sqlContent = new String(decodedBytes, StandardCharsets.UTF_8);
        String user = payload.getOrDefault("commit_user", "system");
        WorkflowDeploy workflowDeploy = deployRepo.findByWorkflowName(workflowName);
        if (workflowDeploy == null) {
            log.info("工作流不存在，创建工作流");
            return addWorkflow(payload);
        }
        long workflowCode = workflowDeploy.getWorkflowCode();
        long projectCode = workflowDeploy.getProjectCode();
        String describe = lineageService.extractComments(sqlContent);
        // 更新工作流之前，必须要下线改任务
        dolphinSchedulerService.offlineWorkflow(projectCode, workflowCode);
        List<Map<String, String>> taskTriples = lineageService.workflowTriples(sqlContent, workflowName);
        WorkflowDefineParam workDefinition = dolphinSchedulerService.createWorkDefinition(taskTriples, projectCode, workflowName, describe);
        String taskCodesString = dolphinSchedulerService.getTaskCodesString(workDefinition);
        WorkflowDefineResp workflowDefineResp = dolphinSchedulerService.updateWorkflow(projectCode, workflowCode, workDefinition);
        // 更新工作流之后，在上线工作流
        dolphinSchedulerService.onlineWorkflow(projectCode, workflowCode);
        return lineageService.updateWorkflow(workflowName, filePath, fileName, sqlContent, user, taskCodesString, workflowCode, projectCode);
    }

    @PostMapping("/addWorkflowAndScheduler")
    public Map<String, Object> addWorkflowScheduler(@RequestBody Map<String, String> payload) {
        String filelName = payload.get("file_name");
        String content = payload.get("content");
        byte[] decodedBytes = Base64.getDecoder().decode(content);
        String sqlContent = new String(decodedBytes, StandardCharsets.UTF_8);
        String user = payload.getOrDefault("commit_user", "system");
        List<String> strings = dolphinSchedulerService.parseFirstLine(sqlContent);
        String dbName = strings.get(0);
        String scheduleTime = strings.get(1);
        String token = strings.get(2);
        String describe = strings.get(3);
        List<Map<String, String>> taskTriples = lineageService.workflowTriples(sqlContent, filelName);
        WorkflowDefineParam workDefinition = dolphinSchedulerService.createAlertWorkDefinition(projectCode, filelName,sqlContent, dbName, token);
        WorkflowDefineResp workflowDefineResp = dolphinSchedulerService.createWorkflow(projectCode, workDefinition);
        long workflowCode = workflowDefineResp.getCode();

        ScheduleDefineParam scheduleDefineParam = dolphinSchedulerService.createScheduleDefineParam(projectCode, workflowCode, scheduleTime);
        ScheduleInfoResp schedule = dolphinSchedulerService.createSchedule(projectCode, scheduleDefineParam);

        dolphinSchedulerService.onlineWorkflow(projectCode, workflowCode);
        dolphinSchedulerService.onlineSchedule(projectCode, (long) schedule.getId());
        return null;
    }
}
