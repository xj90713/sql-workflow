package com.xiaoxj.sqlworkflow.controller;

import com.xiaoxj.sqlworkflow.domain.WorkflowDeploy;
import com.xiaoxj.sqlworkflow.repo.WorkflowDeployRepository;
import com.xiaoxj.sqlworkflow.scheduler.WorkflowOrchestrator;
import com.xiaoxj.sqlworkflow.service.DolphinSchedulerService;
import com.xiaoxj.sqlworkflow.service.SqlLineageService;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WorkflowDefineParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WorkflowDefineResp;
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
public class DependencyController {
    @Autowired
    private SqlLineageService lineageService;

    @Autowired
    private DolphinSchedulerService dolphinSchedulerService;

    @Value("#{${dolphin.project.code}}")
    private Long projectCode;

    @Value("${dolphin.tenant.code}")
    private String tenantCode;

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
        sqlContent = sqlContent.replace("${pt_day}", "'2025-01-01'")
                .replace("${imp_pt_date}", "'2025-01-01'");
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
        lineageService.addWorkflow(workflowName, filePath, fileName, sqlContent, user,workflowCode, projectCode, taskCodesString);
        return null;
    }


    // TODO: 2025/1/1
    // 增加修改任务接口，taskdeploy和taskdependency表 需要增加关联workflowcode 和 taskcode （ds）
    // 每次修改gitlab脚本文件名称以及内容时，需要修改taskdeploy和taskdependency表 以及关联dolphinScheduler里面对应的 workflow 和 task名称信息；
    // 如果是修改脚本文件名称，则需要修改workflow 和task名称（gitlab脚本文件的名称和taskdeploy 名称、ds这边的workflow和task名称都是一一对应的）；
    // 如果是修改脚本文件内容，则需要修改taskdeploy和taskdependency表 以及关联dolphinScheduler里面对应的 workflow 和 task信息；
    // 触发执行，需要调用dolphinScheduler接口，记需要找到workflowcode和taskcode为执行的状态对联，通过接口提交执行任务；
    //
    @PostMapping(value = "/updateWorkflow", consumes = MediaType.APPLICATION_JSON_VALUE)
    public WorkflowDeploy updateWorkflow(@RequestBody Map<String, String> payload) {
        String filePath = payload.get("file_path");
        String workflowName = payload.get("file_path").substring(filePath.lastIndexOf("/") + 1, filePath.lastIndexOf("."));
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        String content = payload.get("content");
        byte[] decodedBytes = Base64.getDecoder().decode(content);
        String sqlContent = new String(decodedBytes, StandardCharsets.UTF_8);
        sqlContent = sqlContent.replace("${pt_day}", "'2025-01-01'")
                .replace("${imp_pt_date}", "'2025-01-01'");
        String user = payload.getOrDefault("commit_user", "system");
        WorkflowDeploy workflowDeploy = deployRepo.findByWorkflowName(workflowName);
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
}
