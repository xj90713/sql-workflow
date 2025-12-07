package com.xiaoxj.sqlworkflow.controller;

import com.xiaoxj.sqlworkflow.domain.TaskDependency;
import com.xiaoxj.sqlworkflow.domain.TaskDeploy;
import com.xiaoxj.sqlworkflow.repo.TaskDeployRepository;
import com.xiaoxj.sqlworkflow.service.DolphinSchedulerService;
import com.xiaoxj.sqlworkflow.service.SqlLineageService;
import com.xiaoxj.sqlworkflow.task.HivecliTask;
import com.xiaoxj.sqlworkflow.util.TaskDefinitionUtils;
import com.xiaoxj.sqlworkflow.util.TaskLocationUtils;
import com.xiaoxj.sqlworkflow.util.TaskRelationUtils;
import com.xiaoxj.sqlworkflow.workflow.TaskDefinition;
import com.xiaoxj.sqlworkflow.workflow.WrokflowDefineParam;
import com.xiaoxj.sqlworkflow.workflow.WrokflowDefineResp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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

    @Value("${dolphin.project.code}")
    private Long projectCode;

    @Value("${dolphin.tenant.code}")
    private String tenantCode;

    @Autowired
    private TaskDeployRepository deployRepo;
    @PostMapping(value = "/addTask", consumes = MediaType.APPLICATION_JSON_VALUE)
    public TaskDependency addTask(@RequestBody Map<String, String> payload) {
        String filePath = payload.get("file_path");
        String taskName = payload.get("file_path").substring(filePath.lastIndexOf("/") + 1, filePath.lastIndexOf("."));
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        String content = payload.get("content");
        byte[] decodedBytes = Base64.getDecoder().decode(content);
        String sqlContent = new String(decodedBytes, StandardCharsets.UTF_8);
        sqlContent = sqlContent.replace("${pt_day}", "'2025-01-01'")
                .replace("${imp_pt_date}", "'2025-01-01'");
        String user = payload.getOrDefault("commit_user", "system");
        List<Long> taskCodes = dolphinSchedulerService.generateTaskCodes(projectCode, 1);
        HivecliTask hivecliTask = new HivecliTask();

        String describe = lineageService.extractComments(sqlContent);
        TaskDefinition hiveTaskDefinition =
                TaskDefinitionUtils.createDefaultTaskDefinition(taskCodes.get(0), hivecliTask, describe);
        WrokflowDefineParam pcr = new WrokflowDefineParam();
        pcr.setName(taskName)
                .setLocations(TaskLocationUtils.horizontalLocation(taskCodes.toArray(new Long[0])))
                .setDescription(fileName)
                .setTenantCode(tenantCode)
                .setTimeout("0")
                .setExecutionType(WrokflowDefineParam.EXECUTION_TYPE_PARALLEL)
                .setTaskDefinitionJson(List.of(hiveTaskDefinition))
                .setTaskRelationJson(TaskRelationUtils.oneLineRelation(taskCodes.toArray(new Long[0])))
                .setGlobalParams(null);
        hivecliTask.setHiveSqlScript(sqlContent);
        WrokflowDefineResp wrokflowDefineResp = dolphinSchedulerService.createWorkflow(projectCode, pcr);
        long workflowCode = wrokflowDefineResp.getCode();
        // 创建任务流之后 需要上线该任务
        dolphinSchedulerService.onlineWorkflow(projectCode, workflowCode);

        long taskCode = taskCodes.get(0);
        long projectCode = wrokflowDefineResp.getProjectCode();
        lineageService.addTask(taskName, filePath, fileName, sqlContent, user, projectCode, workflowCode, taskCode);

        return null;
    }


    // TODO: 2025/1/1
    // 增加修改任务接口，taskdeploy和taskdependency表 需要增加关联workflowcode 和 taskcode （ds）
    // 每次修改gitlab脚本文件名称以及内容时，需要修改taskdeploy和taskdependency表 以及关联dolphinScheduler里面对应的 workflow 和 task名称信息；
    // 如果是修改脚本文件名称，则需要修改workflow 和task名称（gitlab脚本文件的名称和taskdeploy 名称、ds这边的workflow和task名称都是一一对应的）；
    // 如果是修改脚本文件内容，则需要修改taskdeploy和taskdependency表 以及关联dolphinScheduler里面对应的 workflow 和 task信息；
    // 触发执行，需要调用dolphinScheduler接口，记需要找到workflowcode和taskcode为执行的状态对联，通过接口提交执行任务；
    //
    @PostMapping(value = "/updateTask", consumes = MediaType.APPLICATION_JSON_VALUE)
    public TaskDependency updateTask(@RequestBody Map<String, String> payload) {
        String filePath = payload.get("file_path");
        String taskName = payload.get("file_path").substring(filePath.lastIndexOf("/") + 1, filePath.lastIndexOf("."));
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        String content = payload.get("content");
        byte[] decodedBytes = Base64.getDecoder().decode(content);
        String sqlContent = new String(decodedBytes, StandardCharsets.UTF_8);
        sqlContent = sqlContent.replace("${pt_day}", "'2025-01-01'")
                .replace("${imp_pt_date}", "'2025-01-01'");
        String user = payload.getOrDefault("commit_user", "system");
        TaskDeploy taskDeploy = deployRepo.findByTaskName(taskName);
        long taskCode = taskDeploy.getTaskCode();
        long workflowCode = taskDeploy.getWorkflowCode();
        long projectCode = taskDeploy.getProjectCode();
        HivecliTask hivecliTask = new HivecliTask();
        hivecliTask.setHiveSqlScript(sqlContent);
        String describe = lineageService.extractComments(sqlContent);
        // 更新工作流之前，必须要下线改任务
        dolphinSchedulerService.offlineWorkflow(projectCode, workflowCode);
        TaskDefinition hiveTaskDefinition =
                TaskDefinitionUtils.createDefaultTaskDefinition(taskCode, hivecliTask, describe);
        WrokflowDefineParam pcr = new WrokflowDefineParam();
        pcr.setName(taskName)
                .setLocations(TaskLocationUtils.horizontalLocation(new Long[]{taskCode}))
                .setDescription(fileName)
                .setTenantCode(tenantCode)
                .setTimeout("0")
                .setExecutionType(WrokflowDefineParam.EXECUTION_TYPE_PARALLEL)
                .setTaskDefinitionJson(List.of(hiveTaskDefinition))
                .setTaskRelationJson(TaskRelationUtils.oneLineRelation(new Long[]{taskCode}))
                .setGlobalParams(null);
        hivecliTask.setHiveSqlScript(sqlContent);
        WrokflowDefineResp wrokflowDefineResp = dolphinSchedulerService.updateWorkflow(projectCode, workflowCode, pcr);
        // 更新工作流之后，在上线工作流
        dolphinSchedulerService.onlineWorkflow(projectCode, workflowCode);
        return lineageService.updateTask(taskName, filePath, fileName, sqlContent, user, taskCode, workflowCode, projectCode);
    }
}
