package com.xiaoxj.sqlworkflow.controller;

import com.xiaoxj.sqlworkflow.domain.TaskDependency;
import com.xiaoxj.sqlworkflow.service.DolphinSchedulerService;
import com.xiaoxj.sqlworkflow.service.SqlLineageService;
import com.xiaoxj.sqlworkflow.task.HivecliTask;
import com.xiaoxj.sqlworkflow.util.TaskDefinitionUtils;
import com.xiaoxj.sqlworkflow.util.TaskLocationUtils;
import com.xiaoxj.sqlworkflow.util.TaskRelationUtils;
import com.xiaoxj.sqlworkflow.workflow.TaskDefinition;
import com.xiaoxj.sqlworkflow.workflow.WrokflowDefineParam;
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
        lineageService.addTask(taskName, filePath, fileName, sqlContent, user);
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
                .setTaskDefinitionJson(Arrays.asList(hiveTaskDefinition))
                .setTaskRelationJson(TaskRelationUtils.oneLineRelation(taskCodes.toArray(new Long[0])))
                .setGlobalParams(null);
        hivecliTask.setHiveSqlScript(sqlContent);
        dolphinSchedulerService.createWorkflow(projectCode,pcr);
        return null;
    }

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
        return lineageService.updateTask(taskName, filePath, fileName, sqlContent, user);
    }
}
