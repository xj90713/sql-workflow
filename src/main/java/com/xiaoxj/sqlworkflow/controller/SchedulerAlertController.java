package com.xiaoxj.sqlworkflow.controller;

import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WorkflowDefineParam;
import com.xiaoxj.sqlworkflow.dolphinscheduler.workflow.WorkflowDefineResp;
import com.xiaoxj.sqlworkflow.service.DolphinSchedulerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

@RestController
@Slf4j
public class SchedulerAlertController {

    @Autowired
    private DolphinSchedulerService dolphinSchedulerService;

    @Value("#{${dolphin.project.code}}")
    private Long projectCode;

    @PostMapping("/api/ds/addAlertScheduler")
    public Map<String, Object> addAlertScheduler(@RequestBody Map<String, String> payload) {
        String filelName = payload.get("file_name");
        String content = payload.get("content");
        byte[] decodedBytes = Base64.getDecoder().decode(content);
        String sqlContent = new String(decodedBytes, StandardCharsets.UTF_8);
        String user = payload.getOrDefault("commit_user", "system");
        dolphinSchedulerService.createScheduleDefineParam(projectCode, workflowCode, schedule);
        WorkflowDefineParam workDefinition = dolphinSchedulerService.createSchedule(projectCode,)
        WorkflowDefineResp workflowDefineResp = dolphinSchedulerService.createWorkflow(projectCode, workDefinition);
        long start = System.currentTimeMillis();
        long cost = System.currentTimeMillis() - start;
        log.info("file_name={}, duration_ms={}", fileName, cost);
//        return Map.of("duration_ms", cost, "file_name", fileName, "data", resp);
        return null;
    }

}
