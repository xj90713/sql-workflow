package com.xiaoxj.sqlworkflow.controller;

import com.xiaoxj.sqlworkflow.domain.TaskDependency;
import com.xiaoxj.sqlworkflow.service.SqlLineageService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

@RestController
@RequestMapping("/api/dependencies")
public class DependencyController {
    private final SqlLineageService lineageService;
    public DependencyController(SqlLineageService lineageService) { this.lineageService = lineageService; }

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
        return lineageService.addTask(taskName, filePath, fileName, sqlContent, user);
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
