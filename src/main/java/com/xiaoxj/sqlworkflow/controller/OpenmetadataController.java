package com.xiaoxj.sqlworkflow.controller;

import com.xiaoxj.sqlworkflow.service.OpenmetadataService;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

@RestController
@Slf4j
public class OpenmetadataController {

    @Autowired
    private OpenmetadataService openmetadataService;

    @PostMapping("/api/openmetadata/lineage")
    public Map<String, Object> getSqlLineage(@RequestBody Map<String, String> payload) {
        String filePath = payload.get("file_path");
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        String content = payload.get("content");
        byte[] decodedBytes = Base64.getDecoder().decode(content);
        String sqlContent = new String(decodedBytes, StandardCharsets.UTF_8);
        sqlContent = sqlContent.replace("${pt_day}", "'2025-01-01'")
                .replace("${imp_pt_date}", "'2025-01-01'");
        long start = System.currentTimeMillis();
        String resp = openmetadataService.getSqlLineage(fileName, sqlContent);
        long cost = System.currentTimeMillis() - start;
        log.info("file_name={}, duration_ms={}", fileName, cost);
        return Map.of("duration_ms", cost, "file_name", fileName, "data", resp);
    }
}
