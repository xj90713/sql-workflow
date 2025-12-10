package com.xiaoxj.sqlworkflow.controller;

import com.xiaoxj.sqlworkflow.service.IngestInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/ingest")
public class IngestController {
    @Autowired
    private IngestInfoService ingestInfoService;

    @GetMapping("/table")
    public Map<String, Object> query(@RequestParam("source_db") String sourceDb,
                                     @RequestParam("source_tablename") String sourceTablename) {
        List<String> tables = ingestInfoService.findIngestTables(sourceDb, sourceTablename);
        return java.util.Map.of("tables", tables);
    }
}
