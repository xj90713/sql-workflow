package com.xiaoxj.sqlworkflow.service;

import com.xiaoxj.sqlworkflow.domain.WorkflowDeploy;
import com.xiaoxj.sqlworkflow.repo.WorkflowDeployRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class WorkflowQueueService {

    private static final Logger log = LoggerFactory.getLogger(WorkflowQueueService.class);

    private final WorkflowDeployRepository repo;
    private final IngestInfoService ingestInfoService;
    public WorkflowQueueService(WorkflowDeployRepository repo, IngestInfoService ingestInfoService) { this.repo = repo;
        this.ingestInfoService = ingestInfoService;
    }


    public Set<String> buildReadyQueue() {
        String dbs = getDbsAndTables().split("-->")[0];
        String tables = getDbsAndTables().split("-->")[1];
        List<String> ingestTables = ingestInfoService.findIngestTables(dbs, tables);
        Set<String> queue = new LinkedHashSet<>();
        for (WorkflowDeploy wd : repo.findByStatus('Y')) {
            String tgt = wd.getTargetTable();
            if (tgt != null && !tgt.isBlank()) queue.add(tgt.trim());
        }
        // 合并 ingestTables 到 queue 中
        for (String table : ingestTables) {
            if (table != null && !table.isBlank()) {
                queue.add(table.trim());
            }
        }
        return queue;
    }
    public String getTargetWorkflowName() {
        List<WorkflowDeploy> pendings = repo.findByStatus('N');
        if (pendings.isEmpty()) {
            log.info("No pending workflow found, all workflows have finished");
            return "finished";
        }
        Set<String> ready = buildReadyQueue();
        for (WorkflowDeploy wd : pendings) {
            String tgt = wd.getTargetTable();
            String src = wd.getSourceTables();
            boolean allReady = true;
            if (src != null && !src.isBlank()) {
                for (String s : src.split(",")) {
                    String t = s.trim();
                    if (!t.isEmpty() && !ready.contains(t)) { allReady = false; break; }
                }
            }
            if (allReady && tgt != null && !tgt.isBlank() && !ready.contains(tgt)) return tgt.trim();
        }
        return null;
    }

    public String getDbsAndTables() {
        Set<String> dbs = new LinkedHashSet<>();
        Set<String> tables = new LinkedHashSet<>();
        for (WorkflowDeploy wd : repo.findByStatus('N')) {
            String src = wd.getSourceTables();
            if (src == null || src.isBlank()) continue;
            for (String s : src.split(",")) {
                String t = s.trim();
                if (t.isEmpty()) continue;
                String lower = t.toLowerCase();
                if (!lower.startsWith("ods.")) continue;
                String[] parts = t.split("\\.");
                if (parts.length >= 3) {
                    dbs.add(parts[1]);
                    tables.add(parts[2]);
                }
            }
        }
        String dbCsv = String.join(",", dbs);
        String tableCsv = String.join(",", tables);
        return dbCsv + "-->" + tableCsv;
    }
}
