package com.xiaoxj.sqlworkflow.service;

import com.xiaoxj.sqlworkflow.domain.WorkflowDeploy;
import com.xiaoxj.sqlworkflow.repo.WorkflowDeployRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@Slf4j
public class WorkflowQueueService {

    private final WorkflowDeployRepository repo;
    private final IngestInfoService ingestInfoService;
    public WorkflowQueueService(WorkflowDeployRepository repo, IngestInfoService ingestInfoService) { this.repo = repo;
        this.ingestInfoService = ingestInfoService;
    }

    public Set<String> buildReadyQueue() {
        String dbsAndTables = getDbsAndTables();
        if (dbsAndTables == null || dbsAndTables.isBlank()) {
            log.info("No pending workflows found, ready queue is empty");
            return Collections.emptySet();
        }
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
        List<WorkflowDeploy> readyWrokflowList = repo.findByStatus('N');
        System.out.println("Pending workflows: " + readyWrokflowList.size());
        Set<String> ready = buildReadyQueue();
        if (readyWrokflowList.isEmpty() || ready.isEmpty()) {
            log.info("No pending workflow found, all workflows have finished");
            return "finished";
        }
        for (WorkflowDeploy wd : readyWrokflowList) {
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

    // 获取数据库和表
    public String getDbsAndTables() {
        System.out.println("Getting databases and tables...");
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
                if (parts.length >= 2) {
                    dbs.add(parts[0]);
                    tables.add(parts[1]);
                }
            }
        }
        String dbCsv = String.join(",", dbs);
        String tableCsv = String.join(",", tables);
        if (dbCsv.isEmpty() || tableCsv.isEmpty()) {
            return null;
        }
        return dbCsv + "-->" + tableCsv;
    }
}
