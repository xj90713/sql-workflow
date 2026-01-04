package com.xiaoxj.sqlworkflow.service.impl;

import com.xiaoxj.sqlworkflow.entity.WorkflowDeploy;
import com.xiaoxj.sqlworkflow.repository.WorkflowDeployRepository;
import com.xiaoxj.sqlworkflow.service.WorkflowQueueService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class WorkflowQueueServiceImpl implements WorkflowQueueService {

    private final WorkflowDeployRepository repo;
    private final IngestInfoServiceImpl ingestInfoService;

    @Override
    public Set<String> buildReadyQueue() {
        String dbsAndTables = getDbsAndTables();
        String dbs;
        String tables;
        if (dbsAndTables != null) {
            dbs = getDbsAndTables().split("-->")[0];
            tables = getDbsAndTables().split("-->")[1];
        } else {
            dbs = null;
            tables = null;
        }
        List<String> ingestTables = ingestInfoService.findIngestTables(dbs, tables);
        Set<String> queue = new LinkedHashSet<>();
        for (WorkflowDeploy wd : repo.findByStatusAndScheduleType('Y', 1)) {
            String tgt = wd.getTargetTable();
            if (tgt != null && !tgt.isBlank()) queue.add(tgt.trim());
        }
        // 合并 ingestTables 到 queue 中
        for (String table : ingestTables) {
            if (table != null && !table.isBlank()) {
                queue.add(table.trim());
            }
        }
        log.info("queue: {} ", queue);
        return queue;
    }

    @Override
    public String getTargetWorkflowName() {
        List<WorkflowDeploy> readyWrokflowList = repo.findByStatusAndScheduleType('N',1);
        log.info("Pending workflows: {}" ,readyWrokflowList.size());
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
    @Override
    public String getDbsAndTables() {
        Set<String> dbs = new LinkedHashSet<>();
        Set<String> tables = new LinkedHashSet<>();
        for (WorkflowDeploy wd : repo.findByStatusAndScheduleType('N', 1)) {
            log.info("Workflow:{} ", wd.getWorkflowName());
            String src = wd.getSourceTables();
            log.info("Source tables:{} ", src);
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
        log.info("Databases:{} ", dbs);
        log.info("Tables:{} ", tables);
        String dbCsv = String.join(",", dbs);
        String tableCsv = String.join(",", tables);
        if (dbCsv.isEmpty() || tableCsv.isEmpty()) {
            return null;
        }
        return dbCsv + "-->" + tableCsv;
    }

    @Override
    public String filterTables(String targetTable, String sourceTables) {
        if (sourceTables == null || sourceTables.trim().isEmpty()) {
            return "";
        }
        if (targetTable == null) {
            targetTable = "";
        }
        String[] tables = sourceTables.split(",");
        String finalTargetTable = targetTable;
        String result = Arrays.stream(tables)
                .map(String::trim)
                .filter(table -> !table.equals(finalTargetTable.trim()))
                .filter(table -> !table.isEmpty())
                .collect(Collectors.joining(","));
        return result;
    }

    @Override
    public  List<String> getAffectedTables(String changedTable,
                                                 Map<String, String[]> dependencies) {
        List<String> result = new ArrayList<>();
        Set<String> visited = new HashSet<>();

        find(changedTable, dependencies, result, visited);
        return result;
    }

    @Override
    public void find(String currentTable,
                             Map<String, String[]> deps,
                             List<String> result,
                             Set<String> visited) {
        if (visited.contains(currentTable)) return;
        visited.add(currentTable);

        for (Map.Entry<String, String[]> entry : deps.entrySet()) {
            String target = entry.getKey();
            for (String source : entry.getValue()) {
                if (source.equals(currentTable)) {
                    result.add(target);
                    find(target, deps, result, visited);
                }
            }
        }
    }

}
