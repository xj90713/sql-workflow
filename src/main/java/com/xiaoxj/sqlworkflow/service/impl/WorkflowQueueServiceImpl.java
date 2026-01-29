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

    /**
     * 获取已经完成的加工表或者采集表以及无需加工的基础维度表
     * @return
     */
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
        for (WorkflowDeploy wd : repo.findByStatusAndScheduleTypeIn('Y', Arrays.asList(0,1, 2))) {
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

    /**
     * 获取下一个加工任务的名称
     * @return
     */
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
            String targetTable = wd.getTargetTable();
            String sourceTables = wd.getSourceTables();
            String dependencies = wd.getDependencies();
            boolean allReady = true;
            if (sourceTables != null && !sourceTables.isBlank()) {
                for (String sourceTable : sourceTables.split(",")) {
                    sourceTable = sourceTable.trim();
                    if (!sourceTable.isEmpty() && !ready.contains(sourceTable)) { allReady = false; break; }
                }
            }
            if (allReady && targetTable != null && !targetTable.isBlank()) {
                log.info("Found a ready workflow: {}", wd.getWorkflowName());
                log.info("Target table: {}", targetTable);
                if (repo.findByTargetTable(targetTable).size() > 1) return wd.getWorkflowName().trim();
                if (!ready.contains(targetTable)) return wd.getWorkflowName().trim();
            }
            if (dependencies != null && !dependencies.isBlank()) {
                for (String dependency : dependencies.split(",")) {
                    char status = repo.findByWorkflowName(dependency).getStatus();
                    if (status != 'Y') break;
                }
                return repo.findByDependencies(dependencies).getFirst().getWorkflowName();
            }
        }
        return null;
    }

    // 获取数据库和表
    @Override
    public String getDbsAndTables() {
        Set<String> dbs = new LinkedHashSet<>();
        Set<String> tables = new LinkedHashSet<>();
        for (WorkflowDeploy wd : repo.findByStatusAndScheduleType('N', 1)) {
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
    public  Set<String> getAffectedTables(String changedTable,
                                                 Map<String, String> dependencies) {
        Set<String> result = new LinkedHashSet<>();
        Set<String> visited = new HashSet<>();

        find(changedTable, dependencies, result, visited);
        return result;
    }

    @Override
    public void find(String currentTable,
                             Map<String, String> deps,
                             Set<String> result,
                             Set<String> visited) {
        if (visited.contains(currentTable)) return;
        visited.add(currentTable);

        for (Map.Entry<String, String> entry : deps.entrySet()) {
            String target = entry.getKey();
            for (String source : entry.getValue().split(",")) {
                if (source.equals(currentTable)) {
                    result.add(target);
                    find(target, deps, result, visited);
                }
            }
        }
    }

}
