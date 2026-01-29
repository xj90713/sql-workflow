package com.xiaoxj.sqlworkflow.service.impl;

import com.xiaoxj.sqlworkflow.common.utils.TextUtils;
import com.xiaoxj.sqlworkflow.entity.AlertWorkflowDeploy;
import com.xiaoxj.sqlworkflow.entity.WorkflowDeploy;
import com.xiaoxj.sqlworkflow.repository.AlertWorkflowDeployRepository;
import com.xiaoxj.sqlworkflow.repository.WorkflowDeployRepository;
import com.xiaoxj.sqlworkflow.service.SqlLineageService;
import io.github.reata.sqllineage4j.common.model.Table;
import io.github.reata.sqllineage4j.core.LineageRunner;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class SqlLineageServiceImpl implements SqlLineageService {


    private final WorkflowDeployRepository deployRepo;

    private final AlertWorkflowDeployRepository alertDeployRepo;


    @Override
    @Transactional
    public WorkflowDeploy addWorkflowDeploy(String workflowName, String filePath, String fileName, String sqlContent, String commitUser, long workflowCode, long projectCode, String taskCodes) {

        WorkflowDeploy deploy = new WorkflowDeploy();
        deploy.setWorkflowId(workflowName);
        deploy.setWorkflowName(workflowName);
        deploy.setFilePath(filePath);
        deploy.setFileName(fileName);
        deploy.setSourceTables(getTargetAndSourceTablesOrDepedencies(sqlContent, fileName).get("sourceTables"));
        deploy.setTargetTable(getTargetAndSourceTablesOrDepedencies(sqlContent, fileName).get("targetTable"));
        deploy.setDependencies(getTargetAndSourceTablesOrDepedencies(sqlContent, fileName).get("dependencies"));
        deploy.setFileContent(sqlContent);
        deploy.setFileMd5(TextUtils.md5(sqlContent));
        deploy.setCommitUser(commitUser);
        deploy.setTaskCodes(taskCodes);
        deploy.setWorkflowCode(workflowCode);
        deploy.setProjectCode(projectCode);
        if (filePath.contains("shell") || filePath.contains("sh")) {
            deploy.setScheduleType(0);
        }
        deploy.setStatus('N');
        deployRepo.save(deploy);
        return deploy;
    }

    @Override
    @Transactional
    public WorkflowDeploy updateWorkflowDeploy(String workflowName, String filePath, String fileName, String sqlContent, String commitUser, long workflowCode, long projectCode, String taskCodes) {
        WorkflowDeploy latest = deployRepo.findTopByWorkflowNameOrderByUpdateTimeDesc(workflowName);
        if (latest == null) {
            return addWorkflowDeploy(workflowName, filePath, fileName, sqlContent, commitUser, workflowCode, projectCode, taskCodes);
        }

        String newMd5 = TextUtils.md5(sqlContent);
        latest.setFilePath(filePath);
        latest.setFileName(fileName);
        latest.setFileContent(sqlContent);
        latest.setFileMd5(newMd5);
        latest.setCommitUser(commitUser);
        latest.setSourceTables(getTargetAndSourceTablesOrDepedencies(sqlContent, fileName).get("sourceTables"));
        latest.setTargetTable(getTargetAndSourceTablesOrDepedencies(sqlContent, fileName).get("targetTable"));
        latest.setDependencies(getTargetAndSourceTablesOrDepedencies(sqlContent,fileName).get("dependencies"));
        latest.setUpdateTime(LocalDateTime.now());
        latest.setTaskCodes(taskCodes);
        deployRepo.save(latest);
        return latest;
    }

    @Override
    public  Map<String, String> getTargetAndSourceTablesOrDepedencies(String sqlContent, String fileName) {
        Set<String> sourceTables = new LinkedHashSet<>();
        Set<String> targetTables = new LinkedHashSet<>();
        Set<String> dependencies ;
        boolean isShellFile = fileName.contains("shell") || fileName.contains("sh");
        boolean hasKeywords = sqlContent.contains("target_tables") ||
                sqlContent.contains("source_tables") ||
                sqlContent.contains("dependencies");

        if (isShellFile && !hasKeywords && !sqlContent.contains("values(1)")) {
            sqlContent = TextUtils.extractSql(sqlContent);
        }
        try {
            String fixSqlContent = sqlContent.replace("${pt_day}", "'2025-01-01'")
                    .replace("${imp_pt_day}", "'2025-01-01'");
            LineageRunner runner = LineageRunner.builder(fixSqlContent).build();
            List<Table> sources = runner.sourceTables();
            List<Table> targets = runner.targetTables();
            List<Table> intermediateTables = runner.intermediateTables();
            if (!intermediateTables.isEmpty()) {
                targets.addAll(intermediateTables);
            }
            for (Table table : sources) {
                String tableName = table.toString().replace("..", ".");
                sourceTables.add(tableName);
            }
            targets.forEach(table -> targetTables.add(table.toString().replace("..", ".")));
            Set<String> extractedSourceTables = TextUtils.getTablesOrDependencies(sqlContent, "source_tables");
            dependencies = TextUtils.getTablesOrDependencies(sqlContent, "dependencies");
            if (!extractedSourceTables.isEmpty()) {
                sourceTables = extractedSourceTables;// 替换整个集合
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("解析 SQL 失败: " + fileName, e);
        }

        String targetTable = targetTables.stream()
                .map(table -> table.replace("..", "."))
                .map(table -> table.replaceFirst("^hive\\.", ""))
//                .findFirst()
//                .orElseGet(() -> TextUtils.inferTargetFromFilename(fileName));
                .collect(Collectors.joining(","));
        String sourceTableStrings = sourceTables.stream()
                .map(table -> table.replace("..", "."))
                .map(table -> table.replaceFirst("^hive\\.", ""))
                .filter(t -> !t.contains(targetTable))
                .collect(Collectors.joining(","));
        String dependencyStrings = String.join(",", dependencies);
        return Map.of("sourceTables", sourceTableStrings, "targetTable", targetTable, "dependencies", dependencyStrings);
    }

    @Override
    @Transactional
    public AlertWorkflowDeploy addAlertWorkflowDeploy(String workflowName, String filePath, String fileName, String sqlContent, String commitUser, String taskCodes,long scheduleId, long workflowCode, long projectCode, String scheduleTime) {
        AlertWorkflowDeploy deploy = new AlertWorkflowDeploy();
        deploy.setWorkflowId(workflowName);
        deploy.setWorkflowName(workflowName);
        deploy.setFilePath(filePath);
        deploy.setFileName(fileName);
        deploy.setScheduleId(scheduleId);
        deploy.setFileContent(sqlContent);
        deploy.setFileMd5(TextUtils.md5(sqlContent));
        deploy.setCommitUser(commitUser);
        deploy.setTaskCodes(taskCodes);
        deploy.setWorkflowCode(workflowCode);
        deploy.setProjectCode(projectCode);
        deploy.setStatus('N');
        deploy.setCrontab(scheduleTime);
        alertDeployRepo.save(deploy);
        return deploy;
    }

    @Override
    @Transactional
    public AlertWorkflowDeploy updateAlertWorkflowDeploy(String workflowName, String filePath, String fileName, String sqlContent, String commitUser, String taskCodes,long scheduleId,long workflowCode, long projectCode, String scheduleTime) {
        AlertWorkflowDeploy latest = alertDeployRepo.findTopByWorkflowNameOrderByUpdateTimeDesc(workflowName);

        if (latest == null) {
            return addAlertWorkflowDeploy(workflowName, filePath, fileName, sqlContent, commitUser,taskCodes,scheduleId, workflowCode, projectCode, scheduleTime);
        }

        String newMd5 = TextUtils.md5(sqlContent);
        latest.setWorkflowId(workflowName);
        latest.setWorkflowName(workflowName);
        latest.setFilePath(filePath);
        latest.setFileName(fileName);
        latest.setScheduleId(scheduleId);
        latest.setFileContent(sqlContent);
        latest.setFileMd5(TextUtils.md5(sqlContent));
        latest.setCommitUser(commitUser);
        latest.setTaskCodes(taskCodes);
        latest.setFileMd5(newMd5);
        latest.setUpdateTime(LocalDateTime.now());
        latest.setWorkflowCode(workflowCode);
        latest.setProjectCode(projectCode);
        latest.setStatus('N');
        latest.setCrontab(scheduleTime);
        alertDeployRepo.save(latest);
        return latest;
    }
}
