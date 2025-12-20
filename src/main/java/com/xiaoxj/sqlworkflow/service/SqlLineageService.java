package com.xiaoxj.sqlworkflow.service;

import com.xiaoxj.sqlworkflow.domain.AlertWorkflowDeploy;
import com.xiaoxj.sqlworkflow.domain.WorkflowDeploy;
import com.xiaoxj.sqlworkflow.domain.WorkflowInstance;
import com.xiaoxj.sqlworkflow.repo.AlertWorkflowDeployRepository;
import com.xiaoxj.sqlworkflow.repo.WorkflowDeployRepository;
import com.xiaoxj.sqlworkflow.repo.WorkflowInstanceRepository;
import io.github.reata.sqllineage4j.common.model.Table;
import io.github.reata.sqllineage4j.core.LineageRunner;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.time.LocalDateTime;

@Service
public class SqlLineageService {
    private final WorkflowDeployRepository deployRepo;
    private final WorkflowInstanceRepository workflowInstanceRepo;

    private final AlertWorkflowDeployRepository alertDeployRepo;

    public SqlLineageService(WorkflowDeployRepository deployRepo, WorkflowInstanceRepository workflowInstanceRepo, AlertWorkflowDeployRepository alertDeployRepo) {
        this.deployRepo = deployRepo;
        this.workflowInstanceRepo = workflowInstanceRepo;
        this.alertDeployRepo = alertDeployRepo;
    }

    @Transactional
    public WorkflowDeploy addWorkflowDeploy(String workflowName, String filePath, String fileName, String sqlContent, String commitUser, long workflowCode, long projectCode, String taskCodes) {

        Set<String> sourceTables = new LinkedHashSet<>();
        Set<String> targetTables = new LinkedHashSet<>();

        try {
            String fixSqlContent = sqlContent.replace("${pt_day}", "'2025-01-01'")
                .replace("${imp_pt_day}", "'2025-01-01'");
            LineageRunner runner = LineageRunner.builder(fixSqlContent).build();
            List<Table> sources = runner.sourceTables();
            List<Table> targets = runner.targetTables();

            sources.forEach(table -> sourceTables.add(table.toString().replace("..", ".")));
            targets.forEach(table -> targetTables.add(table.toString().replace("..", ".")));
        } catch (Exception e) {
            throw new IllegalArgumentException("解析 SQL 失败: " + fileName, e);
        }

        String targetTable = targetTables.stream()
                .findFirst()
                .orElseGet(() -> inferTargetFromFilename(fileName));
        String sourceTableStrings = sourceTables.stream()
                .map(table -> table.replace("..", "."))
                .collect(Collectors.joining(","));

        WorkflowDeploy deploy = new WorkflowDeploy();
        deploy.setWorkflowId(workflowName);
        deploy.setWorkflowName(workflowName);
        deploy.setFilePath(filePath);
        deploy.setFileName(fileName);
        deploy.setSourceTables(sourceTableStrings);
        deploy.setTargetTable(targetTable);
        deploy.setFileContent(sqlContent);
        deploy.setFileMd5(md5(sqlContent));
        deploy.setCommitUser(commitUser);
        deploy.setTaskCodes(taskCodes);
        deploy.setWorkflowCode(workflowCode);
        deploy.setProjectCode(projectCode);
        deploy.setStatus('N');
        deployRepo.save(deploy);
        return deploy;
    }

    @Transactional
    public WorkflowDeploy updateWorkflowDeploy(String workflowName, String filePath, String fileName, String sqlContent, String commitUser, String taskCodes, long workflowCode, long projectCode) {
        WorkflowDeploy latest = deployRepo.findTopByWorkflowNameOrderByUpdateTimeDesc(workflowName);
        if (latest == null) {
            return addWorkflowDeploy(workflowName, filePath, fileName, sqlContent, commitUser, workflowCode, projectCode, taskCodes);
        }

        String newMd5 = md5(sqlContent);

        Set<String> sourceTables = new LinkedHashSet<>();
        Set<String> targetTables = new LinkedHashSet<>();
        try {
            String fixSqlContent = sqlContent.replace("${pt_day}", "'2025-01-01'")
                    .replace("${imp_pt_day}", "'2025-01-01'");
            LineageRunner runner = LineageRunner.builder(fixSqlContent).build();
            List<Table> sources = runner.sourceTables();
            List<Table> targets = runner.targetTables();
            sources.forEach(table -> sourceTables.add(table.toString().replace("..", ".")));
            targets.forEach(table -> targetTables.add(table.toString().replace("..", ".")));
        } catch (Exception e) {
            throw new IllegalArgumentException("解析 SQL 失败: " + fileName, e);
        }

        String targetTable = targetTables.stream().findFirst().orElseGet(() -> inferTargetFromFilename(fileName));
        String sourceTableStrings = sourceTables.stream().map(t -> t.replace("..", ".")).collect(Collectors.joining(","));

        latest.setFilePath(filePath);
        latest.setFileName(fileName);
        latest.setFileContent(sqlContent);
        latest.setFileMd5(newMd5);
        latest.setCommitUser(commitUser);
        latest.setSourceTables(sourceTableStrings);
        latest.setTargetTable(targetTable);
        latest.setUpdateTime(LocalDateTime.now());
        latest.setTaskCodes(taskCodes);
        deployRepo.save(latest);
        return latest;
    }

    @Transactional
    public WorkflowInstance addWorkflowInstance(String name, String fileName, String sqlContent, char state, long workflowCode, long projectCode, String taskCodes) {
        WorkflowInstance workflowInstance = new WorkflowInstance();
        workflowInstance.setName(name);
        workflowInstance.setWorkflowCode(workflowCode);
        workflowInstance.setProjectCode(projectCode);
        workflowInstance.setStatus(state);
        workflowInstance.setRunTimes(1);
        return workflowInstanceRepo.save(workflowInstance);
    }

    @Transactional
    public AlertWorkflowDeploy addAlertWorkflowDeploy(String workflowName, String filePath, String fileName, String sqlContent, String commitUser, String taskCodes,long schedulerId, long workflowCode, long projectCode) {
        AlertWorkflowDeploy deploy = new AlertWorkflowDeploy();
        deploy.setWorkflowId(workflowName);
        deploy.setWorkflowName(workflowName);
        deploy.setFilePath(filePath);
        deploy.setFileName(fileName);
        deploy.setSchedulerId(schedulerId);
        deploy.setFileContent(sqlContent);
        deploy.setFileMd5(md5(sqlContent));
        deploy.setCommitUser(commitUser);
        deploy.setTaskCodes(taskCodes);
        deploy.setWorkflowCode(workflowCode);
        deploy.setProjectCode(projectCode);
        deploy.setStatus('N');
        alertDeployRepo.save(deploy);
        return deploy;
    }

    @Transactional
    public AlertWorkflowDeploy updateAlertWorkflowDeploy(String workflowName, String filePath, String fileName, String sqlContent, String commitUser, String taskCodes,long schedulerId,long workflowCode, long projectCode) {
        AlertWorkflowDeploy latest = alertDeployRepo.findTopByWorkflowNameOrderByUpdateTimeDesc(workflowName);
        if (latest == null) {
            return addAlertWorkflowDeploy(workflowName, filePath, fileName, sqlContent, commitUser,taskCodes,schedulerId, workflowCode, projectCode);
        }

        String newMd5 = md5(sqlContent);
        AlertWorkflowDeploy deploy = new AlertWorkflowDeploy();
        deploy.setWorkflowId(workflowName);
        deploy.setWorkflowName(workflowName);
        deploy.setFilePath(filePath);
        deploy.setFileName(fileName);
        deploy.setSchedulerId(schedulerId);
        deploy.setFileContent(sqlContent);
        deploy.setFileMd5(md5(sqlContent));
        deploy.setCommitUser(commitUser);
        deploy.setTaskCodes(taskCodes);
        deploy.setFileMd5(newMd5);
        deploy.setWorkflowCode(workflowCode);
        deploy.setProjectCode(projectCode);
        deploy.setStatus('N');
        alertDeployRepo.save(deploy);
        return deploy;
    }
    @Transactional
    public WorkflowInstance updateWorkflowInstance(WorkflowInstance workflowInstance, char state) {
        workflowInstance.setStatus(state);
        return workflowInstanceRepo.save(workflowInstance);
    }

    private String md5(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) { throw new RuntimeException(e); }
    }
    private String inferTargetFromFilename(String filename) {
        int dot = filename.lastIndexOf('.');
        return dot > 0 ? filename.substring(0, dot) : filename;
    }

    public String extractComments(String sql) {
        String[] lines = sql.split("\n");
        StringBuilder comments = new StringBuilder();
        boolean startExtracting = false;
        for (String line : lines) {
            if (line.trim().startsWith("--")) {
                startExtracting = true;
            }
            if (startExtracting && !line.trim().startsWith("--")) {
                break;
            }
            if (startExtracting && line.trim().startsWith("--")) {
                String comment = line.replaceAll("^--", "").replaceAll("#", "").trim();
                comments.append(comment).append("\n");
            }
        }
        return comments.toString().trim();
    }


    public List<Map<String, String>> workflowTriples(String sql, String workflowName) {
        ArrayList<Integer> sepStarts = new ArrayList<>();
        ArrayList<Integer> sepEnds = new ArrayList<>();
        ArrayList<String> types = new ArrayList<>();
        Pattern p = Pattern.compile("(?m)^\\s*---\\s*([a-zA-Z0-9_]+)\\s*---\\s*$");
        Matcher m = p.matcher(sql);
        while (m.find()) {
            sepStarts.add(m.start());
            sepEnds.add(m.end());
            types.add(m.group(1));
        }
        List<Map<String, String>> res = new ArrayList<>();
        if (types.isEmpty()) {
            Map<String, String> t = new LinkedHashMap<>();
            t.put("task_name", workflowName);
            t.put("task_type", "hive");
            t.put("task_content", sql.trim());
            res.add(t);
            return res;
        }
        for (int i = 0; i < types.size(); i++) {
            int contentStart = sepEnds.get(i);
            int contentEnd = (i + 1 < sepStarts.size()) ? sepStarts.get(i + 1) : sql.length();
            String content = sql.substring(contentStart, contentEnd).trim();
            Map<String, String> t = new LinkedHashMap<>();
            t.put("task_name", workflowName + (i + 1));
            t.put("task_type", types.get(i) == null ? "hive" : types.get(i).toLowerCase());
            t.put("task_content", content);
            res.add(t);
        }
        return res;
    }

    public String replaceSqlContent(String sqlContent) {

        sqlContent = sqlContent.replace("${pt_day}", "'2025-01-01'")
                .replace("${imp_pt_day}", "'2025-01-01'");
        return sqlContent;
    }
}
