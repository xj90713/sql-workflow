package com.xiaoxj.sqlworkflow.service;

import com.xiaoxj.sqlworkflow.domain.TaskDeploy;
import com.xiaoxj.sqlworkflow.domain.TaskDependency;
import com.xiaoxj.sqlworkflow.repo.TaskDeployRepository;
import com.xiaoxj.sqlworkflow.repo.TaskDependencyRepository;
import io.github.reata.sqllineage4j.common.model.Table;
import io.github.reata.sqllineage4j.core.LineageRunner;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.stream.Collectors;
import java.time.LocalDateTime;

@Service
public class SqlLineageService {
    private final TaskDeployRepository deployRepo;
    private final TaskDependencyRepository depRepo;

    public SqlLineageService(TaskDeployRepository deployRepo, TaskDependencyRepository depRepo) {
        this.deployRepo = deployRepo;
        this.depRepo = depRepo;
    }

    @Transactional
    public TaskDependency addTask(String taskName, String filePath, String fileName, String sqlContent, String commitUser) {

        Set<String> sourceTables = new LinkedHashSet<>();
        Set<String> targetTables = new LinkedHashSet<>();

        try {
            LineageRunner runner = LineageRunner.builder(sqlContent).build();
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

        TaskDeploy deploy = new TaskDeploy();
        deploy.setTaskId(taskName);
        deploy.setTaskName(taskName);
        deploy.setFilePath(filePath);
        deploy.setFileName(fileName);
        deploy.setFileContent(sqlContent);
        deploy.setFileMd5(md5(sqlContent));
        deploy.setCommitUser(commitUser);
        deployRepo.save(deploy);

        TaskDependency dep = new TaskDependency();
        dep.setTaskId(taskName);
        dep.setTaskName(taskName);
        dep.setSourceTables(sourceTableStrings);
        dep.setTargetTable(targetTable);
        dep.setStatus("PARSED");
        depRepo.save(dep);


        return dep;
    }

    @Transactional
    public TaskDependency updateTask(String taskName, String filePath, String fileName, String sqlContent, String commitUser) {
        TaskDeploy latest = deployRepo.findTopByTaskNameOrderByUpdateTimeDesc(taskName);
        if (latest == null) {
            return addTask(taskName, filePath, fileName, sqlContent, commitUser);
        }

        String newMd5 = md5(sqlContent);

        Set<String> sourceTables = new LinkedHashSet<>();
        Set<String> targetTables = new LinkedHashSet<>();
        try {
            LineageRunner runner = LineageRunner.builder(sqlContent).build();
            List<Table> sources = runner.sourceTables();
            List<Table> targets = runner.targetTables();
            sources.forEach(table -> sourceTables.add(table.toString().replace("..", ".")));
            targets.forEach(table -> targetTables.add(table.toString().replace("..", ".")));
        } catch (Exception e) {
            throw new IllegalArgumentException("解析 SQL 失败: " + fileName, e);
        }

        String targetTable = targetTables.stream().findFirst().orElseGet(() -> inferTargetFromFilename(fileName));
        String sourceTableStrings = sourceTables.stream().map(t -> t.replace("..", ".")).collect(Collectors.joining(","));

        if (Objects.equals(latest.getFileMd5(), newMd5)) {
            List<TaskDependency> deps = depRepo.findByTaskName(taskName);
            return (deps != null && !deps.isEmpty()) ? deps.get(deps.size() - 1) : null;
        }

        latest.setFilePath(filePath);
        latest.setFileName(fileName);
        latest.setFileContent(sqlContent);
        latest.setFileMd5(newMd5);
        latest.setCommitUser(commitUser);
        latest.setUpdateTime(LocalDateTime.now());
        deployRepo.save(latest);

        List<TaskDependency> deps = depRepo.findByTaskName(taskName);
        TaskDependency dep = (deps != null && !deps.isEmpty()) ? deps.get(deps.size() - 1) : new TaskDependency();
        dep.setTaskCode(taskName);
        dep.setTaskName(taskName);
        dep.setSourceTables(sourceTableStrings);
        dep.setTargetTable(targetTable);
        dep.setStatus("UPDATED");
        return depRepo.save(dep);
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
        // 只处理开头部分的注释行
        boolean startExtracting = false;
        for (String line : lines) {
            // 当遇到第一个 -- 注释行，开始提取
            if (line.trim().startsWith("--")) {
                startExtracting = true;
            }
            // 一旦遇到非注释行，停止提取
            if (startExtracting && !line.trim().startsWith("--")) {
                break;
            }
            // 提取注释
            if (startExtracting && line.trim().startsWith("--")) {
                // 去掉 -- 和 # 后的内容
                String comment = line.replaceAll("^--", "").replaceAll("#", "").trim();
                comments.append(comment).append("\n");
            }
        }

        return comments.toString().trim();
    }
}
