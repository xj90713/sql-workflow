package com.xiaoxj.sqlworkflow.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xiaoxj.sqlworkflow.domain.WorkflowDependency;
import com.xiaoxj.sqlworkflow.domain.WorkflowDeploy;
import com.xiaoxj.sqlworkflow.domain.TaskStatus;
import com.xiaoxj.sqlworkflow.dolphinscheduler.instance.WorkflowInstanceQueryResp;
import com.xiaoxj.sqlworkflow.repo.WorkflowDependencyRepository;
import com.xiaoxj.sqlworkflow.repo.WorkflowDeployRepository;
import com.xiaoxj.sqlworkflow.repo.TaskStatusRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;

@Service
public class WorkflowOrchestrator {
    private final TaskStatusRepository statusRepo;
    private final WorkflowDependencyRepository depRepo;
    private final WorkflowDeployRepository deployRepo;
    private final DolphinSchedulerService dolphinService;
    private final ObjectMapper mapper = new ObjectMapper();
    @Value("${workflow.maxParallelism:4}")
    private int maxParallelism;

    public WorkflowOrchestrator(TaskStatusRepository statusRepo,
                                WorkflowDependencyRepository depRepo,
                                WorkflowDeployRepository deployRepo,
                                DolphinSchedulerService dolphinService) {
        this.statusRepo = statusRepo;
        this.depRepo = depRepo;
        this.deployRepo = deployRepo;
        this.dolphinService = dolphinService;
    }

    private List<String> parseSources(String s) {
        if (s == null || s.isBlank()) return List.of();
        String[] arr = s.split(",");
        List<String> list = new ArrayList<>();
        for (String x : arr) { String t = x.trim(); if (!t.isEmpty()) list.add(t); }
        return list;
    }

    public void markReady(String tableName) {
        TaskStatus ts = Optional.ofNullable(statusRepo.findTopByTaskNameOrderByUpdatedAtDesc(tableName)).orElse(new TaskStatus());
        ts.setTaskName(tableName);
        ts.setCurrentStatus(TaskStatus.Status.SUCCESS);
        ts.setUpdatedAt(LocalDateTime.now());
        statusRepo.save(ts);
        for (WorkflowDependency d : depRepo.findAll()) {
            List<String> sources = parseSources(d.getSourceTables());
            if (sources.contains(tableName)) {
                boolean allReady = sources.stream().allMatch(src -> {
                    TaskStatus st = statusRepo.findTopByTaskNameOrderByUpdatedAtDesc(src);
                    return st != null && st.getCurrentStatus() == TaskStatus.Status.SUCCESS;
                });
                if (allReady) {
                    TaskStatus pending = Optional.ofNullable(statusRepo.findTopByTaskNameOrderByUpdatedAtDesc(d.getTaskName())).orElse(new TaskStatus());
                    Map<String, String> depMap = new HashMap<>();
                    for (String s : sources) depMap.put(s, "COMPLETED");
                    try { pending.setDependentTables(mapper.writeValueAsString(depMap)); } catch (Exception ignored) {}
                    pending.setTaskName(d.getTaskName());
                    pending.setCurrentStatus(TaskStatus.Status.PENDING);
                    pending.setUpdatedAt(LocalDateTime.now());
                    statusRepo.save(pending);
                }
            }
        }
    }

    @Scheduled(fixedDelayString = "${workflow.triggerIntervalSeconds:60}000")
    @Transactional
    public void triggerPending() {
        List<TaskStatus> pending = statusRepo.findByCurrentStatus(TaskStatus.Status.PENDING);
        int runningCount = statusRepo.findByCurrentStatus(TaskStatus.Status.RUNNING).size();
        int slots = Math.max(0, maxParallelism - runningCount);
        for (TaskStatus t : pending) {
            if (slots <= 0) break;
            WorkflowDeploy deploy = deployRepo.findByTaskName(t.getTaskName());
            if (deploy == null) continue;
//            boolean started = dolphinService.startWorkflow(deploy.getProjectCode(), deploy.getWorkflowCode());
            boolean started = true;
            if (started) {
                t.setCurrentStatus(TaskStatus.Status.RUNNING);
                t.setUpdatedAt(LocalDateTime.now());
                statusRepo.save(t);
                slots--;
            }
        }
    }

    @Scheduled(fixedDelayString = "${workflow.checkIntervalSeconds:30}000")
    @Transactional
    public void checkRunning() {
        List<TaskStatus> running = statusRepo.findByCurrentStatus(TaskStatus.Status.RUNNING);
        for (TaskStatus t : running) {
            WorkflowDeploy deploy = deployRepo.findByTaskName(t.getTaskName());
            if (deploy == null) continue;
            List<WorkflowInstanceQueryResp> instances = dolphinService.listWorkflowInstances(deploy.getProjectCode(), deploy.getWorkflowCode());
            if (instances == null || instances.isEmpty()) continue;
            String state = instances.get(0).getState();
            if ("SUCCESS".equalsIgnoreCase(state)) {
                t.setCurrentStatus(TaskStatus.Status.SUCCESS);
                t.setUpdatedAt(LocalDateTime.now());
                statusRepo.save(t);
                List<WorkflowDependency> deps = depRepo.findByTaskName(t.getTaskName());
                if (deps != null && !deps.isEmpty()) {
                    String targetTable = deps.get(deps.size()-1).getTargetTable();
                    if (targetTable != null && !targetTable.isBlank()) {
                        markReady(targetTable);
                    }
                }
            } else if ("FAIL".equalsIgnoreCase(state) || "FAILED".equalsIgnoreCase(state)) {
                t.setCurrentStatus(TaskStatus.Status.FAILED);
                t.setUpdatedAt(LocalDateTime.now());
                statusRepo.save(t);
            }
        }
    }
}
