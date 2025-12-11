package com.xiaoxj.sqlworkflow.scheduler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xiaoxj.sqlworkflow.domain.WorkflowDependency;
import com.xiaoxj.sqlworkflow.domain.WorkflowDeploy;
import com.xiaoxj.sqlworkflow.domain.TaskStatus;
import com.xiaoxj.sqlworkflow.dolphinscheduler.instance.WorkflowInstanceQueryResp;
import com.xiaoxj.sqlworkflow.remote.HttpRestResult;
import com.xiaoxj.sqlworkflow.repo.WorkflowDependencyRepository;
import com.xiaoxj.sqlworkflow.repo.WorkflowDeployRepository;
import com.xiaoxj.sqlworkflow.repo.TaskStatusRepository;
import com.xiaoxj.sqlworkflow.service.DolphinSchedulerService;
import com.xiaoxj.sqlworkflow.service.WorkflowQueueService;
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
    private final WorkflowQueueService queueService;
    private final ObjectMapper mapper = new ObjectMapper();
    @Value("${workflow.maxParallelism:4}")
    private int maxParallelism;

    public WorkflowOrchestrator(TaskStatusRepository statusRepo,
                                WorkflowDependencyRepository depRepo,
                                WorkflowDeployRepository deployRepo,
                                DolphinSchedulerService dolphinService,
                                WorkflowQueueService queueService) {
        this.statusRepo = statusRepo;
        this.depRepo = depRepo;
        this.deployRepo = deployRepo;
        this.dolphinService = dolphinService;
        this.queueService = queueService;
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
        int runningCount = deployRepo.findByStatus("N").size();
        int slots = Math.max(0, maxParallelism - runningCount);
        if (slots <= 0) return;
        String target = queueService.getTargetWorkflowName();
        if (target == null || target.isBlank()) return;
        WorkflowDeploy deploy = deployRepo.findByTargetTable(target);
        if (deploy == null) return;
        TaskStatus existing = statusRepo.findTopByTaskNameOrderByUpdatedAtDesc(deploy.getTaskName());
        if (existing != null && (existing.getCurrentStatus() == TaskStatus.Status.RUNNING || existing.getCurrentStatus() == TaskStatus.Status.SUCCESS)) {
            return;
        }
        HttpRestResult<JsonNode> result = dolphinService.startWorkflow(deploy.getProjectCode(), deploy.getWorkflowCode());
        if (result != null && result.getSuccess()) {
            TaskStatus ts = Optional.ofNullable(existing).orElse(new TaskStatus());
            ts.setTaskName(deploy.getTaskName());
            ts.setCurrentStatus(TaskStatus.Status.RUNNING);
            ts.setUpdatedAt(LocalDateTime.now());
            statusRepo.save(ts);
            deploy.setStatus("R");
            deploy.setUpdateTime(LocalDateTime.now());
            deployRepo.save(deploy);
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
                WorkflowDeploy d = deployRepo.findByTaskName(t.getTaskName());
                if (d != null) { d.setStatus("Y"); d.setUpdateTime(LocalDateTime.now()); deployRepo.save(d); }
            } else if ("FAIL".equalsIgnoreCase(state) || "FAILED".equalsIgnoreCase(state)) {
                t.setCurrentStatus(TaskStatus.Status.FAILED);
                t.setUpdatedAt(LocalDateTime.now());
                statusRepo.save(t);
            }
        }
    }
}
