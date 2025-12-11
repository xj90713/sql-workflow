package com.xiaoxj.sqlworkflow.scheduler;

import com.fasterxml.jackson.databind.JsonNode;
import com.xiaoxj.sqlworkflow.domain.WorkflowDeploy;
import com.xiaoxj.sqlworkflow.domain.WorkflowInstance;
import com.xiaoxj.sqlworkflow.remote.HttpRestResult;
import com.xiaoxj.sqlworkflow.repo.WorkflowDeployRepository;
import com.xiaoxj.sqlworkflow.repo.WorkflowInstanceRepository;
import com.xiaoxj.sqlworkflow.service.DolphinSchedulerService;
import com.xiaoxj.sqlworkflow.service.WorkflowQueueService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;

@Service
public class WorkflowOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(WorkflowOrchestrator.class);

    private final WorkflowInstanceRepository instanceRepo;
    private final WorkflowDeployRepository deployRepo;
    private final DolphinSchedulerService dolphinService;
    private final WorkflowQueueService queueService;

    @Value("${workflow.maxParallelism:4}")
    private int maxParallelism;

    public WorkflowOrchestrator(WorkflowInstanceRepository instanceRepo,
                                WorkflowDeployRepository deployRepo,
                                DolphinSchedulerService dolphinService,
                                WorkflowQueueService queueService) {
        this.instanceRepo = instanceRepo;
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


    @Scheduled(fixedDelayString = "${workflow.triggerIntervalSeconds:60}000")
    @Transactional
    public void triggerPending() {
        int runningCount = deployRepo.findByStatus("R").size();
        int slots = Math.max(0, maxParallelism - runningCount);
        if (slots <= 0) {
            log.info("Maximum number of tasks executed has been reached.");
            return;
        }
        String target = queueService.getTargetWorkflowName();
        if (Objects.equals(target, "finished")) {
            log.info("All workflows have finished.");
            return;
        }
        if (target == null || target.isBlank()) {
            log.info("No pending workflow found.");
            return;
        };
        WorkflowDeploy deploy = deployRepo.findByTargetTable(target);
        HttpRestResult<JsonNode> result = dolphinService.startWorkflow(deploy.getProjectCode(), deploy.getWorkflowCode());
        if (result != null && result.getSuccess()) {
            Long workflowInstanceId = Long.parseLong(result.getData().get("workflowInstanceId").textValue());
            Long workflowCode = Long.parseLong(result.getData().get("workflowCode").textValue());
            WorkflowInstance instance = new WorkflowInstance();
            instance.setWorkflowCode(workflowCode);
            instance.setProjectCode(deploy.getProjectCode());
            instance.setWorkflowInstanceId(workflowInstanceId);
            instance.setState('R');
            instance.setWorkflowName(target);
            instance.setName(target);
            instanceRepo.save(instance);
            deploy.setStatus("R");
            deploy.setUpdateTime(LocalDateTime.now());
            deployRepo.save(deploy);
        }
    }

    @Scheduled(fixedDelayString = "${workflow.checkIntervalSeconds:30}000")
    @Transactional
    public void checkRunning() {
        List<WorkflowInstance> running = instanceRepo.findByStatus('R');
        if (running.isEmpty()) {
            log.info("No running workflow found.");
            return;
        }
        running.forEach(instance -> {
            Long workflowInstanceId = instance.getWorkflowInstanceId();
            Long workflowCode = instance.getWorkflowCode();
            String status = dolphinService.getWorkflowInstanceStatus(instance.getProjectCode(), workflowInstanceId);
            if (Objects.equals(status, "SUCCESS")) {
                WorkflowDeploy workflowDeploy = deployRepo.findByWorkflowCode(workflowCode);
                workflowDeploy.setStatus("Y");
                instance.setState('Y');
                instanceRepo.save(instance);
                deployRepo.save(workflowDeploy);
            }
        });
    }
}
