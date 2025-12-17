package com.xiaoxj.sqlworkflow.scheduler;

import com.fasterxml.jackson.databind.JsonNode;
import com.xiaoxj.sqlworkflow.domain.WorkflowDeploy;
import com.xiaoxj.sqlworkflow.domain.WorkflowInstance;
import com.xiaoxj.sqlworkflow.remote.HttpRestResult;
import com.xiaoxj.sqlworkflow.repo.WorkflowDeployRepository;
import com.xiaoxj.sqlworkflow.repo.WorkflowInstanceRepository;
import com.xiaoxj.sqlworkflow.service.DolphinSchedulerService;
import com.xiaoxj.sqlworkflow.service.WorkflowQueueService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;

@Component
@RequiredArgsConstructor
public class WorkflowOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(WorkflowOrchestrator.class);

    private final WorkflowInstanceRepository instanceRepo;
    private final WorkflowDeployRepository deployRepo;
    private final DolphinSchedulerService dolphinService;
    private final WorkflowQueueService queueService;

    @Value("${workflow.maxParallelism:16}")
    private int maxParallelism;

    @Value("${workflow.schedule.enabled}")
    private boolean scheduleEnabled;


    private List<String> parseSources(String s) {
        if (s == null || s.isBlank()) return List.of();
        String[] arr = s.split(",");
        List<String> list = new ArrayList<>();
        for (String x : arr) { String t = x.trim(); if (!t.isEmpty()) list.add(t); }
        return list;
    }


    @Scheduled(cron = "${workflow.schedule.triggerPending}")
    @Async("taskExecutor")
    public void triggerPending() {
        if (!scheduleEnabled) {
            log.warn("close workflow orchestrator schedule.");
            return;
        }
        int runningCount = deployRepo.findByStatus('R').size();
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
            Long workflowInstanceId = Long.parseLong(result.getData().get("workflowInstanceId").toString());
            Long workflowCode = Long.parseLong(result.getData().get("workflowCode").toString());
            WorkflowInstance instance = new WorkflowInstance();
            instance.setWorkflowCode(workflowCode);
            instance.setProjectCode(deploy.getProjectCode());
            instance.setWorkflowInstanceId(workflowInstanceId);
            instance.setStatus('R');
            instance.setWorkflowName(target);
            instance.setName(target);
            instance.setStartTime(LocalDateTime.now());
            instanceRepo.save(instance);
            deploy.setStatus('R');
            deploy.setUpdateTime(LocalDateTime.now());
            deployRepo.save(deploy);
        }
    }
    @Scheduled(cron = "${workflow.schedule.checkRunning}")
    @Async("taskExecutor")
    public void checkRunning() {
        if (!scheduleEnabled) {
            log.warn("close workflow orchestrator schedule.");
            return;
        }
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
                workflowDeploy.setStatus('Y');
                workflowDeploy.setUpdateTime(LocalDateTime.now());
                instance.setStatus('Y');
                instance.setFinishTime(LocalDateTime.now());
                instanceRepo.save(instance);
                deployRepo.save(workflowDeploy);
            } else if (Objects.equals(status, "FAIL")) {
                WorkflowDeploy workflowDeploy = deployRepo.findByWorkflowCode(workflowCode);
                workflowDeploy.setStatus('E');
                workflowDeploy.setUpdateTime(LocalDateTime.now());
                instance.setStatus('E');
                instance.setFinishTime(LocalDateTime.now());
                instanceRepo.save(instance);
                deployRepo.save(workflowDeploy);
            }
        });
    }
    @Scheduled(cron = "${workflow.schedule.initialize}")
    @Async("taskExecutor")
    public void initializeStatus() {
//        if (!scheduleEnabled) {
//            log.warn("close workflow orchestrator schedule.");
//            return;
//        }
        int n = deployRepo.initializeAllStatusToN();
        log.info("Initialized {} workflow status to N.", n);
    }
}
