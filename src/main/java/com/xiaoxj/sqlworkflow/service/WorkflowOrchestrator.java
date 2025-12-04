//package com.xiaoxj.sqlworkflow.service;
//
//import com.fasterxml.jackson.core.type.TypeReference;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.xiaoxj.sqlworkflow.core.DagBuilder;
//import com.xiaoxj.sqlworkflow.domain.TaskDependency;
//import com.xiaoxj.sqlworkflow.domain.TaskStatus;
//import com.xiaoxj.sqlworkflow.repo.TaskDependencyRepository;
//import com.xiaoxj.sqlworkflow.repo.TaskStatusRepository;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.scheduling.annotation.Scheduled;
//import org.springframework.stereotype.Service;
//import org.springframework.transaction.annotation.Transactional;
//
//import java.time.Instant;
//import java.util.*;
//
//@Service
//public class WorkflowOrchestrator {
//    private final TaskStatusRepository statusRepo;
//    private final TaskDependencyRepository depRepo;
//    private final DolphinGatewayClient gateway;
//    private final ObjectMapper mapper = new ObjectMapper();
//    @Value("${workflow.maxParallelism:4}")
//    private int maxParallelism;
//
//    public WorkflowOrchestrator(TaskStatusRepository statusRepo, TaskDependencyRepository depRepo, DolphinGatewayClient gateway) {
//        this.statusRepo = statusRepo;
//        this.depRepo = depRepo;
//        this.gateway = gateway;
//    }
//
//    public Map<String, List<String>> buildEdges() {
//        Map<String, List<String>> edges = new HashMap<>();
//        for (TaskDependency d : depRepo.findAll()) {
//            String target = d.getTargetTable();
//            List<String> sources = parseList(d.getSourceTables());
//            edges.put(target, sources);
//        }
//        return edges;
//    }
//
//    private List<String> parseList(String json) {
//        try { return mapper.readValue(json == null ? "[]" : json, new TypeReference<>(){}); } catch (Exception e) { return List.of(); }
//    }
//
//    @Scheduled(fixedDelayString = "${workflow.triggerIntervalSeconds:300}000")
//    @Transactional
//    public void triggerPending() throws Exception {
//        Map<String, List<String>> edges = buildEdges();
//        try { DagBuilder.topoSort(edges); } catch (IllegalStateException e) { /* cycle detected, skip triggering */ return; }
//        List<TaskStatus> pending = statusRepo.findPending();
//        int runningCount = (int) statusRepo.findAll().stream().filter(t -> t.getCurrentStatus() == TaskStatus.Status.RUNNING).count();
//        int slots = Math.max(0, maxParallelism - runningCount);
//        for (TaskStatus t : pending) {
//            if (slots <= 0) break;
//            Map<String, String> deps = mapper.readValue(t.getDependentTables(), new TypeReference<>() {});
//            boolean ready = deps.values().stream().allMatch(s -> "COMPLETED".equalsIgnoreCase(s));
//            if (ready) {
//                gateway.executeTask(Map.of("taskName", t.getTaskName()));
//                t.setCurrentStatus(TaskStatus.Status.RUNNING);
//                t.setStartTime(Instant.now());
//                statusRepo.save(t);
//                slots--;
//            }
//        }
//    }
//}
