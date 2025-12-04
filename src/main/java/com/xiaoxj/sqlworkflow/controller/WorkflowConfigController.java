//package com.xiaoxj.sqlworkflow.api;
//
//import com.fasterxml.jackson.core.type.TypeReference;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.xiaoxj.sqlworkflow.service.WorkflowOrchestrator;
//import org.springframework.web.bind.annotation.*;
//
//import java.util.List;
//import java.util.Map;
//
//@RestController
//@RequestMapping("/api/workflow")
//public class WorkflowConfigController {
//    private final WorkflowOrchestrator orchestrator;
//    private final ObjectMapper mapper = new ObjectMapper();
//    public WorkflowConfigController(WorkflowOrchestrator orchestrator) { this.orchestrator = orchestrator; }
//
//    @GetMapping("/dag")
//    public Map<String, List<String>> dag() { return orchestrator.buildEdges(); }
//
//    @GetMapping("/dag/dot")
//    public String dagDot() {
//        Map<String, List<String>> edges = orchestrator.buildEdges();
//        StringBuilder sb = new StringBuilder("digraph G {\n");
//        edges.forEach((t, sources) -> sources.forEach(s -> sb.append('"').append(s).append('"').append(" -> ").append('"').append(t).append('"').append(";\n")));
//        sb.append("}\n");
//        return sb.toString();
//    }
//
//    @PostMapping("/dag")
//    public Map<String, List<String>> updateDag(@RequestBody Map<String, List<String>> edges) {
//        // 预留：可将自定义DAG持久化为配置表；当前直接回显
//        return edges;
//    }
//}
