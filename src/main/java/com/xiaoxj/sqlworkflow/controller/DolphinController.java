package com.xiaoxj.sqlworkflow.controller;

import com.xiaoxj.sqlworkflow.service.DolphinGatewayClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/ds")
public class DolphinController {
    private final DolphinGatewayClient client;
    public DolphinController(DolphinGatewayClient client) { this.client = client; }

    @PostMapping("/workflow")
    public String workflow(@RequestBody Map<String, Object> payload) { return client.createOrUpdateWorkflow(payload); }
    @PostMapping("/execute")
    public String execute(@RequestBody Map<String, Object> payload) { return client.executeTask(payload); }
    @GetMapping("/status")
    public String status(@RequestParam String taskName) { return client.status(taskName); }
    @PostMapping("/retry")
    public String retry(@RequestBody Map<String, Object> payload) { return client.retry(payload); }

    @GetMapping("/get")
    public ResponseEntity<String> get() { return client.get();  }
}
