package com.xiaoxj.sqlworkflow.service;

import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Map;

@Component
public class DolphinGatewayClient {
    private final RestTemplate restTemplate = new RestTemplate();
    @Value("${dolphin.baseUrl}")
    private String baseUrl;
    @Value("${dolphin.authToken}")
    private String token;

    private HttpHeaders headers() {
        HttpHeaders h = new HttpHeaders();
//        h.setContentType(MediaType.APPLICATION_JSON);
        if (token != null && !token.isEmpty()) {
            h.set("Token", token);
            h.set("Accept", "application/json");
        }
        return h;
    }

    @Retry(name = "ds-api")
    public String createOrUpdateWorkflow(Map<String, Object> payload) {
        return restTemplate.postForObject(baseUrl + "/dolphinscheduler/workflow", new HttpEntity<>(payload, headers()), String.class);
    }

    @Retry(name = "ds-api")
    public String executeTask(Map<String, Object> payload) {
        return restTemplate.postForObject(baseUrl + "/dolphinscheduler/execute", new HttpEntity<>(payload, headers()), String.class);
    }

    public String status(String taskName) {
        return restTemplate.getForObject(baseUrl + "/dolphinscheduler/status?taskName=" + taskName, String.class);
    }

    @Retry(name = "ds-api")
    public String retry(Map<String, Object> payload) {
        return restTemplate.postForObject(baseUrl + "/dolphinscheduler/retry", new HttpEntity<>(payload, headers()), String.class);
    }

    @Retry(name = "ds-api")
    public ResponseEntity<String> get() {
        HttpEntity<Void> entity = new HttpEntity<>(headers());
        ResponseEntity<String> resp = restTemplate.exchange(baseUrl + "/dolphinscheduler/projects/list", HttpMethod.GET, entity, String.class);
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(resp.getBody());
    }
}
