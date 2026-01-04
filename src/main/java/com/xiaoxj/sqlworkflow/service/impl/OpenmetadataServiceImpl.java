package com.xiaoxj.sqlworkflow.service.impl;

import com.xiaoxj.sqlworkflow.service.OpenmetadataService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Service
public class OpenmetadataServiceImpl implements OpenmetadataService {
    @Value("${openmetadata.api.url}")
    private String apiUrl;

    public String getSqlLineage(String fileName, String sqlContent) {
        RestTemplate rt = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        Map<String, Object> payload = new HashMap<>();
        payload.put("sql_name", fileName);
        payload.put("sql", sqlContent);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(payload, headers);
        return rt.postForObject(apiUrl, entity, String.class);
    }
}
