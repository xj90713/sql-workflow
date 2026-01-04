package com.xiaoxj.sqlworkflow.service;

public interface OpenmetadataService {
    String getSqlLineage(String fileName, String sqlContent);
}
