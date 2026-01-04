package com.xiaoxj.sqlworkflow.service;


import java.util.List;

public interface IngestInfoService {
    List<String> findIngestTables(String sourceDbs, String sourceTables);
}
