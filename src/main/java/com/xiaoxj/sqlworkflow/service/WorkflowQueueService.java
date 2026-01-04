package com.xiaoxj.sqlworkflow.service;
import java.util.*;

public interface WorkflowQueueService {


    Set<String> buildReadyQueue();
    String getTargetWorkflowName();

    // 获取数据库和表
    String getDbsAndTables();

    String filterTables(String targetTable, String sourceTables);

    List<String> getAffectedTables(String changedTable, Map<String, String[]> dependencies);

    void find(String currentTable, Map<String, String[]> deps, List<String> result, Set<String> visited) ;
}
