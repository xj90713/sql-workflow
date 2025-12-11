package com.xiaoxj.sqlworkflow.repo;

import com.xiaoxj.sqlworkflow.domain.WorkflowDeploy;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface WorkflowDeployRepository extends JpaRepository<WorkflowDeploy, Integer> {
    List<WorkflowDeploy> findByStatus(String status);
    WorkflowDeploy findByTaskName(String taskName);
    WorkflowDeploy findTopByTaskNameOrderByUpdateTimeDesc(String taskName);
    WorkflowDeploy findByTargetTable(String targetTable);
}
