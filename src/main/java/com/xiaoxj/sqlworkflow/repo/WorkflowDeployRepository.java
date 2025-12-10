package com.xiaoxj.sqlworkflow.repo;

import com.xiaoxj.sqlworkflow.domain.WorkflowDeploy;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WorkflowDeployRepository extends JpaRepository<WorkflowDeploy, Integer> {
    WorkflowDeploy findTopByTaskNameOrderByUpdateTimeDesc(String taskName);
    WorkflowDeploy findByTaskName(String taskName);
}
