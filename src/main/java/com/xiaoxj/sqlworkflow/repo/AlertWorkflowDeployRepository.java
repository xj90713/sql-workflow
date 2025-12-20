package com.xiaoxj.sqlworkflow.repo;

import com.xiaoxj.sqlworkflow.domain.AlertWorkflowDeploy;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface AlertWorkflowDeployRepository extends JpaRepository<AlertWorkflowDeploy, Integer> {
    List<AlertWorkflowDeploy> findByStatus(char status);
    AlertWorkflowDeploy findByWorkflowName(String workflowName);
    AlertWorkflowDeploy findTopByWorkflowNameOrderByUpdateTimeDesc(String workflowName);
    AlertWorkflowDeploy findByTargetTable(String targetTable);
    AlertWorkflowDeploy findByWorkflowCode(Long workflowCode);

}
