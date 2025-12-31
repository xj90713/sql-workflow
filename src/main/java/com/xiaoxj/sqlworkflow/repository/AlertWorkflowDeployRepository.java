package com.xiaoxj.sqlworkflow.repository;

import com.xiaoxj.sqlworkflow.entity.AlertWorkflowDeploy;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface AlertWorkflowDeployRepository extends JpaRepository<AlertWorkflowDeploy, Integer> {
    AlertWorkflowDeploy findByWorkflowName(String workflowName);
    AlertWorkflowDeploy findTopByWorkflowNameOrderByUpdateTimeDesc(String workflowName);

}
