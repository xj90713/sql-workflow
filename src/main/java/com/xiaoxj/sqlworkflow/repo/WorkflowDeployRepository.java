package com.xiaoxj.sqlworkflow.repo;

import com.xiaoxj.sqlworkflow.domain.WorkflowDeploy;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface WorkflowDeployRepository extends JpaRepository<WorkflowDeploy, Integer> {
    List<WorkflowDeploy> findByStatus(char status);
    WorkflowDeploy findByWorkflowName(String workflowName);
    WorkflowDeploy findTopByWorkflowNameOrderByUpdateTimeDesc(String workflowName);
    WorkflowDeploy findByTargetTable(String targetTable);
    WorkflowDeploy findByWorkflowCode(Long workflowCode);

    // 初始化整个表：将所有记录的status更新为N
    @Modifying
    @Query("UPDATE WorkflowDeploy w SET w.status = 'N', w.updateTime = CURRENT_TIMESTAMP")
    int initializeAllStatusToN();
}
