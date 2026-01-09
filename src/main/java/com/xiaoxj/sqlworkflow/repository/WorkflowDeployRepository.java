package com.xiaoxj.sqlworkflow.repository;

import com.xiaoxj.sqlworkflow.entity.WorkflowDeploy;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface WorkflowDeployRepository extends JpaRepository<WorkflowDeploy, Integer> {
    List<WorkflowDeploy> findByStatusAndScheduleType(char status, int scheduleType);
    WorkflowDeploy findByWorkflowName(String workflowName);
    WorkflowDeploy findTopByWorkflowNameOrderByUpdateTimeDesc(String workflowName);

    @Query(value = "SELECT * FROM workflow_deploy WHERE target_table = ?1 and status='N'", nativeQuery = true)
    List<WorkflowDeploy> findByTargetTable(String targetTable);
    WorkflowDeploy findByWorkflowCode(Long workflowCode);

    // 初始化整个表：将所有记录的status更新为N
    @Modifying
    @Query(value = "UPDATE workflow_deploy w SET w.status = 'N', w.update_time = CURRENT_TIMESTAMP", nativeQuery = true)
    int initializeAllStatusToN();

    // 更新指定表的状态
    @Modifying
    @Query(value = "UPDATE workflow_deploy SET status = 'N' WHERE target_table IN ?1",
            nativeQuery = true)
    int updateStatusByTargetTable(Set<String> tables);

//    @Query("SELECT w.targetTable as target, w.sourceTables as source FROM WorkflowDeploy w")
//    List<Object[]> findAllTargetAndSource();
}
