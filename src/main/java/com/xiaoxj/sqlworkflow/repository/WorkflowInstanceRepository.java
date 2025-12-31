package com.xiaoxj.sqlworkflow.repository;

import com.xiaoxj.sqlworkflow.entity.WorkflowInstance;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface WorkflowInstanceRepository extends JpaRepository<WorkflowInstance, Integer> {
    List<WorkflowInstance> findByStatus(char status);
}
