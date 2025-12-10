package com.xiaoxj.sqlworkflow.repo;

import com.xiaoxj.sqlworkflow.domain.WorkflowInstance;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WorkflowInstanceRepository extends JpaRepository<WorkflowInstance, Integer> {

}
