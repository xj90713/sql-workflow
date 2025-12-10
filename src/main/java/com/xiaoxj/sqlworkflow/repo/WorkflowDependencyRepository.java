package com.xiaoxj.sqlworkflow.repo;

import com.xiaoxj.sqlworkflow.domain.WorkflowDependency;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface WorkflowDependencyRepository extends JpaRepository<WorkflowDependency, Integer> {
    List<WorkflowDependency> findByTaskName(String taskName);
}
