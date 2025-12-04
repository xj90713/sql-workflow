package com.xiaoxj.sqlworkflow.repo;

import com.xiaoxj.sqlworkflow.domain.TaskDependency;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface TaskDependencyRepository extends JpaRepository<TaskDependency, Integer> {
    List<TaskDependency> findByTaskName(String taskName);
}
