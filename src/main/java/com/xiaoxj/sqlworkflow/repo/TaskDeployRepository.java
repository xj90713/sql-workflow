package com.xiaoxj.sqlworkflow.repo;

import com.xiaoxj.sqlworkflow.domain.TaskDeploy;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TaskDeployRepository extends JpaRepository<TaskDeploy, Integer> {
    TaskDeploy findTopByTaskNameOrderByUpdateTimeDesc(String taskName);
}
