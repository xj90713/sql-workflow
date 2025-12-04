package com.xiaoxj.sqlworkflow.repo;

import com.xiaoxj.sqlworkflow.domain.TaskStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface TaskStatusRepository extends JpaRepository<TaskStatus, Integer> {
    List<TaskStatus> findByCurrentStatus(TaskStatus.Status currentStatus);
}
