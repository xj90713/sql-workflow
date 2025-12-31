package com.xiaoxj.sqlworkflow.repository;

import com.xiaoxj.sqlworkflow.entity.NoSchedulerTable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface NoSchedulerTableRepository extends JpaRepository<NoSchedulerTable, Integer> {
    @Query(value = "SELECT table_name FROM no_scheduler_table WHERE is_deleted = 0", nativeQuery = true)
    List<String> findTableNamesByDeleteStatusNative();
}
