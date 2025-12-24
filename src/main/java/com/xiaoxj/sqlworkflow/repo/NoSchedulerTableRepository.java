package com.xiaoxj.sqlworkflow.repo;

import com.xiaoxj.sqlworkflow.domain.NoSchedulerTable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface NoSchedulerTableRepository extends JpaRepository<NoSchedulerTable, Integer> {
    @Query(value = "SELECT table_name FROM no_scheduler_table WHERE is_delete = 0", nativeQuery = true)
    List<String> findTableNamesByDeleteStatusNative();
}
