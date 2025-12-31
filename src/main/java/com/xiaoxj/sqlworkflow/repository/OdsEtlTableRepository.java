package com.xiaoxj.sqlworkflow.repository;

import com.xiaoxj.sqlworkflow.entity.OdsEtlTable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OdsEtlTableRepository extends JpaRepository<OdsEtlTable, Integer> {
    OdsEtlTable findBySourceDbAndSourceTable(String sourceDb, String sourceTable);
}
