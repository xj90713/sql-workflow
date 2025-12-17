package com.xiaoxj.sqlworkflow.repo;

import com.xiaoxj.sqlworkflow.domain.OdsEtlTable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OdsEtlTableRepository extends JpaRepository<OdsEtlTable, Integer> {
    OdsEtlTable findBySourceDbAndSourceTable(String sourceDb, String sourceTable);
}
