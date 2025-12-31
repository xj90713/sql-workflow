package com.xiaoxj.sqlworkflow.entity;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Table(name = "ods_etl_table")
@Data
public class OdsEtlTable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    private String sourceDb;
    private String sourceTable;
    private String targetDb;
    private String targetTable;
}
