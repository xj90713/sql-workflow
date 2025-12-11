package com.xiaoxj.sqlworkflow.domain;

import jakarta.persistence.*;
import lombok.Data;

import java.time.LocalDateTime;

@Entity
@Table(name = "etl_tables")
@Data
public class ETLTable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    private String sourceTable;
    private String targetTable;
    private String sourceDb;
    private String targetDb;
    private String DbName;
    @Column
    private LocalDateTime finishTime = LocalDateTime.now();
    private char status;
}
