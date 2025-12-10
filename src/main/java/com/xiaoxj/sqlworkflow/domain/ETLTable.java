package com.xiaoxj.sqlworkflow.domain;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Table(name = "etl_tables")
@Data
public class ETLTable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    private String tableName;
    private String DbName;
    private String source;
}
