package com.xiaoxj.sqlworkflow.domain;

import jakarta.persistence.*;
import lombok.Data;

import java.time.Instant;
import java.time.LocalDateTime;

@Entity
@Table(name = "task_dependencies")
@Data
public class TaskDependency {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    private String taskCode;
    private String taskName;
    private String workflowCode;
    private String projectCode;
    private String sourceTables;
    private String targetTable;
    private String status;

    @Column(nullable = false, columnDefinition = "DATETIME")
    private LocalDateTime createTime = LocalDateTime.now();

    @Column(nullable = false, columnDefinition = "DATETIME")
    private LocalDateTime updateTime = LocalDateTime.now();
}
