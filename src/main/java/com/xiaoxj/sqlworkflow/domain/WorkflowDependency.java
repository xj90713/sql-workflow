package com.xiaoxj.sqlworkflow.domain;

import jakarta.persistence.*;
import lombok.Data;

import java.time.LocalDateTime;

@Entity
@Table(name = "workflow_dependencies")
@Data
public class WorkflowDependency {
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
