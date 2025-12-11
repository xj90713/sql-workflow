package com.xiaoxj.sqlworkflow.domain;

import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDateTime;

@Entity
@Table(name = "workflow_instance")
@Data
public class WorkflowInstance {
    public enum Status { PENDING, RUNNING, SUCCESS, FAILED }
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    private String taskName;
    @Column(columnDefinition = "JSON")
    private String dependentTables;

    private int state;

    private String workflowInstanceId;

    private String name;

    private Long workflowCode;

    private Long projectCode;

    private int runTimes;

    @Column(nullable = false)
    private LocalDateTime createdAt = LocalDateTime.now();

    @Column
    private LocalDateTime updatedAt = LocalDateTime.now();
}

