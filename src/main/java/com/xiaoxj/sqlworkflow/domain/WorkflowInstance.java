package com.xiaoxj.sqlworkflow.domain;

import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDateTime;

@Entity
@Table(name = "workflow_instance")
@Data
public class WorkflowInstance {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;
    private String name;
    private char status;
    private String workflowName;
    private Long workflowInstanceId;
    private Long workflowCode;
    private Long projectCode;
    private int runTimes;
    @Column(nullable = false)
    private LocalDateTime startTime = LocalDateTime.now();
    @Column
    private LocalDateTime finishTime = LocalDateTime.now();
}

