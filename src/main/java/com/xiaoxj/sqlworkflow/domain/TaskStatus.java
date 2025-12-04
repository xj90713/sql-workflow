package com.xiaoxj.sqlworkflow.domain;

import jakarta.persistence.*;
import java.time.Instant;
import java.time.LocalDateTime;

@Entity
@Table(name = "task_status")
public class TaskStatus {
    public enum Status { PENDING, RUNNING, SUCCESS, FAILED }
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    private String taskName;
    @Column(columnDefinition = "JSON")
    private String dependentTables;
    @Enumerated(EnumType.STRING)
    private Status currentStatus;
    @Column(nullable = false)
    private LocalDateTime createdAt = LocalDateTime.now();

    @Column
    private LocalDateTime updatedAt = LocalDateTime.now();
}
