package com.xiaoxj.sqlworkflow.domain;

import jakarta.persistence.*;
import lombok.Data;

import java.time.Instant;
import java.time.LocalDateTime;

@Entity
@Table(name = "task_deploy")
@Data
public class TaskDeploy {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    private String taskId;
    private String taskName;
    private long taskCode;
    private long workflowCode;
    private long projectCode;
    private String filePath;
    private String fileName;
    @Column(columnDefinition = "LONGTEXT")
    private String fileContent;
    private String fileMd5;
    private String commitUser;
    @Column(nullable = false, columnDefinition = "DATETIME")
    private LocalDateTime createTime = LocalDateTime.now();

    @Column(nullable = false, columnDefinition = "DATETIME")
    private LocalDateTime updateTime = LocalDateTime.now();

}
