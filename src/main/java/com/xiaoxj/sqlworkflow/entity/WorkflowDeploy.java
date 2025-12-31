package com.xiaoxj.sqlworkflow.entity;

import jakarta.persistence.*;
import lombok.Data;

import java.time.LocalDateTime;

@Entity
@Table(name = "workflow_deploy")
@Data
public class WorkflowDeploy {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    private String workflowId;
    private String workflowName;
    private String taskCodes;
    private long workflowCode;
    private long projectCode;
    private String filePath;
    private String fileName;
    @Column(columnDefinition = "LONGTEXT")
    private String fileContent;
    private String fileMd5;
    private String sourceTables;
    private String targetTable;
    private char status;
    private int scheduleType=1;
    private int isDelete=0;
    private String commitUser;
    @Column(nullable = false, columnDefinition = "DATETIME")
    private LocalDateTime createTime = LocalDateTime.now();

    @Column(nullable = false, columnDefinition = "DATETIME")
    private LocalDateTime updateTime = LocalDateTime.now();

}
