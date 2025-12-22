package com.xiaoxj.sqlworkflow.domain;

import jakarta.persistence.*;
import lombok.Data;

import java.time.LocalDateTime;

@Entity
@Table(name = "alert_workflow_deploy")
@Data
public class AlertWorkflowDeploy {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    private long schedulerId;
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
    private String crontab;
    private char status;
    private int isDelete;
    private String commitUser;
    @Column(nullable = false, columnDefinition = "DATETIME")
    private LocalDateTime createTime = LocalDateTime.now();

    @Column(nullable = false, columnDefinition = "DATETIME")
    private LocalDateTime updateTime = LocalDateTime.now();

}
