CREATE TABLE workflow_deploy (
                                 id INT NOT NULL AUTO_INCREMENT,
                                 workflow_id VARCHAR(255),
                                 workflow_name VARCHAR(255),
                                 task_codes VARCHAR(255),
                                 workflow_code BIGINT,
                                 project_code BIGINT,
                                 file_path VARCHAR(255),
                                 file_name VARCHAR(255),
                                 file_content LONGTEXT,
                                 file_md5 VARCHAR(255),
                                 source_tables VARCHAR(1000),
                                 target_table VARCHAR(255),
                                 status CHAR(1) DEFAULT 'N' comment 'N:未运行，R:运行中,Y:已发布，E:运行错误',
                                 is_depedent TINYINT(1) DEFAULT 1 COMMENT '是否依赖,0:非依赖,1:依赖',
                                 is_deleted TINYINT(1) DEFAULT 0 COMMENT '是否删除,0:未删除,1:已删除',
                                 commit_user VARCHAR(255),
                                 create_time DATETIME NOT NULL,
                                 update_time DATETIME NOT NULL,
                                 PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='工作流部署表';

CREATE TABLE workflow_instance (
                                   id INT NOT NULL AUTO_INCREMENT,
                                   name VARCHAR(255),
                                   workflow_name VARCHAR(255),
                                   workflow_instance_id BIGINT,
                                   workflow_code BIGINT,
                                   project_code BIGINT,
                                   status CHAR(1),
                                   run_times INT,
                                   start_time DATETIME NOT NULL,
                                   finish_time DATETIME,
                                   PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='工作流实例表';

CREATE TABLE alert_workflow_deploy (
                                 id INT NOT NULL AUTO_INCREMENT,
                                 workflow_id VARCHAR(255),
                                 workflow_name VARCHAR(255),
                                 schedule_id BIGINT,
                                 task_codes VARCHAR(255),
                                 workflow_code BIGINT,
                                 project_code BIGINT,
                                 file_path VARCHAR(255),
                                 file_name VARCHAR(255),
                                 file_content LONGTEXT,
                                 file_md5 VARCHAR(255),
                                 crontab VARCHAR(1000),
                                 status CHAR(1),
                                 commit_user VARCHAR(255),
                                 create_time DATETIME NOT NULL,
                                 update_time DATETIME NOT NULL,
                                 is_deleted TINYINT(1) DEFAULT 0 COMMENT '是否删除,0:未删除,1:已删除',
                                 PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='告警工作流部署表';