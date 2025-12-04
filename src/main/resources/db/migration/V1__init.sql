CREATE TABLE `task_deploy` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `task_id` varchar(255) DEFAULT NULL,
  `task_name` varchar(255) DEFAULT NULL,
  `file_path` varchar(255) DEFAULT NULL,
  `file_name` varchar(255) DEFAULT NULL,
  `file_content` longtext,
  `file_md5` varchar(255) DEFAULT NULL,
  `commit_user` varchar(255) DEFAULT NULL,
  `create_time` datetime NOT NULL,
  `update_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS task_dependencies (
  id INT AUTO_INCREMENT,
  task_id VARCHAR(64),
  task_name VARCHAR(255),
  source_tables VARCHAR(255),
  target_table VARCHAR(64),
  status VARCHAR(16),
  `create_time` datetime NOT NULL,
  update_time datetime NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS task_status (
  id INT AUTO_INCREMENT,
  task_name VARCHAR(64),
  dependent_tables VARCHAR(255),
  current_status ENUM('PENDING','RUNNING','SUCCESS','FAILED'),
  start_time datetime NOT NULL,
  end_time datetime NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
