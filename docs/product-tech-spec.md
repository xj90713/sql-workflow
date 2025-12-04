# 基于 GitLab 与 DolphinScheduler 的自动化 SQL 任务管理系统 — 产品技术文档（草案）

## 文档目标
- 明确系统的业务目标、技术架构与实现细节，便于后续评审与修改
- 规范 GitLab SQL 文件管理、依赖分析、工作流自动组装、DolphinScheduler 对接、监控告警与部署测试
- 提供可执行的接口定义、表结构、配置与运维指引

## 系统概述
- 业务目标：将数据加工任务以 SQL 为最小单元进行版本化管理与自动调度，以表级血缘关系驱动工作流 DAG，保证正确的触发顺序与并行能力，集成 DolphinScheduler 的调度与重试能力，实现生产级调度控制与监控告警。
- 范围：覆盖 SQL 文件规范、依赖分析服务、工作流引擎、DolphinScheduler 网关、监控与告警、部署与测试。
- 非目标：不处理引擎内核执行性能优化；不覆盖多租户、跨环境迁移策略（可在后续版本扩展）。

## 架构总览
- 技术栈：Spring Boot 3.2.4、JDK 17、MySQL、sqllineage4j、DolphinScheduler、GitLab。
- 组件划分：
  - GitLab 脚本与 CI：提交 SQL 文件与触发依赖分析
  - 依赖分析服务：解析 SQL，构建并入库表级血缘
  - 工作流服务：构建 DAG，拓扑排序，动态触发与并行度控制
  - DolphinScheduler 网关：统一封装 DS REST API
  - 监控服务：超时检测、失败重试策略入口、邮件与钉钉告警

## GitLab SQL 管理规范
- 命名规范：每个加工目标表一个独立脚本文件 `{table_name}.sql`。
- 提交的sql文件如下：
```sql
-- #########################################################
-- 表名： cdm.dwd_gzg_chl_app_source
-- Output：输出表： cdm.dwd_gzg_chl_app_source
-- Input:  输入表：ods.ods_db_sscf_app_data_trace_0db,pdw.dim_sscf_user
-- Author: 操作者: wangdongdong
-- Create: 时间 2019-03/05
-- 作业方式: 每日全量
-- #########################################################
-- 修改时间 2022-09-26 16:42
insert overwrite table cdm.dwd_gzg_chl_app_source
select t1.id                                                                                  --自增字段标识
     , t1.guid                                                                                --下载设备guid
     , nvl(case
               when regexp_extract(t1.map_col, '"from_user_id":"([^"]*)"', 1) = '' then null
               else regexp_extract(t1.map_col, '"from_user_id":"([^"]*)"', 1) end,
           t2.user_id)                                                        as from_user_id --邀请者user_id
     , case
           when regexp_extract(t1.map_col, '"from_guid":"([^"]*)"', 1) = '' then null
           else regexp_extract(t1.map_col, '"from_guid":"([^"]*)"', 1) end    as from_guid    --邀请者guid
     , case
           when regexp_extract(t1.map_col, '"from_id":"([^"]*)"', 1) = '' then null
           else regexp_extract(t1.map_col, '"from_id":"([^"]*)"', 1) end      as from_id      --站内邀请场景标识
     , case
           when regexp_extract(t1.map_col, '"from_sign_id":"([^"]*)"', 1) = '' then null
           else regexp_extract(t1.map_col, '"from_sign_id":"([^"]*)"', 1) end as from_sign_id --站外邀请场景标识
     , map_col                                                                                --渠道参数
     , date_format(t1.create_time, 'yyyy-MM-dd HH:mm:ss')                     as create_time  --创建时间
     , 'YYZQZX'                                                               as tenant_id
from ods.ods_db_sscf_app_data_trace_0db t1
         left join
     cdm.dim_gzg_usr_user_info t2
     on regexp_extract(t1.map_col, '"serial_num":"([^"]*)"', 1) = t2.user_number;
```
- 任务说明：以“目标表”为任务标识，文件名称作为唯一 SQL 来源定位。

## 依赖分析模块（GitLab Runner 接口）
- 表task_deploy接收 GitLab Runner 提交的 SQL 文件，解析表级依赖，更新数据库。
- 表结构：
  ```sql
  CREATE TABLE task_deploy (
    id int auto_increment,
    task_id VARCHAR(32),
    task_name VARCHAR(200),
    file_path VARCHAR(200),
    file_name VARCHAR(200),
    file_content LONGTEXT,
    file_md5 VARCHAR(100),
    commit_user VARCHAR(500),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
  ```
- 表级血缘解析：提取 `source_tables` 与 `target_table`，生成 `task_dependencies` 记录。
- 表结构：
  ```sql
  CREATE TABLE task_dependencies (
    id int auto_increment,
    task_id VARCHAR(64) ,
    task_name VARCHAR(255),
    source_tables JSON,
    target_table VARCHAR(64),
    status VARCHAR(16),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
  );
  ```
- API：`POST /api/dependencies/analyze`
  - 入参：`sql_name`、`sql_content`
  - 出参：保存后的 TaskDependency JSON
- 解析策略：直接利用 sqllineage4j来解析表级别的依赖
- 还需要保存到一张task表中，记录任务的基本信息，如任务名称、任务内容、任务类型、任务状态、任务依赖（source_tables）、创建时间、更新时间等。

## 工作流自动组装
- 构图：节点为目标表（任务），边为源表 → 目标表。
- DAG 规则：
  - 拓扑排序并检测环；环则阻断或报警
  - 动态触发：`task_status` 为 `PENDING` 且所有依赖表状态为 `COMPLETED`
  - 并行度控制：`workflow.maxParallelism` 可配置，默认 4
- 任务状态表：
  ```sql
  CREATE TABLE task_status (
    id int auto_increment,
    task_name VARCHAR(64),
    dependent_tables JSON,
    current_status ENUM('PENDING','RUNNING','SUCCESS','FAILED'),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    PRIMARY KEY (id)
  );
  ```
- 状态流转：`PENDING → RUNNING → SUCCESS/FAILED`；`dependent_tables` 保存依赖状态映射。

## DolphinScheduler 集成层
- 职责：封装 DS REST API，提供工作流创建/更新、执行、状态查询、重试。
- API：
  - `POST /api/ds/workflow`
  - `POST /api/ds/execute`
  - `GET /api/ds/status?taskName=xxx`
  - `POST /api/ds/retry`
- 触发逻辑：工作流服务定时检查，依赖就绪且并发空位即调用网关执行，并更新为 `RUNNING`，执行成功则更新为 `SUCCESS`，失败则更新为 `FAILED`。

## 监控与异常处理（可后续优化）
- 超时监控：默认 2 小时（`TASK_TIMEOUT_SECONDS`），超时判定并告警。
- 告警渠道：邮件（`spring.mail.*`）、钉钉（`DING_WEBHOOK_URL`）。
- 死锁检测：周期构图环检测；简易恢复策略为断边并报警（可后续优化）。

## 运行时配置
- 环境变量：
  - 数据源：`DB_URL`、`DB_USERNAME`、`DB_PASSWORD`
  - 工作流：`WORKFLOW_MAX_PARALLELISM`、`WORKFLOW_TRIGGER_INTERVAL`
  - 网关：`DS_BASE_URL`、`DS_AUTH_TOKEN`
  - 监控：`TASK_TIMEOUT_SECONDS`、`DING_WEBHOOK_URL`、`MAIL_*`
- 示例：
  - `DB_URL=jdbc:mysql://<host>:3306/sql_workflow?useSSL=false&serverTimezone=UTC`
  - `WORKFLOW_MAX_PARALLELISM=8`
  - `WORKFLOW_TRIGGER_INTERVAL=300`

## 部署架构
- 前置环境：JDK 17、Maven 3.8+、MySQL 8.x。
- springboot 3.2.4 框架
- 只部署一个服务，所有功能模块都在这个服务中实现，包括依赖分析、工作流自动组装、DolphinScheduler 集成层、监控与异常处理等。
- 按照springboot工程化的标准来实现；包括目录结构、配置文件、依赖管理、日志配置、异常处理等。

## CI/CD 流程
- 开发者提交 `spec.json` 或 SQL 变更
- CI 生成 SQL 文件、调用依赖分析 API 更新 `task_dependencies`
- 可扩展 MR 检查：校验 SQL 头元数据完整性与血缘合理性

## 安全与合规
- 认证鉴权：网关对接 DS 使用 Token；内部服务可扩展 API Key/JWT。
- 敏感信息：禁止提交密钥；通过环境变量或密钥管理服务注入。
- 访问控制：仅 Runner 与内部服务访问依赖分析接口；网关暴露在内网。

## 测试方案
- 单元测试：SQL 解析头、DAG 拓扑与环检测。
- 集成测试：GitLab→依赖分析→工作流触发→DS 执行→状态更新 全链路。
- 压力测试：模拟 100+ 并发调度，验证并行度与数据库压力。
- 回归测试：血缘更新对既有流程的影响评估。

## 性能与容量规划
- 数据库：为 `task_id`/`task_name` 建索引；记录线性增长。
- 调度：并行度与限流结合，避免下游DolphinScheduler引擎任务过载。


## 里程碑与路线图
- M1：最小可用版本（当前）
- M2：引入 sqllineage4j 深度解析，兼容多方言 SQL
- M3：完善重试与失败分级策略
- M4：接入指标系统与告警策略
- M5：支持多租户与跨环境迁移

## 示例流程
1.  提交已表名称为文件名的SQL文件到 GitLab
2. CI 检测新增或者修改sql文件，并调用相关接口依赖分析更新或者新增入库，入库的表包括任务表（task_name、task_content、task_type、task_status、dependent_tables，create_time、update_time，author）和依赖表（task_name、dependent_table，create_time、update_time）；
3. 如果是新增的sql文件，则需要根据依赖关系，自动组装工作流，并调用DolphinScheduler API创建工作流等；如果是修改的sql文件，则需要根据依赖关系，判断是否需要更新工作流；如果需要更新，则需要调用DolphinScheduler API更新工作流；
4. 工作流服务按依赖与并发策略触发执行，即根据 `task_status` 为 `PENDING` 且所有依赖表状态为 `COMPLETED` 来触发执行，这里也是通过调用DolphinScheduler API来触发执行的，所以需要判断sql对应的任务和DolphinScheduler任务流的关联关系，在调用api执行；
5. DolphinScheduler 执行并回传状态，更新 `task_status`
6. 监控服务检查超时并告警

## 待确认与修改项
- SQL 文件头是否扩展字段（业务域、优先级、调度窗口等）
- 依赖就绪判断的来源与格式（当前基于 `dependent_tables` 映射）
- DolphinScheduler REST 路由与鉴权方案与现网版本对齐
- 并行度控制与队列优先级策略
- 告警策略细节（通知人群、时间窗口、抑制策略）
- 并发与容量目标（并发上限、数据库规模）
