# SQL Workflow

基于 Spring Boot + Apache DolphinScheduler 的 SQL 工作流编排服务。  
核心能力是将 SQL 脚本解析为表级依赖关系，并自动完成工作流创建、更新、触发与状态回写。

## 1. 项目目标

- 接收 SQL（通常来自 GitLab/CI）并解析血缘关系。
- 将 SQL 任务映射为 DolphinScheduler 工作流。
- 基于依赖就绪状态自动调度执行，并控制全局并发。
- 提供告警类工作流的创建和定时配置能力。

## 2. 核心功能

- SQL 血缘解析：提取 `source_tables`、`target_table`、`dependencies`。
- 工作流生命周期管理：创建、更新、上下线、启动、状态查询。
- 依赖调度编排：按上游表/任务完成情况触发下游任务。
- 运行态记录：保存部署信息与实例运行状态。
- OpenMetadata 血缘代理：转发 SQL 血缘查询请求。

## 3. 架构概览

1. CI/调用方提交 SQL（Base64 编码）到本服务。
2. 服务解析 SQL，落库到 `workflow_deploy`。
3. 服务调用 DolphinScheduler API 创建或更新工作流定义。
4. `WorkflowOrchestrator` 定时轮询，挑选可执行任务并触发运行。
5. 定时检查实例状态，回写 `workflow_instance` 与 `workflow_deploy`。

## 4. 技术栈

- Java 21
- Spring Boot 3.2.4
- Spring Data JPA
- MySQL / PostgreSQL Driver
- Nacos（配置与服务发现）
- sqllineage4j（SQL 血缘解析）
- Apache DolphinScheduler（工作流调度）

## 5. 目录结构

```text
src/main/java/com/xiaoxj/sqlworkflow
├── controller        # 对外 REST API
├── service           # 业务服务接口与实现
├── scheduler         # 定时编排器
├── dolphinscheduler  # DS 相关对象与操作封装
├── repository        # JPA Repository
├── entity            # 数据实体
├── common            # 工具类/异常/返回对象
└── core/remote       # HTTP 客户端与 DS 网关底层实现
```

## 6. 配置说明

项目通过 Nacos 加载业务配置，`bootstrap.yml` 负责引导连接信息。

### 6.1 启动配置（示例）

```yaml
spring:
  application:
    name: sql-workflow
  profiles:
    active: local
```

### 6.2 关键业务配置（建议放 Nacos）

```yaml
dolphin:
  base-url: http://<dolphinscheduler-host>/dolphinscheduler
  token: <token>
  tenant:
    code: <tenant-code>
  alertProject:
    code: 123456789

workflow:
  schedule:
    enabled: true
    triggerPending: "0/30 * * * * ?"
    checkRunning: "0/30 * * * * ?"
    initialize: "0 0 0 * * ?"
    deleteWorkflowInstance: "0 0 3 * * ?"
    maxParallelism: 16
    projectCodes: "123,456"
    days: 7

openmetadata:
  api:
    url: http://<openmetadata-api>/api/v1/sql/lineage
```

## 7. API 概览

### 7.1 工作流接口

1. `POST /api/dependencies/addWorkflow`  
新增工作流（自动建流并上线）。

2. `POST /api/dependencies/updateWorkflow`  
更新已有工作流（先下线、更新、再上线）。

3. `POST /api/dependencies/addWorkflowAndScheduler`  
新增“告警类”工作流并创建调度。

4. `POST /api/dependencies/updateWorkflowAndScheduler`  
更新“告警类”工作流及调度配置。

5. `POST /api/dependencies/updateWorkflowStatus`  
按变更表名批量重置受影响下游任务状态。

### 7.2 OpenMetadata 接口

1. `POST /api/openmetadata/lineage`  
转发 SQL 到 OpenMetadata 血缘接口并返回结果。

### 7.3 典型请求体（`addWorkflow` / `updateWorkflow`）

```json
{
  "file_path": "/dw/path/ods_xxx.sql",
  "project_code": "123456789",
  "content": "<base64-sql>",
  "commit_user": "your_name"
}
```

说明：
- `content` 为 Base64 编码 SQL 内容。
- `project_code` 为 DolphinScheduler 项目编码，当前实现中必填。

## 8. 数据表

初始化脚本位于 `src/main/resources/db/migration/V1__init.sql`，核心表如下：

- `workflow_deploy`：工作流部署元信息、解析结果、状态。
- `workflow_instance`：工作流实例运行记录。
- `alert_workflow_deploy`：告警类工作流部署与调度信息。
- `no_scheduler_table`：无需调度的表配置。

状态约定（`workflow_deploy.status`）：
- `N`：未运行
- `R`：运行中
- `Y`：成功
- `E`：失败

## 9. 本地构建与运行

```bash
# 构建
mvn clean package -DskipTests

# 运行
java -jar target/sql-workflow-*.jar --spring.profiles.active=local
```

## 10. 常见问题

1. 任务一直不触发
- 检查 `workflow.schedule.enabled=true`。
- 检查待执行任务是否为 `schedule_type=1` 且 `status=N`。
- 检查依赖表是否已进入就绪队列。

2. 创建工作流失败
- 检查 `dolphin.base-url`、`token`、`project_code` 是否正确。
- 检查 DolphinScheduler 项目和租户权限。

3. SQL 解析失败
- 优先确认 SQL 语法和方言兼容性。
- 对复杂脚本可先在 CI 侧做预清洗，再提交。

## 11. 版本与贡献

- 当前版本：`1.59.0-SNAPSHOT`
- 提交规范：建议分支命名 `feature/*`、`fix/*`，PR 描述包含变更动机、影响范围、验证方式。
