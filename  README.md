# SQL 工作流调度器 (SQL Workflow Scheduler)

本项目是一个基于 **Apache DolphinScheduler** 的自动化服务，旨在解决 SQL 脚本的依赖解析与自动调度问题。它作为中间件连接了 SQL 代码仓库与工作流调度系统，能够自动分析表级血缘关系并管理执行顺序。

## 🚀 功能特性

- **自动依赖解析**：深度解析 SQL 脚本，自动提取源表（输入）和目标表（输出），构建依赖关系图。
- **DolphinScheduler 集成**：通过 API 无缝对接 DolphinScheduler，自动创建、更新和管理工作流定义。
- **数据驱动调度**：不仅支持定时调度，更支持基于上游数据（源表）就绪状态的触发式调度。
- **并发控制**：内置全局并发控制机制，防止因任务过多导致资源耗尽。
- **全生命周期管理**：提供完整的 API 用于工作流的发布、更新和状态监控。
- **告警支持**：集成了工作流告警配置机制。

## 🏗 架构设计

本系统作为 SQL 仓库与 DolphinScheduler 之间的桥梁：

1.  **接收 (Ingestion)**：通过 API 接收 SQL 脚本内容。
2.  **解析 (Parsing)**：利用 `SqlLineageService` 分析 SQL 逻辑，识别输入/输出表。
3.  **注册 (Registration)**：
    -   在 DolphinScheduler 中创建对应的工作流定义。
    -   在本地 `workflow_deploy` 表中存储元数据（依赖关系、调度配置）。
4.  **编排 (Orchestration)**：
    -   `WorkflowOrchestrator` 定期轮询，检查待执行任务的源表是否已就绪。
    -   当依赖满足且有可用执行槽位时，触发 DolphinScheduler 工作流实例。

## 🛠 环境要求
- **Java**: 17+ (推荐)
- **Maven**: 3.6+
- **Apache DolphinScheduler**: 3.x
- **Nacos**: 用于配置管理
- **MySQL**: 用于元数据存储

## ⚙️ 配置指南

项目主要通过 **Nacos** 进行配置管理。

### 引导配置 (`bootstrap.yml`)
在 `src/main/resources/bootstrap.yml` 中配置 Nacos 服务地址和命名空间：

```yaml
spring:
  cloud:
    nacos:
      discovery:
        server-addr: ${NACOS_HOST:localhost}:8848
        namespace: ${NACOS_NAMESPACE}
      config:
        server-addr: ${NACOS_HOST:localhost}:8848
        file-extension: yaml
```

### 核心参数 (在 Nacos 或 `application.yml` 中配置)

```yaml
dolphin:
  base-url: http://dolphinscheduler-api:12345/dolphinscheduler
  token: <your-dolphin-token>
  project:
    code: <default-project-code>
  alertProject:
    code: <alert-project-code>

workflow:
  schedule:
    enabled: true            # 是否启用调度编排器
    triggerPending: "0/30 * * * * ?" # 检查待执行任务的 Cron 表达式
    maxParallelism: 16       # 最大并发工作流数量
```

## 🔌 API 使用说明

服务提供 REST API 用于管理工作流。

### 新增工作流

注册一个新的 SQL 工作流。

- **URL**: `/api/dependencies/addWorkflow`
- **Method**: `POST`
- **Content-Type**: `application/json`

**请求体:**

```json
{
  "file_path": "/path/to/your/script.sql",
  "content": "<base64-encoded-sql-content>",
  "commit_user": "user_name"
}
```

### 更新工作流

更新现有的工作流定义及其依赖关系。

- **URL**: `/api/dependencies/updateWorkflow`
- **Method**: `POST`
- **Content-Type**: `application/json`

**请求体:**

```json
{
  "file_path": "/path/to/your/script.sql",
  "content": "<base64-encoded-sql-content>",
  "commit_user": "user_name"
}
```

## 🗄 数据库模型

核心调度表说明：

-   `workflow_deploy`: 存储工作流定义、源表/目标表信息及当前状态。
-   `workflow_instance`: 记录工作流的执行历史和运行时状态。
-   `alert_workflow_deploy`: 管理告警相关配置。

## 📦 构建与运行

1.  **构建项目**:
    ```bash
    mvn clean package -DskipTests
    ```

2.  **启动应用**:
    ```bash
    java -jar target/sql-workflow-*.jar --spring.profiles.active=local
    ```

## 🤝 参与贡献

1.  Fork 本仓库
2.  创建特性分支 (`git checkout -b feature/amazing-feature`)
3.  提交改动 (`git commit -m 'Add some amazing feature'`)
4.  推送到分支 (`git push origin feature/amazing-feature`)
5.  提交 Pull Request
