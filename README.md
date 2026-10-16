# GitHub Crawler

通过任务下发接口接收上游监测任务，搜索 GitHub 公开仓库和代码；爬取结果进入 RabbitMQ 后由 worker 写入 MySQL，并可选调用下游 report 接口。
## 启动

```bash
cp .env.example .env
# 编辑 .env：填写 GitHub Token、RabbitMQ、Redis、MySQL 等配置
docker compose -f docker-compose-build.yml up --build -d
```

两个 Dockerfile 都直接从 `python:3.11-slim` 构建，不需要预先构建自定义基础镜像。依赖安装单独缓存，改源码时 Docker 可以复用依赖层。Compose 使用 host 网络，依赖服务地址请按宿主机实际地址填写。日志在 `src/app/logs` 和 `src/crawler/logs`。

MySQL 数据库需事先创建；首次启动前创建任务表：

```sql
CREATE TABLE tasks (
  task_id VARCHAR(255) PRIMARY KEY,
  task_type TINYINT NOT NULL,
  keywords JSON NOT NULL,
  priority TINYINT NOT NULL,
  max_page INT NOT NULL,
  request_interval INT NOT NULL,
  status TINYINT NOT NULL DEFAULT 1
);
```

结果表由程序按 `RESULTS_TABLE` 自动创建。

## 下发监测任务

接口：`POST http://localhost:8008/crawler/task`

```bash
# Set API_KEY to a long random value in .env, then export that same value for curl.
export API_KEY='same-value-as-in-dot-env'
curl -X POST 'http://localhost:8008/crawler/task' \
  -H 'Content-Type: application/json' \
  -H "X-API-Key: ${API_KEY}" \
  -H 'Idempotency-Key: leak-monitor-submit-001' \
  -d '{
    "task_id": "leak-monitor-20261013-001",
    "task_type": 0,
    "keywords": ["keyword1", "keyword2"],
    "priority": 5,
    "max_page": 2,
    "request_interval": 3600
  }'
```

`task_type` 为 `0` 表示单次任务，`1` 表示周期任务；`priority` 范围为 0–10，`max_page` 范围为 1–100，周期单位为秒。任务会投递到 GitHub 队列；只有在部署了文库爬虫后，才将 `ENABLE_WENKU_CRAWLER=1`。

API 返回的 `run_id` 标识一次实际运行。查询任务最近运行：`GET /crawler/task/{task_id}`；查询某次运行及关键词子任务：`GET /crawler/task/{task_id}/runs/{run_id}`。若请求可能因超时重发，请传同一个 `Idempotency-Key`，服务会返回同一运行记录。

```bash
curl 'http://localhost:8008/crawler/task/leak-monitor-20261013-001' \
  -H "X-API-Key: ${API_KEY}"
```

运行状态包含 `pending`、`queued`、`running`、`retrying`、`succeeded`、`partial`、`failed`、`cancelled`；每个关键词/来源子任务还返回进度、当前阶段、尝试次数和错误摘要。

停止任务：

```bash
curl -X DELETE 'http://localhost:8008/crawler/task?task_id=leak-monitor-20261013-001' \
  -H "X-API-Key: ${API_KEY}"
```

## 结果保存和上报

在 `.env` 配置 MySQL 后，设置 `RESULTS_DB_ENABLED=1`，worker 会自动创建 `RESULTS_TABLE`（默认 `crawler_results`）并保存完整结果 JSON。相同运行、关键词、仓库、匹配类型和文件路径的重投消息不会重复插入；每次周期运行有自己的 `run_id`，因此可保留跨周期的观察历史。结果包含可供下游幂等处理的 `result_id`。

不配置 `REPORT_URL_GITHUB` 时，结果只入库；配置后会在入库成功后 POST 同一份 JSON 到该地址。report 请求失败会重试队列消息，数据库写入具备幂等性。文库来源使用 `REPORT_URL_WENKU`。

`RESULTS_DB_ENABLED=0` 时可以关闭数据库保存；此时至少配置对应来源的 report URL。查看接口文档：`http://localhost:8008/docs`。

## Tests

运行单元测试：

```bash
.venv/bin/python -m unittest discover -s tests -v
```

当前可靠性测试覆盖重复任务消息，以及消息发布成功但 outbox 状态更新失败后的重试恢复。测试使用 mock，不连接真实 MySQL 或 RabbitMQ。

## 配置与发布

`.env.example` 是模板，真实 token 和密码只放在本地 `.env`，不要提交。`.gitignore` 已忽略 `.env`。

配置 `API_KEY` 后所有 API 路由都需要 `X-API-Key`；生产部署应设置强随机值。任务投递使用 MySQL outbox 和 RabbitMQ publisher confirms；临时发布失败会退避重试。超过 `MAX_TASK_ATTEMPTS` 或 `MAX_RESULT_ATTEMPTS` 的消息进入对应队列的 `.dead` 死信队列。当前仓库没有文库爬虫实现，默认只消费 GitHub 任务。GitHub Token 可以在 `GITHUB_TOKEN` 中用逗号配置多个。任务运行、关键词子任务和待发送 outbox 表会在启动时自动创建；请给 MySQL 用户建表权限。运行状态表示爬虫结果已经发入结果队列，不代表下游 report 回调已成功处理。
