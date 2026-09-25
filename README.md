# 机器人作业数据回流服务

这是一个面向机器人作业数据团队的服务端应用，负责管理机型、场景、技能、作业记录、人工标注和数据集。服务使用 FastAPI 提供本地 HTTP 接口，以 SQLite 保存业务数据；质量评分、数据集审核、版本、订阅和统计分析均在同一进程内完成。

## 目录

- `main.py`：应用入口、健康检查、路由注册与发件箱后台派发线程。
- `app/models`：业务实体及其关系，含发件箱事件、派发尝试、消费确认与分析投影。
- `app/routers`：基础资源、作业、数据集、分析和发件箱查询接口。
- `app/services`：评分、统计、策略目录、时间窗口工具，以及发件箱（`outbox`）、进程内派发器（`dispatcher`）与分析消费者（`analytics_consumer`）。
- `app/seed_data.py`：可重复执行的示例数据初始化逻辑。
- `scripts/init_sample_data.py`：初始化脚本的兼容入口。

## 事件发件箱

数据集发布、版本撤回、标注批准三类业务事件通过持久化发件箱传递给内部分析组件：

- 业务写入与事件记录在**同一事务**提交，回滚时事件一并消失，杜绝"状态已变但事件缺失"。
- 事件载荷使用稳定版本号（`PAYLOAD_VERSIONS`），按白名单构造，联系人、审核人等敏感字段不会进入载荷。
- 进程内派发器按创建顺序原子领取事件（并发 worker 不会领到同一事件），逐次记录派发尝试；后台线程周期性派发，也可通过 `POST /api/v1/outbox/dispatch` 手动触发。
- 消费确认以 `(event_id, consumer)` 唯一约束落库，重复确认或确认丢失后的重派不会产生二次影响。
- 失败事件按可注入时钟计算指数退避的重试时间，超过上限进入 `quarantined` 隔离状态，可通过 `GET /api/v1/outbox/events?status=quarantined` 查询；领取后崩溃的事件在租约过期后自动回收重派。

## 配置与运行

默认数据库文件为项目根目录的 `robot_data.db`，可以通过 `DATABASE_URL` 指定 SQLite 文件。安装依赖后运行 `python3 main.py`，服务默认监听 `8000` 端口；`GET /health` 返回服务状态，接口文档位于 `/docs`。

初始化示例数据可执行 `python3 scripts/init_sample_data.py`。该命令会重建本地数据库并写入机型、场景、技能、作业、标注及数据集示例。

## 验证

运行 `python3 -m pytest -q` 执行服务和领域工具测试，运行 `python3 -m compileall -q app main.py scripts` 检查编译。测试只使用临时 SQLite 数据库，不需要额外服务。
