# 分区血缘 API（Partition Lineage API）

面向零售数仓的**分区级数据血缘服务**（FastAPI + SQLite）：登记门店原始日分区的版本，
以及清洗、月汇总作业的完成记录，保留每版产出**实际读取**与**实际产出**的分区版本；
在销售数据更正后，列出过期产出及通往更正源头的依赖路径，用于安排重跑。

## 1. 领域规则

| 概念 | 说明 |
| --- | --- |
| 分区（partition） | 由 `partition_key`（JSON 对象，如 `{"dataset":"raw_sales","store_id":"S01","dt":"2026-10-01"}`）标识，键顺序不影响等价判断 |
| 分区版本 | 整数版本号，**由调用方递增分配**；同分区新版本号必须大于已有版本 |
| 作业运行（job run） | 一次清洗/月汇总作业的完成记录，声明实际读取的输入版本与产出版本 |
| 产出级血缘 | 多产出作业通过每个产出的 `input_indices` 声明它实际消费的输入；不声明则保守视为消费全部输入 |

**过期判定（仅针对各分区的当前版本）**

1. `input_replaced`：产出实际读取的某个输入版本已被同分区更高版本替换；
2. `upstream_stale`：产出读取的上游分区版本本身已过期（递归上溯）；
3. 满足任一条件即过期。`GET /api/v1/stale-outputs` 返回每个过期产出的原因，
   以及沿实际依赖上溯到被替换源头分区的完整路径。

**重跑与迟到记录**

- 重跑登记新作业后，新产出只按**本次实际输入**重新判断；旧产出的依赖不会再影响它
  （旧版本已被替换，不在当前版本评估范围内）。
- 迟到的低版本作业记录不能替换当前产出：产出版本号不高于分区当前版本时直接拒绝（409）。
- 一次作业的多个产出在**同一个 `BEGIN IMMEDIATE` 事务**中登记，任一校验失败整体回滚，
  查询不可能读到登记一半的血缘；过期计算同样在单一快照事务内完成。

## 2. 目录结构

```
lineage_api/
├── app/
│   ├── main.py        # FastAPI 应用、异常处理、/admin/reset
│   ├── routes.py      # HTTP 路由
│   ├── service.py     # 登记事务、过期判断、依赖路径搜索
│   ├── models.py      # Pydantic 请求/响应模型
│   └── db.py          # SQLite 连接、WAL、建表脚本
├── tests/             # pytest 自测（21 个用例）
├── demo/run_demo.py   # 可演示 API 入口（真实 HTTP 走完整业务流程）
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── pytest.ini
```

## 3. 本地运行

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
# 交互式文档： http://127.0.0.1:8000/docs
```

环境变量：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `LINEAGE_DB_PATH` | `./data/lineage.db` | SQLite 数据库文件路径 |
| `LINEAGE_ENABLE_RESET` | `1` | 是否开放 `POST /admin/reset`，生产置 `0` 关闭 |

## 4. Docker Compose 启动

```bash
docker compose up --build
# 服务： http://127.0.0.1:8000 ，数据库持久化在命名卷 lineage-data
```

## 5. API 一览（前缀 `/api/v1`）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/partitions` | 登记一个分区版本（版本号调用方递增） |
| GET | `/partitions?partition_key=<JSON>` | 列出分区版本，可按分区过滤 |
| GET | `/partitions/lineage?partition_key=<JSON>&version=n` | 单版本血缘、是否过期与依赖路径 |
| POST | `/job-runs` | 登记作业完成记录（多产出原子提交） |
| GET | `/job-runs` | 列出作业记录 |
| GET | `/stale-outputs` | 列出当前过期产出及通往更正源头的路径 |
| GET | `/health` | 健康检查 |
| POST | `/admin/reset` | 清空数据（仅演示/测试，可关闭） |

### 快速示例

```bash
# 登记门店 S01 原始日分区 v1
curl -s -X POST localhost:8000/api/v1/partitions -H 'Content-Type: application/json' -d '{
  "partition_key": {"dataset":"raw_sales","store_id":"S01","dt":"2026-10-01"},
  "partition_type": "raw_daily", "version": 1}'

# 清洗作业：读取 raw v1，产出 clean v1（input_indices 指向第 0 个输入）
curl -s -X POST localhost:8000/api/v1/job-runs -H 'Content-Type: application/json' -d '{
  "run_id": "clean-1", "job_name": "daily_clean", "job_type": "clean",
  "finished_at": "2026-11-02T01:00:00Z",
  "inputs":  [{"partition_key": {"dataset":"raw_sales","store_id":"S01","dt":"2026-10-01"}, "version": 1}],
  "outputs": [{"partition_key": {"dataset":"clean_sales","store_id":"S01","dt":"2026-10-01"},
               "version": 1, "partition_type": "clean_daily", "input_indices": [0]}]}'

# 更正：原始分区登记 v2 后查询过期产出
curl -s -X POST localhost:8000/api/v1/partitions -H 'Content-Type: application/json' -d '{
  "partition_key": {"dataset":"raw_sales","store_id":"S01","dt":"2026-10-01"},
  "partition_type": "raw_daily", "version": 2}'
curl -s localhost:8000/api/v1/stale-outputs
```

## 6. 一键演示

先启动服务，再执行：

```bash
python demo/run_demo.py --base-url http://127.0.0.1:8000
```

演示覆盖：两门店初始链路 → S01 销售更正 → 过期产出与路径 → 重跑清洗（旧依赖失效、
月汇总仍过期）→ 重跑月汇总恢复健康 → 迟到低版本记录被拒 → 多产出原子回滚。
脚本内置断言，可重复执行（开头自动调用 `/admin/reset`）。

## 7. 自测

```bash
pip install pytest httpx
python -m pytest
```

用例覆盖：版本递增/重复/规范化校验、两层血缘的过期原因与路径、重跑后旧依赖失效、
迟到低版本拒绝、多产出原子回滚（非法输入/重复产出）、产出级 `input_indices` 语义、
并发登记串行化。

## 8. 主要设计取舍

- **WAL + `BEGIN IMMEDIATE`**：写事务在第一个语句即取写锁串行化，配合 `busy_timeout`
  处理并发；查询在只读快照事务内完成整张血缘图的计算，不混入登记中途状态。
- **只评估当前版本**：过期清单面向“需要安排重跑的当前产出”；旧版本保留可查，
  但不再参与新产出的判断。
- **保守兜底**：未提供 `input_indices` 的历史/简单作业，按消费全部输入处理，宁可多报不漏报。
- **递归带 memo 与环保护**：血缘深度按作业层级增长，正常为数层；实现上仍带访问集合防御环。
