"""SQLite 连接管理与表结构。

设计要点：
- 分区版本由调用方递增分配，服务端只做校验与登记。
- 一次作业的输入/输出与作业记录在同一个 ``BEGIN IMMEDIATE`` 事务中写入，
  要么全部成功、要么整体回滚，查询不会读到登记一半的血缘。
- 开启 WAL 与外键约束；写入事务串行化，避免并发下的版本竞争。
"""

from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

# 允许通过环境变量覆盖数据库位置（Docker Compose 挂载卷时使用）。
DB_PATH = os.environ.get(
    "LINEAGE_DB_PATH", os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "lineage.db")
)

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS partitions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    partition_key   TEXT NOT NULL,          -- 规范化后的分区标识 JSON
    partition_type  TEXT NOT NULL,          -- raw_daily / clean_daily / monthly_summary / 其他
    version         INTEGER NOT NULL,       -- 调用方递增分配
    registered_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE (partition_key, version),
    UNIQUE (partition_key, id)
);

CREATE INDEX IF NOT EXISTS idx_partitions_lookup
    ON partitions (partition_key, version);

CREATE TABLE IF NOT EXISTS job_runs (
    run_id          TEXT PRIMARY KEY,       -- 调用方提供的作业运行唯一 ID（幂等键）
    job_name        TEXT NOT NULL,
    job_type        TEXT NOT NULL,          -- clean / monthly_summary / 其他
    finished_at     TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS job_inputs (
    run_id              TEXT NOT NULL REFERENCES job_runs(run_id),
    partition_id        INTEGER NOT NULL REFERENCES partitions(id),
    partition_key       TEXT NOT NULL,
    version             INTEGER NOT NULL,
    ordinal             INTEGER NOT NULL,   -- 保留调用方给出的输入顺序
    PRIMARY KEY (run_id, partition_id)
);

CREATE TABLE IF NOT EXISTS job_outputs (
    run_id              TEXT NOT NULL REFERENCES job_runs(run_id),
    partition_id        INTEGER NOT NULL REFERENCES partitions(id),
    partition_key       TEXT NOT NULL,
    version             INTEGER NOT NULL,
    ordinal             INTEGER NOT NULL,
    -- 每个分区版本最多由一次作业产出（登记失败整体回滚，不会出现半成品）
    UNIQUE (partition_id),
    PRIMARY KEY (run_id, partition_id)
);

-- 产出级血缘边：某个产出实际消费了哪些输入分区版本。
-- 多产出作业中各产出的上游可能不同，必须按产出分别记录，
-- 否则一个输入的更正会错误株连同作业的其他产出。
CREATE TABLE IF NOT EXISTS job_output_inputs (
    run_id                  TEXT NOT NULL REFERENCES job_runs(run_id),
    output_partition_id     INTEGER NOT NULL REFERENCES partitions(id),
    input_partition_id      INTEGER NOT NULL REFERENCES partitions(id),
    PRIMARY KEY (run_id, output_partition_id, input_partition_id)
);

CREATE INDEX IF NOT EXISTS idx_job_inputs_run ON job_inputs (run_id);
CREATE INDEX IF NOT EXISTS idx_job_outputs_lookup ON job_outputs (partition_key, version);
CREATE INDEX IF NOT EXISTS idx_job_output_inputs_out ON job_output_inputs (output_partition_id);

-- 任务分派：提交时固定作业信息、实际输入与预定产出版本（JSON 快照），
-- 之后输入分区被更正也不影响任务内已固定的版本，完成时再校验是否仍有效。
CREATE TABLE IF NOT EXISTS tasks (
    task_id         TEXT PRIMARY KEY,       -- 调用方提供的任务唯一 ID（幂等键）
    job_name        TEXT NOT NULL,
    job_type        TEXT NOT NULL,
    inputs_json     TEXT NOT NULL,          -- 固定的实际输入 [{partition_key, version}, ...]
    outputs_json    TEXT NOT NULL,          -- 预定产出 [{partition_key, version, partition_type, input_indices}, ...]
    status          TEXT NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'CLAIMED', 'SUCCEEDED', 'NEEDS_RESCHEDULE')),
    run_id          TEXT,                   -- 成功后关联的 job_runs.run_id
    completed_token TEXT,                   -- 完成该任务的凭证（用于原凭证重报的幂等回放）
    status_detail   TEXT,                   -- 诊断信息（如需重新安排的原因）
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- 领取记录（租约）：每次领取/接管追加一行，当前租约为该任务 lease_seq 最大的一行。
-- 全部落库，服务重启后领取状态不丢失。
CREATE TABLE IF NOT EXISTS task_leases (
    lease_seq       INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         TEXT NOT NULL REFERENCES tasks(task_id),
    claim_token     TEXT NOT NULL UNIQUE,   -- 完成回报凭证
    executor_id     TEXT NOT NULL,
    claimed_at      TEXT NOT NULL,
    expires_at      TEXT NOT NULL           -- 超过即过期，可被其他执行器接管
);

CREATE INDEX IF NOT EXISTS idx_task_leases_task ON task_leases (task_id, lease_seq);
"""


def canonical_key(partition_key: dict[str, Any]) -> str:
    """把分区标识字典规范化为可比较、可唯一约束的 JSON 字符串。"""
    if not isinstance(partition_key, dict) or not partition_key:
        raise ValueError("partition_key 必须是非空对象")
    return json.dumps(partition_key, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def get_connection(db_path: str | None = None) -> sqlite3.Connection:
    """打开一个连接并配置推荐的 PRAGMA。"""
    path = db_path or DB_PATH
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    # 隔离级别交给我们显式 BEGIN/COMMIT 管理
    conn.isolation_level = None
    return conn


def init_db(db_path: str | None = None) -> None:
    """建表（幂等）。"""
    conn = get_connection(db_path)
    try:
        # executescript 会隐式提交；CREATE TABLE IF NOT EXISTS 本身可重复执行
        conn.executescript(SCHEMA)
    finally:
        conn.close()


@contextmanager
def db_session(db_path: str | None = None) -> Iterator[sqlite3.Connection]:
    """FastAPI 依赖：每请求一个连接，请求结束关闭。"""
    conn = get_connection(db_path)
    try:
        yield conn
    finally:
        conn.close()
