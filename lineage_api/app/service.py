"""领域服务：分区/作业登记（事务）与过期血缘计算。"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from .db import canonical_key
from .models import (
    InputRef,
    JobRunInfo,
    JobRunRegisterRequest,
    OutputRef,
    PartitionInfo,
    PartitionRegisterRequest,
    PathNode,
    StaleOutput,
)


class DomainError(Exception):
    """业务校验失败，由路由层转成 HTTP 错误响应。"""

    def __init__(self, status_code: int, error_code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code
        self.message = message


@dataclass(frozen=True)
class PNode:
    id: int
    key: str
    type: str
    version: int


# --------------------------------------------------------------------------- #
# 写入：分区版本登记
# --------------------------------------------------------------------------- #
def register_partition(conn: sqlite3.Connection, req: PartitionRegisterRequest) -> PartitionInfo:
    key = canonical_key(req.partition_key)
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT version FROM partitions WHERE partition_key = ? AND version = ?",
            (key, req.version),
        ).fetchone()
        if row is not None:
            raise DomainError(409, "VERSION_ALREADY_REGISTERED", f"分区版本 {key}#v{req.version} 已登记")
        max_row = conn.execute(
            "SELECT MAX(version) AS v FROM partitions WHERE partition_key = ?", (key,)
        ).fetchone()
        if max_row["v"] is not None and req.version <= max_row["v"]:
            raise DomainError(
                409,
                "VERSION_NOT_INCREMENTAL",
                f"新版本号 {req.version} 必须大于当前版本 {max_row['v']}（版本由调用方递增分配）",
            )
        conn.execute(
            "INSERT INTO partitions (partition_key, partition_type, version) VALUES (?, ?, ?)",
            (key, req.partition_type, req.version),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return get_partition(conn, req.partition_key, req.version)  # 读走独立快照


# --------------------------------------------------------------------------- #
# 写入：作业完成记录（多产出原子登记）
# --------------------------------------------------------------------------- #
def register_job_run(conn: sqlite3.Connection, req: JobRunRegisterRequest) -> JobRunInfo:
    try:
        conn.execute("BEGIN IMMEDIATE")

        if conn.execute("SELECT 1 FROM job_runs WHERE run_id = ?", (req.run_id,)).fetchone():
            raise DomainError(409, "RUN_ID_EXISTS", f"作业运行 ID {req.run_id} 已存在")

        # 1) 校验所有输入分区版本均已登记
        input_nodes: list[PNode] = []
        for ref in req.inputs:
            node = _load_partition(conn, ref.partition_key, ref.version)
            if node is None:
                raise DomainError(
                    422,
                    "INPUT_PARTITION_NOT_FOUND",
                    f"输入分区 {canonical_key(ref.partition_key)}#v{ref.version} 尚未登记",
                )
            input_nodes.append(node)

        # 2) 校验/创建所有产出分区版本（迟到的低版本不得成为当前产出）
        output_nodes: list[PNode] = []
        output_edges: list[tuple[int, int]] = []  # (output_partition_id, input_partition_id)
        for ref in req.outputs:
            node = _load_partition(conn, ref.partition_key, ref.version)
            if node is None:
                key = canonical_key(ref.partition_key)
                max_row = conn.execute(
                    "SELECT MAX(version) AS v FROM partitions WHERE partition_key = ?", (key,)
                ).fetchone()
                if max_row["v"] is not None and ref.version <= max_row["v"]:
                    raise DomainError(
                        409,
                        "OUTPUT_VERSION_NOT_NEWER",
                        f"迟到的低版本作业记录：{key}#v{ref.version} 不能替换当前版本 {max_row['v']}",
                    )
                cur = conn.execute(
                    "INSERT INTO partitions (partition_key, partition_type, version) VALUES (?, ?, ?)",
                    (key, ref.partition_type, ref.version),
                )
                node = PNode(cur.lastrowid, key, ref.partition_type, ref.version)
            else:
                if node.type != ref.partition_type:
                    raise DomainError(
                        409,
                        "TYPE_MISMATCH",
                        f"分区 {node.key}#v{node.version} 已登记类型为 {node.type}，与 {ref.partition_type} 冲突",
                    )
            if conn.execute(
                "SELECT 1 FROM job_outputs WHERE partition_id = ?", (node.id,)
            ).fetchone():
                raise DomainError(
                    409,
                    "OUTPUT_ALREADY_PRODUCED",
                    f"分区版本 {node.key}#v{node.version} 已有作业产出记录",
                )
            output_nodes.append(node)

            # 产出级输入映射：显式下标或保守地消费全部输入
            if ref.input_indices is None:
                edge_inputs = input_nodes
            else:
                if any(i >= len(input_nodes) for i in ref.input_indices):
                    raise DomainError(
                        422,
                        "INVALID_INPUT_INDEX",
                        f"产出 {node.key}#v{node.version} 的 input_indices 超出 inputs 范围",
                    )
                edge_inputs = [input_nodes[i] for i in ref.input_indices]
            output_edges.extend((node.id, inp.id) for inp in edge_inputs)

        # 3) 全部校验通过后统一写入（任一失败已在上面抛错并整体回滚）
        conn.execute(
            "INSERT INTO job_runs (run_id, job_name, job_type, finished_at) VALUES (?, ?, ?, ?)",
            (req.run_id, req.job_name, req.job_type, req.finished_at),
        )
        conn.executemany(
            "INSERT INTO job_inputs (run_id, partition_id, partition_key, version, ordinal)"
            " VALUES (?, ?, ?, ?, ?)",
            [
                (req.run_id, n.id, n.key, n.version, i)
                for i, n in enumerate(input_nodes)
            ],
        )
        conn.executemany(
            "INSERT INTO job_outputs (run_id, partition_id, partition_key, version, ordinal)"
            " VALUES (?, ?, ?, ?, ?)",
            [
                (req.run_id, n.id, n.key, n.version, i)
                for i, n in enumerate(output_nodes)
            ],
        )
        conn.executemany(
            "INSERT INTO job_output_inputs (run_id, output_partition_id, input_partition_id)"
            " VALUES (?, ?, ?)",
            [(req.run_id, out_id, in_id) for out_id, in_id in output_edges],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    info = get_job_run(conn, req.run_id)
    if info is None:  # 理论不可达
        raise DomainError(500, "INTERNAL", "作业登记后无法读回")
    return info


# --------------------------------------------------------------------------- #
# 读取
# --------------------------------------------------------------------------- #
def _load_partition(
    conn: sqlite3.Connection, partition_key: dict[str, Any], version: int
) -> PNode | None:
    row = conn.execute(
        "SELECT id, partition_key, partition_type, version FROM partitions"
        " WHERE partition_key = ? AND version = ?",
        (canonical_key(partition_key), version),
    ).fetchone()
    if row is None:
        return None
    return PNode(row["id"], row["partition_key"], row["partition_type"], row["version"])


def _row_to_info(conn: sqlite3.Connection, row: sqlite3.Row) -> PartitionInfo:
    import json

    current = conn.execute(
        "SELECT MAX(version) AS v FROM partitions WHERE partition_key = ?", (row["partition_key"],)
    ).fetchone()["v"]
    return PartitionInfo(
        partition_key=json.loads(row["partition_key"]),
        partition_type=row["partition_type"],
        version=row["version"],
        current_version=current,
        is_current=row["version"] == current,
        registered_at=row["registered_at"],
    )


def get_partition(
    conn: sqlite3.Connection, partition_key: dict[str, Any], version: int
) -> PartitionInfo:
    row = conn.execute(
        "SELECT * FROM partitions WHERE partition_key = ? AND version = ?",
        (canonical_key(partition_key), version),
    ).fetchone()
    if row is None:
        raise DomainError(404, "PARTITION_NOT_FOUND", "指定分区版本不存在")
    return _row_to_info(conn, row)


def list_partitions(
    conn: sqlite3.Connection, partition_key: dict[str, Any] | None = None
) -> list[PartitionInfo]:
    if partition_key is None:
        rows = conn.execute("SELECT * FROM partitions ORDER BY partition_key, version").fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM partitions WHERE partition_key = ? ORDER BY version",
            (canonical_key(partition_key),),
        ).fetchall()
    return [_row_to_info(conn, r) for r in rows]


def get_job_run(conn: sqlite3.Connection, run_id: str) -> JobRunInfo | None:
    run = conn.execute("SELECT * FROM job_runs WHERE run_id = ?", (run_id,)).fetchone()
    if run is None:
        return None
    import json

    inputs = [
        InputRef(partition_key=json.loads(r["partition_key"]), version=r["version"])
        for r in conn.execute(
            "SELECT * FROM job_inputs WHERE run_id = ? ORDER BY ordinal", (run_id,)
        ).fetchall()
    ]
    input_rows = conn.execute(
        "SELECT partition_id, ordinal FROM job_inputs WHERE run_id = ?", (run_id,)
    ).fetchall()
    input_index = {r["partition_id"]: r["ordinal"] for r in input_rows}
    outputs: list[OutputRef] = []
    for r in conn.execute(
        "SELECT jo.partition_id, jo.partition_key, jo.version, jo.ordinal, p.partition_type"
        " FROM job_outputs jo JOIN partitions p ON p.id = jo.partition_id"
        " WHERE jo.run_id = ? ORDER BY jo.ordinal",
        (run_id,),
    ).fetchall():
        edge_rows = conn.execute(
            "SELECT input_partition_id FROM job_output_inputs"
            " WHERE run_id = ? AND output_partition_id = ?",
            (run_id, r["partition_id"]),
        ).fetchall()
        indices = sorted(input_index[e["input_partition_id"]] for e in edge_rows)
        # 边覆盖全部输入时等价于“消费全部输入”，按约定回显为 null
        if indices and len(indices) == len(inputs):
            indices = None  # type: ignore[assignment]
        outputs.append(
            OutputRef(
                partition_key=json.loads(r["partition_key"]),
                version=r["version"],
                partition_type=r["partition_type"],
                input_indices=indices,
            )
        )
    return JobRunInfo(
        run_id=run["run_id"],
        job_name=run["job_name"],
        job_type=run["job_type"],
        finished_at=run["finished_at"],
        inputs=inputs,
        outputs=outputs,
    )


def list_job_runs(conn: sqlite3.Connection) -> list[JobRunInfo]:
    return [
        info
        for r in conn.execute("SELECT run_id FROM job_runs ORDER BY finished_at, run_id").fetchall()
        if (info := get_job_run(conn, r["run_id"])) is not None
    ]


# --------------------------------------------------------------------------- #
# 核心：过期判断与依赖路径
# --------------------------------------------------------------------------- #
def _load_graph(
    conn: sqlite3.Connection,
) -> tuple[
    dict[int, PNode],
    dict[str, int],
    dict[int, str],
    dict[str, list[PNode]],
    dict[int, list[PNode]],
]:
    """一次性读取血缘图，整个计算共享同一快照。

    返回：
      nodes            分区版本节点 id -> 节点
      current          分区键 -> 当前最大版本号
      producer         产出分区 id -> 作业 run_id
      run_inputs       run_id -> 该作业声明的全部输入（审计/兜底用）
      inputs_by_output 产出分区 id -> 该产出实际消费的输入节点列表
    """
    nodes: dict[int, PNode] = {}
    current: dict[str, int] = {}
    producer: dict[int, str] = {}  # partition_id -> run_id
    run_inputs: dict[str, list[PNode]] = {}
    edges: dict[int, list[PNode]] = {}

    conn.execute("BEGIN")  # 只读快照，保证计算期间不混入新登记
    try:
        for r in conn.execute("SELECT id, partition_key, partition_type, version FROM partitions"):
            node = PNode(r["id"], r["partition_key"], r["partition_type"], r["version"])
            nodes[node.id] = node
            if node.version > current.get(node.key, 0):
                current[node.key] = node.version
        for r in conn.execute("SELECT partition_id, run_id FROM job_outputs"):
            producer[r["partition_id"]] = r["run_id"]
        for r in conn.execute(
            "SELECT ji.run_id, p.id, p.partition_key, p.partition_type, p.version"
            " FROM job_inputs ji JOIN partitions p ON p.id = ji.partition_id"
            " ORDER BY ji.run_id, ji.ordinal"
        ):
            run_inputs.setdefault(r["run_id"], []).append(
                PNode(r["id"], r["partition_key"], r["partition_type"], r["version"])
            )
        for r in conn.execute(
            "SELECT oi.run_id, oi.output_partition_id, p.id, p.partition_key,"
            " p.partition_type, p.version"
            " FROM job_output_inputs oi JOIN partitions p ON p.id = oi.input_partition_id"
        ):
            edges.setdefault(r["output_partition_id"], []).append(
                PNode(r["id"], r["partition_key"], r["partition_type"], r["version"])
            )
    finally:
        conn.commit()  # 结束只读事务，释放快照

    # 产出级输入优先；若缺少边（历史数据/异常），保守回退到作业全部输入
    inputs_by_output: dict[int, list[PNode]] = {}
    for out_id, run_id in producer.items():
        inputs_by_output[out_id] = edges.get(out_id, run_inputs.get(run_id, []))
    return nodes, current, producer, run_inputs, inputs_by_output


def _is_replaced(node: PNode, current: dict[str, int]) -> bool:
    return node.version < current.get(node.key, node.version)


def compute_stale_outputs(conn: sqlite3.Connection) -> list[StaleOutput]:
    import json

    nodes, current, producer, _run_inputs, inputs_by_output = _load_graph(conn)

    stale_memo: dict[int, bool] = {}

    def upstream_of(node: PNode) -> list[PNode]:
        return inputs_by_output.get(node.id, [])

    def is_stale(node: PNode, seen: frozenset[int] = frozenset()) -> bool:
        """当前产出是否过期：所读版本被替换，或所读上游本身过期。"""
        if node.id in stale_memo:
            return stale_memo[node.id]
        if node.id in seen:  # 防御环
            return False
        run_id = producer.get(node.id)
        if run_id is None:  # 源头分区（门店原始日分区）不存在“过期产出”
            stale_memo[node.id] = False
            return False
        result = False
        for inp in upstream_of(node):
            if _is_replaced(inp, current) or is_stale(inp, seen | {node.id}):
                result = True
                break
        stale_memo[node.id] = result
        return result

    def to_path_node(node: PNode) -> PathNode:
        return PathNode(
            partition_key=json.loads(node.key),
            version=node.version,
            current_version=current.get(node.key, node.version),
            partition_type=node.type,
            is_raw=node.id not in producer,
            replaced=_is_replaced(node, current),
            producer_run_id=producer.get(node.id),
        )

    def find_paths(node: PNode, path: list[PNode], seen_keys: frozenset[str]) -> list[list[PNode]]:
        """沿该产出实际读取的输入向上，找到通往被替换源头分区的路径。"""
        if node.key in seen_keys:  # 环保护
            return []
        path = path + [node]
        seen_keys = seen_keys | {node.key}
        if node.id not in producer:
            return [path] if _is_replaced(node, current) else []
        results: list[list[PNode]] = []
        went_deeper = False
        for inp in upstream_of(node):
            if not _is_replaced(inp, current) and not is_stale(inp):
                continue  # 健康分支，不属于过期路径
            went_deeper = True
            results.extend(find_paths(inp, path, seen_keys))
        if not went_deeper and _is_replaced(node, current):
            # 被替换的派生版本且无法继续上溯：在此终止
            results.append(path)
        return results

    # 只评估各分区“当前版本”的派生产出；旧版本已被替换，不再影响新产出的判断
    latest_by_key: dict[str, PNode] = {}
    for node in nodes.values():
        cur = latest_by_key.get(node.key)
        if cur is None or node.version > cur.version:
            latest_by_key[node.key] = node
    stale_outputs: list[StaleOutput] = []
    for key, node in latest_by_key.items():
        run_id = producer.get(node.id)
        if run_id is None or not is_stale(node):
            continue
        reasons: set[str] = set()
        for inp in upstream_of(node):
            if _is_replaced(inp, current):
                reasons.add("input_replaced")
            elif is_stale(inp):
                reasons.add("upstream_stale")
        run_row = conn.execute(
            "SELECT job_name FROM job_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        paths = [[to_path_node(n) for n in p] for p in find_paths(node, [], frozenset())]
        stale_outputs.append(
            StaleOutput(
                partition_key=json.loads(key),
                partition_type=node.type,
                version=node.version,
                current_version=current[key],
                producer_run_id=run_id,
                producer_job_name=run_row["job_name"],
                reasons=sorted(reasons),  # type: ignore[arg-type]
                paths=paths,
            )
        )
    stale_outputs.sort(key=lambda s: (json.dumps(s.partition_key, sort_keys=True), s.version))
    return stale_outputs


def get_partition_lineage(
    conn: sqlite3.Connection, partition_key: dict[str, Any], version: int
) -> dict[str, Any]:
    """单个分区版本的血缘视图（含过期判断与路径），用于演示与排查。"""
    import json

    node = _load_partition(conn, partition_key, version)
    if node is None:
        raise DomainError(404, "PARTITION_NOT_FOUND", "指定分区版本不存在")
    nodes, current, producer, _run_inputs, inputs_by_output = _load_graph(conn)

    def upstream_of(n: PNode) -> list[PNode]:
        return inputs_by_output.get(n.id, [])

    stale_memo: dict[int, bool] = {}

    def is_stale(n: PNode, seen: frozenset[int] = frozenset()) -> bool:
        if n.id in stale_memo:
            return stale_memo[n.id]
        if n.id in seen:
            return False
        if n.id not in producer:
            stale_memo[n.id] = False
            return False
        result = any(
            _is_replaced(i, current) or is_stale(i, seen | {n.id})
            for i in upstream_of(n)
        )
        stale_memo[n.id] = result
        return result

    def to_path_node(n: PNode) -> PathNode:
        return PathNode(
            partition_key=json.loads(n.key),
            version=n.version,
            current_version=current.get(n.key, n.version),
            partition_type=n.type,
            is_raw=n.id not in producer,
            replaced=_is_replaced(n, current),
            producer_run_id=producer.get(n.id),
        )

    def find_paths(n: PNode, path: list[PNode], seen_keys: frozenset[str]) -> list[list[PNode]]:
        if n.key in seen_keys:
            return []
        path = path + [n]
        seen_keys = seen_keys | {n.key}
        if n.id not in producer:
            return [path] if _is_replaced(n, current) else []
        out: list[list[PNode]] = []
        deeper = False
        for i in upstream_of(n):
            if not _is_replaced(i, current) and not is_stale(i):
                continue
            deeper = True
            out.extend(find_paths(i, path, seen_keys))
        if not deeper and _is_replaced(n, current):
            out.append(path)
        return out

    run_id = producer.get(node.id)
    reasons: list[str] = []
    paths: list[list[PathNode]] = []
    if run_id is not None:
        reason_set: set[str] = set()
        for i in upstream_of(node):
            if _is_replaced(i, current):
                reason_set.add("input_replaced")
            elif is_stale(i):
                reason_set.add("upstream_stale")
        reasons = sorted(reason_set)
        paths = [[to_path_node(x) for x in p] for p in find_paths(node, [], frozenset())]

    return {
        "partition_key": json.loads(node.key),
        "partition_type": node.type,
        "version": node.version,
        "current_version": current.get(node.key, node.version),
        "is_current": node.version == current.get(node.key, node.version),
        "is_raw": run_id is None,
        "producer_run_id": run_id,
        "stale": is_stale(node),
        "reasons": reasons,
        "paths": paths,
    }
