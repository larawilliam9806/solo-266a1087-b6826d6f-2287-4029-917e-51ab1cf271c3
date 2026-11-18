"""任务分派：提交 / 领取（有期限凭证）/ 完成回报 / 状态查询。

覆盖：
- 提交固定作业信息、实际输入与预定产出版本；重复提交幂等冲突
- 并发领取只成功一个；过期后可接管；重启后领取状态保留
- 仅当前未过期凭证可报完成；接管后的旧回报不得登记产出
- 输入被替换/过期 -> 拒绝登记并标记需重新安排
- 任务成功与多产出血缘登记原子生效，失败不留部分结果
- 成功回执丢失后原凭证重报返回原结果，不重复登记
"""

import json
import threading
import time

from conftest import CLEAN, CLEAN_2, RAW, RAW_2, register_clean_run, register_raw

TASK_ID = "rerun-clean-001"


def submit_task(client, task_id=TASK_ID, in_ver=1, out_ver=2, key_in=RAW, key_out=CLEAN):
    return client.post(
        "/api/v1/tasks",
        json={
            "task_id": task_id,
            "job_name": "daily_clean",
            "job_type": "clean",
            "inputs": [{"partition_key": key_in, "version": in_ver}],
            "outputs": [
                {"partition_key": key_out, "version": out_ver, "partition_type": "clean_daily"}
            ],
        },
    )


def claim(client, task_id=TASK_ID, executor="executor-a", lease_seconds=None):
    body = {"executor_id": executor}
    if lease_seconds is not None:
        body["lease_seconds"] = lease_seconds
    return client.post(f"/api/v1/tasks/{task_id}/claim", json=body)


def complete(client, task_id, token):
    return client.post(f"/api/v1/tasks/{task_id}/complete", json={"claim_token": token})


def get_task(client, task_id=TASK_ID):
    return client.get(f"/api/v1/tasks/{task_id}")


# --------------------------------------------------------------------------- #
# 提交与查询
# --------------------------------------------------------------------------- #
def test_submit_task_fixes_spec_and_is_queryable(client):
    register_raw(client, RAW, 1)
    r = submit_task(client)
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "PENDING"
    assert body["job_name"] == "daily_clean"
    assert body["inputs"][0]["version"] == 1
    assert body["outputs"][0]["version"] == 2
    assert body["lease"] is None and body["run_id"] is None

    r = get_task(client)
    assert r.status_code == 200
    assert r.json()["status"] == "PENDING"

    # 列表 + 状态过滤
    assert len(client.get("/api/v1/tasks").json()) == 1
    assert len(client.get("/api/v1/tasks", params={"status": "PENDING"}).json()) == 1
    assert client.get("/api/v1/tasks", params={"status": "SUCCEEDED"}).json() == []
    assert client.get("/api/v1/tasks", params={"status": "BOGUS"}).status_code == 400


def test_submit_duplicate_task_id_conflict(client):
    register_raw(client, RAW, 1)
    assert submit_task(client).status_code == 201
    r = submit_task(client)
    assert r.status_code == 409
    assert r.json()["error_code"] == "TASK_ID_EXISTS"


def test_submit_task_requires_registered_inputs(client):
    r = submit_task(client)  # 输入分区尚未登记
    assert r.status_code == 422
    assert r.json()["error_code"] == "INPUT_PARTITION_NOT_FOUND"
    assert get_task(client).status_code == 404


# --------------------------------------------------------------------------- #
# 领取：并发唯一、凭证保密、过期接管、重启保留
# --------------------------------------------------------------------------- #
def test_concurrent_claims_only_one_wins(client):
    register_raw(client, RAW, 1)
    submit_task(client)
    results: list[tuple[int, str]] = []
    barrier = threading.Barrier(4)

    def worker(name):
        barrier.wait()
        r = claim(client, executor=name)
        token = r.json().get("claim_token", "") if r.status_code == 200 else ""
        results.append((r.status_code, token))

    threads = [threading.Thread(target=worker, args=(f"exec-{i}",)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    codes = sorted(c for c, _ in results)
    assert codes == [200, 409, 409, 409]
    tokens = [t for _, t in results if t]
    assert len(tokens) == 1  # 只签发了一个凭证，不会重复分派
    # 失败响应说明持有者
    r = claim(client, executor="exec-late")
    assert r.status_code == 409
    assert r.json()["error_code"] == "TASK_ALREADY_CLAIMED"
    # 任务状态可查询，且不泄露凭证
    info = get_task(client).json()
    assert info["status"] == "CLAIMED"
    assert info["lease"]["executor_id"].startswith("exec-")
    assert info["lease"]["expired"] is False
    assert "claim_token" not in json.dumps(info)


def test_claim_validation_and_unknown_task(client):
    assert claim(client, task_id="nope").status_code == 404
    register_raw(client, RAW, 1)
    submit_task(client)
    r = claim(client, executor="")
    assert r.status_code == 422
    r = claim(client, executor="e", lease_seconds=0)
    assert r.status_code == 422


def test_lease_expiry_allows_takeover_and_old_token_rejected(client):
    register_raw(client, RAW, 1)
    submit_task(client)
    token_a = claim(client, executor="executor-a", lease_seconds=1).json()["claim_token"]

    # 租约未过期：他人不能接管
    assert claim(client, executor="executor-b").status_code == 409

    time.sleep(1.1)  # 等租约过期
    r = claim(client, executor="executor-b", lease_seconds=60)
    assert r.status_code == 200, r.json()
    token_b = r.json()["claim_token"]
    assert token_b != token_a

    # 接管后的旧回报不得登记产出
    r = complete(client, TASK_ID, token_a)
    assert r.status_code == 409
    assert r.json()["error_code"] == "CLAIM_TOKEN_SUPERSEDED"
    assert client.get("/api/v1/job-runs").json() == []

    # 新凭证可以完成
    r = complete(client, TASK_ID, token_b)
    assert r.status_code == 200, r.json()
    assert get_task(client).json()["status"] == "SUCCEEDED"


def test_expired_current_token_cannot_complete(client):
    register_raw(client, RAW, 1)
    submit_task(client)
    token = claim(client, lease_seconds=1).json()["claim_token"]
    time.sleep(1.1)
    r = complete(client, TASK_ID, token)
    assert r.status_code == 409
    assert r.json()["error_code"] == "LEASE_EXPIRED"
    assert client.get("/api/v1/job-runs").json() == []
    assert get_task(client).json()["status"] == "CLAIMED"  # 仍可被接管


def test_claim_state_survives_restart(tmp_path):
    """服务重启（新建应用实例、同一数据库文件）后领取状态保留。"""
    from fastapi.testclient import TestClient

    from app.main import create_app

    db_path = str(tmp_path / "restart.db")
    with TestClient(create_app(db_path)) as c1:
        register_raw(c1, RAW, 1)
        submit_task(c1)
        token = claim(c1, executor="executor-a").json()["claim_token"]

    # 模拟重启：同一 DB 文件新建应用
    with TestClient(create_app(db_path)) as c2:
        info = get_task(c2).json()
        assert info["status"] == "CLAIMED"
        assert info["lease"]["executor_id"] == "executor-a"
        # 他人仍不能重复领取
        assert claim(c2, executor="executor-b").status_code == 409
        # 原凭证重启后仍可报完成
        r = complete(c2, TASK_ID, token)
        assert r.status_code == 200
        assert get_task(c2).json()["status"] == "SUCCEEDED"


# --------------------------------------------------------------------------- #
# 完成：凭证校验、原子登记、幂等重报
# --------------------------------------------------------------------------- #
def test_complete_happy_path_registers_lineage_atomically(client):
    register_raw(client, RAW, 1)
    register_raw(client, RAW_2, 1)
    # 多产出任务：两个清洗分区分别消费两个输入
    r = client.post(
        "/api/v1/tasks",
        json={
            "task_id": "rerun-multi",
            "job_name": "daily_clean",
            "job_type": "clean",
            "inputs": [
                {"partition_key": RAW, "version": 1},
                {"partition_key": RAW_2, "version": 1},
            ],
            "outputs": [
                {"partition_key": CLEAN, "version": 1, "partition_type": "clean_daily",
                 "input_indices": [0]},
                {"partition_key": CLEAN_2, "version": 1, "partition_type": "clean_daily",
                 "input_indices": [1]},
            ],
        },
    )
    assert r.status_code == 201
    token = claim(client, task_id="rerun-multi").json()["claim_token"]
    r = complete(client, "rerun-multi", token)
    assert r.status_code == 200
    body = r.json()
    assert body["replayed"] is False
    assert body["run_id"] == "task:rerun-multi"
    assert len(body["job_run"]["outputs"]) == 2

    # 血缘已生效：产出可查询、产出级输入映射正确
    lineage = client.get(
        "/api/v1/partitions/lineage",
        params={"partition_key": json.dumps(CLEAN), "version": 1},
    ).json()
    assert lineage["producer_run_id"] == "task:rerun-multi"
    assert lineage["stale"] is False
    # 任务状态联动
    info = get_task(client, "rerun-multi").json()
    assert info["status"] == "SUCCEEDED"
    assert info["run_id"] == "task:rerun-multi"


def test_complete_requires_valid_current_token(client):
    register_raw(client, RAW, 1)
    submit_task(client)
    # 未领取就回报
    r = complete(client, TASK_ID, "whatever")
    assert r.status_code == 409
    assert r.json()["error_code"] == "TASK_NOT_CLAIMED"
    # 错误凭证
    claim(client)
    r = complete(client, TASK_ID, "wrong-token")
    assert r.status_code == 409
    assert r.json()["error_code"] == "CLAIM_TOKEN_INVALID"
    assert client.get("/api/v1/job-runs").json() == []


def test_complete_replay_returns_original_result(client):
    """成功回执丢失后，原凭证重报返回原结果，不重复登记。"""
    register_raw(client, RAW, 1)
    submit_task(client)
    token = claim(client).json()["claim_token"]
    r1 = complete(client, TASK_ID, token)
    assert r1.status_code == 200
    first = r1.json()

    runs_before = client.get("/api/v1/job-runs").json()
    parts_before = client.get("/api/v1/partitions").json()

    r2 = complete(client, TASK_ID, token)  # 重报
    assert r2.status_code == 200
    second = r2.json()
    assert second["replayed"] is True
    assert second["run_id"] == first["run_id"]
    assert second["job_run"] == first["job_run"]
    # 没有重复登记
    assert client.get("/api/v1/job-runs").json() == runs_before
    assert client.get("/api/v1/partitions").json() == parts_before

    # 其他凭证不能冒领已完成任务
    r = complete(client, TASK_ID, "someone-else")
    assert r.status_code == 409
    assert r.json()["error_code"] == "TASK_ALREADY_COMPLETED"
    # 已完成任务不能再被领取
    assert claim(client).status_code == 409
    assert claim(client).json()["error_code"] == "TASK_ALREADY_COMPLETED"


def test_complete_registration_failure_rolls_back_everything(client):
    """产出登记失败（版本冲突）时：不留部分结果，任务保持 CLAIMED 可重试。"""
    register_raw(client, RAW, 1)
    register_raw(client, RAW_2, 1)
    # 任务有两个产出；其中 CLEAN v1 已被别的作业产出
    register_clean_run(client, "other-run", 1, 1)
    r = client.post(
        "/api/v1/tasks",
        json={
            "task_id": "rerun-conflict",
            "job_name": "daily_clean",
            "job_type": "clean",
            "inputs": [
                {"partition_key": RAW, "version": 1},
                {"partition_key": RAW_2, "version": 1},
            ],
            "outputs": [
                {"partition_key": CLEAN, "version": 1, "partition_type": "clean_daily",
                 "input_indices": [0]},
                {"partition_key": CLEAN_2, "version": 1, "partition_type": "clean_daily",
                 "input_indices": [1]},
            ],
        },
    )
    assert r.status_code == 201
    token = claim(client, task_id="rerun-conflict").json()["claim_token"]

    parts_before = client.get("/api/v1/partitions").json()
    r = complete(client, "rerun-conflict", token)
    assert r.status_code == 409
    assert r.json()["error_code"] == "OUTPUT_ALREADY_PRODUCED"
    # 不留部分结果：CLEAN_2 分区未被创建，无新作业记录
    assert client.get("/api/v1/partitions").json() == parts_before
    assert {x["run_id"] for x in client.get("/api/v1/job-runs").json()} == {"other-run"}
    # 任务仍是 CLAIMED（凭证未消费），状态可查
    info = get_task(client, "rerun-conflict").json()
    assert info["status"] == "CLAIMED"


# --------------------------------------------------------------------------- #
# 输入失效：拒绝登记并标记需重新安排
# --------------------------------------------------------------------------- #
def test_stale_input_rejected_and_task_marked_for_reschedule(client):
    register_raw(client, RAW, 1)
    submit_task(client)  # 固定输入 RAW v1
    token = claim(client).json()["claim_token"]

    # 销售数据更正：RAW 登记 v2，任务固定的 v1 被替换
    register_raw(client, RAW, 2)

    runs_before = client.get("/api/v1/job-runs").json()
    r = complete(client, TASK_ID, token)
    assert r.status_code == 409
    body = r.json()
    assert body["error_code"] == "INPUTS_STALE"
    assert body["details"]["stale_inputs"][0]["reason"] == "input_replaced"
    assert body["details"]["stale_inputs"][0]["current_version"] == 2

    # 未登记任何产出
    assert client.get("/api/v1/job-runs").json() == runs_before
    rows = client.get("/api/v1/partitions", params={"partition_key": json.dumps(CLEAN)}).json()
    assert rows == []

    # 任务标记为需重新安排，且不可再领取/回报
    info = get_task(client).json()
    assert info["status"] == "NEEDS_RESCHEDULE"
    assert "input_replaced" in info["status_detail"]
    assert claim(client).status_code == 409
    assert claim(client).json()["error_code"] == "TASK_NEEDS_RESCHEDULE"
    assert complete(client, TASK_ID, token).status_code == 409

    # 基于新输入重新提交新任务后可正常完成
    assert submit_task(client, task_id="rerun-clean-002", in_ver=2, out_ver=2).status_code == 201
    token2 = claim(client, task_id="rerun-clean-002").json()["claim_token"]
    assert complete(client, "rerun-clean-002", token2).status_code == 200


def test_stale_input_via_upstream_chain_rejected(client):
    """输入本身过期（上游被替换）同样拒绝登记。"""
    register_raw(client, RAW, 1)
    register_clean_run(client, "clean-1", 1, 1)  # CLEAN v1 <- RAW v1
    # 任务固定输入为派生分区 CLEAN v1
    r = client.post(
        "/api/v1/tasks",
        json={
            "task_id": "rerun-monthly",
            "job_name": "monthly_summary",
            "job_type": "monthly_summary",
            "inputs": [{"partition_key": CLEAN, "version": 1}],
            "outputs": [
                {"partition_key": {"dataset": "monthly_sales", "store_id": "S01",
                                   "month": "2026-10"},
                 "version": 1, "partition_type": "monthly_summary"}
            ],
        },
    )
    assert r.status_code == 201
    token = claim(client, task_id="rerun-monthly").json()["claim_token"]
    register_raw(client, RAW, 2)  # 上游更正 -> CLEAN v1 过期

    r = complete(client, "rerun-monthly", token)
    assert r.status_code == 409
    assert r.json()["error_code"] == "INPUTS_STALE"
    assert r.json()["details"]["stale_inputs"][0]["reason"] == "input_stale"
    assert get_task(client, "rerun-monthly").json()["status"] == "NEEDS_RESCHEDULE"
