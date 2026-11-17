"""事务原子性与并发一致性。"""

import json
import threading

from conftest import CLEAN, CLEAN_2, RAW, RAW_2, register_raw


def test_multi_output_atomic_rollback_on_bad_input(client):
    register_raw(client, RAW, 1)
    before_runs = client.get("/api/v1/job-runs").json()
    before_parts = client.get("/api/v1/partitions").json()

    r = client.post(
        "/api/v1/job-runs",
        json={
            "run_id": "atomic-fail",
            "job_name": "monthly_summary",
            "job_type": "monthly_summary",
            "finished_at": "2026-11-04T00:00:00Z",
            # 第二个输入不存在 -> 整体失败
            "inputs": [
                {"partition_key": RAW, "version": 1},
                {"partition_key": {"dataset": "raw_sales", "store_id": "NOPE", "dt": "2026-10-01"},
                 "version": 1},
            ],
            "outputs": [
                {"partition_key": {"dataset": "monthly_sales", "store_id": "X", "month": "2026-10"},
                 "version": 1, "partition_type": "monthly_summary", "input_indices": [0]},
                {"partition_key": {"dataset": "monthly_sales", "store_id": "Y", "month": "2026-10"},
                 "version": 1, "partition_type": "monthly_summary", "input_indices": [1]},
            ],
        },
    )
    assert r.status_code == 422
    # 作业记录、输出分区版本均不得留下
    assert client.get("/api/v1/job-runs").json() == before_runs
    assert client.get("/api/v1/partitions").json() == before_parts


def test_multi_output_atomic_rollback_on_duplicate_output(client):
    register_raw(client, RAW, 1)
    register_raw(client, RAW_2, 1)
    # 第一次作业产出 CLEAN v1
    r = client.post(
        "/api/v1/job-runs",
        json={
            "run_id": "ok-run",
            "job_name": "daily_clean",
            "job_type": "clean",
            "finished_at": "2026-11-02T01:00:00Z",
            "inputs": [{"partition_key": RAW, "version": 1}],
            "outputs": [
                {"partition_key": CLEAN, "version": 1, "partition_type": "clean_daily"}
            ],
        },
    )
    assert r.status_code == 201

    # 第二次作业包含两个产出，其中 CLEAN v1 已被产出 -> 两个产出都不得登记
    r = client.post(
        "/api/v1/job-runs",
        json={
            "run_id": "dup-output-run",
            "job_name": "daily_clean",
            "job_type": "clean",
            "finished_at": "2026-11-02T03:00:00Z",
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
    assert r.status_code == 409
    assert r.json()["error_code"] == "OUTPUT_ALREADY_PRODUCED"
    run_ids = {x["run_id"] for x in client.get("/api/v1/job-runs").json()}
    assert run_ids == {"ok-run"}
    # CLEAN_2 的输出分区不得被自动创建
    part_rows = client.get(
        "/api/v1/partitions", params={"partition_key": json.dumps(CLEAN_2)}
    ).json()
    assert part_rows == []


def test_duplicate_refs_rejected_by_schema(client):
    register_raw(client, RAW, 1)
    r = client.post(
        "/api/v1/job-runs",
        json={
            "run_id": "bad-refs",
            "job_name": "daily_clean",
            "job_type": "clean",
            "finished_at": "2026-11-02T01:00:00Z",
            "inputs": [
                {"partition_key": RAW, "version": 1},
                {"partition_key": RAW, "version": 1},
            ],
            "outputs": [
                {"partition_key": CLEAN, "version": 1, "partition_type": "clean_daily"}
            ],
        },
    )
    assert r.status_code == 422


def test_concurrent_registration_serialized(client):
    """两个线程并发登记同一分区的 v2，只能一个成功，数据库不损坏。"""
    register_raw(client, RAW, 1)
    results: list[int] = []
    barrier = threading.Barrier(2)

    def worker():
        barrier.wait()
        r = register_raw(client, RAW, 2)
        results.append(r.status_code)

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    assert sorted(results) == [201, 409]
    rows = client.get(
        "/api/v1/partitions", params={"partition_key": json.dumps(RAW)}
    ).json()
    assert [p["version"] for p in rows] == [1, 2]
