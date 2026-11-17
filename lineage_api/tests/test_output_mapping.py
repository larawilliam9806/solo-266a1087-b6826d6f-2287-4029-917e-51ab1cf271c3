"""产出级血缘映射（input_indices）语义。"""

from conftest import CLEAN, CLEAN_2, MONTH, MONTH_2, RAW, RAW_2, register_raw


def _two_store_raw(client):
    register_raw(client, RAW, 1)
    register_raw(client, RAW_2, 1)


def test_explicit_input_indices_isolate_outputs(client):
    """显式映射下，S01 更正只株连 S01 的产出，不影响 S02。"""
    _two_store_raw(client)
    r = client.post(
        "/api/v1/job-runs",
        json={
            "run_id": "clean-1",
            "job_name": "daily_clean",
            "job_type": "clean",
            "finished_at": "2026-11-02T01:00:00Z",
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
    # 读回应包含 input_indices
    body = r.json()
    assert body["outputs"][0]["input_indices"] == [0]
    assert body["outputs"][1]["input_indices"] == [1]

    register_raw(client, RAW, 2)
    stale = client.get("/api/v1/stale-outputs").json()
    assert stale["count"] == 1
    assert stale["stale_outputs"][0]["partition_key"] == CLEAN


def test_missing_input_indices_conservatively_uses_all_inputs(client):
    """不声明映射时，保守地认为每个产出都消费全部输入（宁可多报不漏报）。"""
    _two_store_raw(client)
    r = client.post(
        "/api/v1/job-runs",
        json={
            "run_id": "clean-1",
            "job_name": "daily_clean",
            "job_type": "clean",
            "finished_at": "2026-11-02T01:00:00Z",
            "inputs": [
                {"partition_key": RAW, "version": 1},
                {"partition_key": RAW_2, "version": 1},
            ],
            "outputs": [
                {"partition_key": CLEAN, "version": 1, "partition_type": "clean_daily"},
                {"partition_key": CLEAN_2, "version": 1, "partition_type": "clean_daily"},
            ],
        },
    )
    assert r.status_code == 201
    # 全量消费时读回 input_indices 为 null
    assert all(o["input_indices"] is None for o in r.json()["outputs"])

    register_raw(client, RAW, 2)
    stale = client.get("/api/v1/stale-outputs").json()
    # 两个产出都被保守判为过期
    assert stale["count"] == 2
    keys = {tuple(sorted(o["partition_key"].items())) for o in stale["stale_outputs"]}
    assert tuple(sorted(CLEAN.items())) in keys
    assert tuple(sorted(CLEAN_2.items())) in keys


def test_invalid_input_index_rejected_and_rolled_back(client):
    _two_store_raw(client)
    r = client.post(
        "/api/v1/job-runs",
        json={
            "run_id": "bad-index",
            "job_name": "daily_clean",
            "job_type": "clean",
            "finished_at": "2026-11-02T01:00:00Z",
            "inputs": [{"partition_key": RAW, "version": 1}],
            "outputs": [
                {"partition_key": CLEAN, "version": 1, "partition_type": "clean_daily",
                 "input_indices": [5]}
            ],
        },
    )
    assert r.status_code == 422
    assert r.json()["error_code"] == "INVALID_INPUT_INDEX"
    assert client.get("/api/v1/job-runs").json() == []


def test_duplicate_and_negative_indices_rejected_by_schema(client):
    _two_store_raw(client)
    for bad in ([0, 0], [-1]):
        r = client.post(
            "/api/v1/job-runs",
            json={
                "run_id": f"bad-{bad}",
                "job_name": "daily_clean",
                "job_type": "clean",
                "finished_at": "2026-11-02T01:00:00Z",
                "inputs": [
                    {"partition_key": RAW, "version": 1},
                    {"partition_key": RAW_2, "version": 1},
                ],
                "outputs": [
                    {"partition_key": MONTH, "version": 1,
                     "partition_type": "monthly_summary", "input_indices": bad},
                    {"partition_key": MONTH_2, "version": 1,
                     "partition_type": "monthly_summary", "input_indices": [1]},
                ],
            },
        )
        assert r.status_code == 422
