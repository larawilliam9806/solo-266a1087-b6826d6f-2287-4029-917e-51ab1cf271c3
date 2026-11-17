"""核心场景：过期判断、依赖路径、重跑后旧依赖失效、迟到低版本。"""

import json

from conftest import (
    CLEAN,
    CLEAN_2,
    MONTH,
    MONTH_2,
    RAW,
    RAW_2,
    register_clean_run,
    register_raw,
)


def _setup_pipeline(client):
    """原始 v1 -> 清洗 v1 -> 月汇总 v1 的两层血缘。"""
    register_raw(client, RAW, 1)
    register_raw(client, RAW_2, 1)
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
    r = client.post(
        "/api/v1/job-runs",
        json={
            "run_id": "month-1",
            "job_name": "monthly_summary",
            "job_type": "monthly_summary",
            "finished_at": "2026-11-02T02:00:00Z",
            "inputs": [
                {"partition_key": CLEAN, "version": 1},
                {"partition_key": CLEAN_2, "version": 1},
            ],
            "outputs": [
                {"partition_key": MONTH, "version": 1, "partition_type": "monthly_summary",
                 "input_indices": [0]},
                {"partition_key": MONTH_2, "version": 1, "partition_type": "monthly_summary",
                 "input_indices": [1]},
            ],
        },
    )
    assert r.status_code == 201


def test_healthy_pipeline_has_no_stale_output(client):
    _setup_pipeline(client)
    body = client.get("/api/v1/stale-outputs").json()
    assert body["count"] == 0
    assert body["stale_outputs"] == []


def test_correction_marks_derived_outputs_stale_with_paths(client):
    _setup_pipeline(client)
    # 销售数据更正：S01 原始分区登记 v2
    register_raw(client, RAW, 2)

    body = client.get("/api/v1/stale-outputs").json()
    assert body["count"] == 2  # S01 的清洗 v1 与月汇总 v1
    keys = {tuple(sorted(o["partition_key"].items())) for o in body["stale_outputs"]}
    assert tuple(sorted(CLEAN.items())) in keys
    assert tuple(sorted(MONTH.items())) in keys

    clean_stale = next(o for o in body["stale_outputs"] if o["partition_key"] == CLEAN)
    assert clean_stale["reasons"] == ["input_replaced"]
    # 路径从过期产出通向被替换的源头原始分区
    path = clean_stale["paths"][0]
    assert [n["version"] for n in path] == [1, 1]
    assert [n["partition_key"] for n in path] == [CLEAN, RAW]
    assert path[-1]["is_raw"] is True
    assert path[-1]["replaced"] is True
    assert path[0]["producer_run_id"] == "clean-1"

    month_stale = next(o for o in body["stale_outputs"] if o["partition_key"] == MONTH)
    assert "upstream_stale" in month_stale["reasons"]
    # 月汇总 -> 清洗 -> 原始 的完整路径
    path = month_stale["paths"][0]
    assert [n["partition_key"] for n in path] == [MONTH, CLEAN, RAW]
    assert path[-1]["is_raw"] is True
    # S02 未更正，不应出现其路径
    assert all(
        all(n["partition_key"] != CLEAN_2 for n in p) for p in month_stale["paths"]
    )


def test_rerun_clean_drops_old_dependency_but_monthly_remains_stale(client):
    _setup_pipeline(client)
    register_raw(client, RAW, 2)

    # 重跑清洗：基于原始 v2 产出清洗 v2
    r = register_clean_run(client, "clean-2", in_ver=2, out_ver=2)
    assert r.status_code == 201

    body = client.get("/api/v1/stale-outputs").json()
    stale_keys = {tuple(sorted(o["partition_key"].items())) for o in body["stale_outputs"]}
    # 新清洗产出不再过期；月汇总仍读取清洗 v1，依旧过期
    assert tuple(sorted(CLEAN.items())) not in stale_keys
    assert tuple(sorted(MONTH.items())) in stale_keys

    month_stale = next(o for o in body["stale_outputs"] if o["partition_key"] == MONTH)
    # 月汇总 -> 被替换的清洗 v1 -> 被替换的原始 v1，完整上溯到更正源头
    path = month_stale["paths"][0]
    assert [n["partition_key"] for n in path] == [MONTH, CLEAN, RAW]
    assert [n["version"] for n in path] == [1, 1, 1]
    assert path[1]["is_raw"] is False
    assert path[1]["replaced"] is True
    assert path[-1]["is_raw"] is True
    assert path[-1]["replaced"] is True


def test_rerun_monthly_restores_healthy_state(client):
    _setup_pipeline(client)
    register_raw(client, RAW, 2)
    register_clean_run(client, "clean-2", in_ver=2, out_ver=2)

    r = client.post(
        "/api/v1/job-runs",
        json={
            "run_id": "month-2",
            "job_name": "monthly_summary",
            "job_type": "monthly_summary",
            "finished_at": "2026-11-03T02:00:00Z",
            "inputs": [
                {"partition_key": CLEAN, "version": 2},
                {"partition_key": CLEAN_2, "version": 1},
            ],
            "outputs": [
                {"partition_key": MONTH, "version": 2, "partition_type": "monthly_summary",
                 "input_indices": [0]},
                {"partition_key": MONTH_2, "version": 2, "partition_type": "monthly_summary",
                 "input_indices": [1]},
            ],
        },
    )
    assert r.status_code == 201
    body = client.get("/api/v1/stale-outputs").json()
    assert body["count"] == 0


def test_late_lower_version_job_cannot_replace_current_output(client):
    _setup_pipeline(client)
    register_raw(client, RAW, 2)
    register_clean_run(client, "clean-2", in_ver=2, out_ver=2)

    clean_s09 = {"dataset": "clean_sales", "store_id": "S09", "dt": "2026-10-01"}
    # S09 清洗分区 v2 先成为当前版本（例如更正后的正常重跑已产出更高版本占位）
    register_raw(client, clean_s09, 2, ptype="clean_daily")

    # 迟到的低版本作业记录试图补登 v1（v1 < 当前 v2），必须被拒绝
    r = client.post(
        "/api/v1/job-runs",
        json={
            "run_id": "clean-late-2",
            "job_name": "daily_clean",
            "job_type": "clean",
            "finished_at": "2026-10-31T00:00:00Z",
            "inputs": [{"partition_key": RAW, "version": 1}],
            "outputs": [
                {"partition_key": clean_s09, "version": 1, "partition_type": "clean_daily"}
            ],
        },
    )
    assert r.status_code == 409
    assert r.json()["error_code"] == "OUTPUT_VERSION_NOT_NEWER"
    # v1 未被创建，当前版本仍是 v2，未被替换
    info = client.get(
        "/api/v1/partitions/lineage",
        params={"partition_key": json.dumps(clean_s09), "version": 2},
    ).json()
    assert info["is_current"] is True
    assert info["producer_run_id"] is None  # 占位分区没有作业产出记录


def test_duplicate_run_id_rejected(client):
    register_raw(client, RAW, 1)
    r1 = register_clean_run(client, "dup-run", 1, 1)
    assert r1.status_code == 201
    r2 = register_clean_run(client, "dup-run", 1, 2)
    assert r2.status_code == 409
    assert r2.json()["error_code"] == "RUN_ID_EXISTS"


def test_input_must_exist(client):
    r = register_clean_run(client, "ghost", 9, 1)
    assert r.status_code == 422
    assert r.json()["error_code"] == "INPUT_PARTITION_NOT_FOUND"


def test_single_partition_lineage_endpoint(client):
    _setup_pipeline(client)
    register_raw(client, RAW, 2)
    r = client.get(
        "/api/v1/partitions/lineage",
        params={"partition_key": json.dumps(MONTH), "version": 1},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["stale"] is True
    assert body["producer_run_id"] == "month-1"
    assert body["paths"][0][-1]["is_raw"] is True
    assert body["paths"][0][-1]["partition_key"] == RAW
