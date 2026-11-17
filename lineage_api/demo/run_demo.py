#!/usr/bin/env python3
"""分区血缘 API 演示入口（通过真实 HTTP 调用走完整业务流程）。

用法：
    1. 启动服务： uvicorn app.main:app --port 8000
       （或：   docker compose up --build）
    2. 运行演示： python demo/run_demo.py --base-url http://127.0.0.1:8000

演示场景：
    门店 S01/S02 的 2026-10-01 原始日分区先登记 v1，清洗与月汇总依次产出；
    随后 S01 销售数据更正，原始分区登记 v2，查询立刻标出过期产出与依赖路径；
    重跑清洗（基于 v2）后，旧清洗产出的依赖不再影响新产出，但月汇总仍过期；
    再重跑月汇总，全部恢复健康；
    最后演示迟到的低版本作业记录被拒绝、以及多产出原子登记失败整体回滚。
"""

from __future__ import annotations

import argparse
import json
import sys

import httpx

STORE_DAY = {"dataset": "raw_sales", "store_id": "S01", "dt": "2026-10-01"}
STORE_DAY_2 = {"dataset": "raw_sales", "store_id": "S02", "dt": "2026-10-01"}
CLEAN_DAY = {"dataset": "clean_sales", "store_id": "S01", "dt": "2026-10-01"}
CLEAN_DAY_2 = {"dataset": "clean_sales", "store_id": "S02", "dt": "2026-10-01"}
MONTH = {"dataset": "monthly_sales", "store_id": "S01", "month": "2026-10"}
MONTH_2 = {"dataset": "monthly_sales", "store_id": "S02", "month": "2026-10"}
CLEAN_S09 = {"dataset": "clean_sales", "store_id": "S09", "dt": "2026-10-01"}


def pp(title: str, payload: object) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(description="分区血缘 API 演示")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    args = parser.parse_args()
    base = args.base_url.rstrip("/")

    with httpx.Client(base_url=base, timeout=10) as c:
        print(f"# 目标服务：{base}")
        pp("健康检查", c.get("/health").json())

        # 清空历史数据，保证演示可重复执行
        reset = c.post("/admin/reset")
        if reset.status_code != 200:
            raise RuntimeError(
                f"/admin/reset 返回 {reset.status_code}（可通过 LINEAGE_ENABLE_RESET=1 开启）"
            )
        print("# 已重置演示数据")

        # 1) 登记门店原始日分区 v1（两个门店）
        for key in (STORE_DAY, STORE_DAY_2):
            r = c.post(
                "/api/v1/partitions",
                json={"partition_key": key, "partition_type": "raw_daily", "version": 1},
            )
            r.raise_for_status()
        pp("登记原始分区 S01/S02 v1", r.json())

        # 2) 清洗作业：读取原始 v1，产出清洗日分区 v1（两个产出）
        r = c.post(
            "/api/v1/job-runs",
            json={
                "run_id": "clean-0001",
                "job_name": "daily_clean",
                "job_type": "clean",
                "finished_at": "2026-11-02T01:00:00Z",
                "inputs": [
                    {"partition_key": STORE_DAY, "version": 1},
                    {"partition_key": STORE_DAY_2, "version": 1},
                ],
                "outputs": [
                    {"partition_key": CLEAN_DAY, "version": 1, "partition_type": "clean_daily",
                     "input_indices": [0]},
                    {"partition_key": CLEAN_DAY_2, "version": 1, "partition_type": "clean_daily",
                     "input_indices": [1]},
                ],
            },
        )
        r.raise_for_status()
        pp("清洗作业 clean-0001（多产出原子登记）", r.json())

        # 3) 月汇总作业：读取清洗 v1，产出月汇总 v1
        r = c.post(
            "/api/v1/job-runs",
            json={
                "run_id": "monthly-0001",
                "job_name": "monthly_summary",
                "job_type": "monthly_summary",
                "finished_at": "2026-11-02T02:00:00Z",
                "inputs": [
                    {"partition_key": CLEAN_DAY, "version": 1},
                    {"partition_key": CLEAN_DAY_2, "version": 1},
                ],
                "outputs": [
                    {"partition_key": MONTH, "version": 1, "partition_type": "monthly_summary",
                     "input_indices": [0]},
                    {"partition_key": MONTH_2, "version": 1, "partition_type": "monthly_summary",
                     "input_indices": [1]},
                ],
            },
        )
        r.raise_for_status()
        pp("月汇总作业 monthly-0001", r.json())
        pp("初始状态：过期产出（期望为空）", c.get("/api/v1/stale-outputs").json())

        # 4) 销售数据更正：原始分区 S01 登记 v2
        r = c.post(
            "/api/v1/partitions",
            json={"partition_key": STORE_DAY, "partition_type": "raw_daily", "version": 2},
        )
        r.raise_for_status()
        pp("更正：原始分区 S01 登记 v2", r.json())
        pp("更正后：过期产出与依赖路径", c.get("/api/v1/stale-outputs").json())

        # 5) 重跑清洗（只重跑受影响的 S01），基于原始 v2 产出清洗 v2
        r = c.post(
            "/api/v1/job-runs",
            json={
                "run_id": "clean-0002",
                "job_name": "daily_clean",
                "job_type": "clean",
                "finished_at": "2026-11-03T01:00:00Z",
                "inputs": [{"partition_key": STORE_DAY, "version": 2}],
                "outputs": [
                    {"partition_key": CLEAN_DAY, "version": 2, "partition_type": "clean_daily"}
                ],
            },
        )
        r.raise_for_status()
        stale_after_clean = c.get("/api/v1/stale-outputs").json()
        pp("重跑清洗 clean-0002 后的过期产出（月汇总仍过期）", stale_after_clean)
        assert not any(
            o["partition_key"] == CLEAN_DAY for o in stale_after_clean["stale_outputs"]
        ), "新清洗产出不应再被旧依赖判为过期"

        # 6) 重跑月汇总，读取清洗 v2（S01）与未受影响的 v1（S02）
        r = c.post(
            "/api/v1/job-runs",
            json={
                "run_id": "monthly-0002",
                "job_name": "monthly_summary",
                "job_type": "monthly_summary",
                "finished_at": "2026-11-03T02:00:00Z",
                "inputs": [
                    {"partition_key": CLEAN_DAY, "version": 2},
                    {"partition_key": CLEAN_DAY_2, "version": 1},
                ],
                "outputs": [
                    {"partition_key": MONTH, "version": 2, "partition_type": "monthly_summary",
                     "input_indices": [0]},
                    {"partition_key": MONTH_2, "version": 2, "partition_type": "monthly_summary",
                     "input_indices": [1]},
                ],
            },
        )
        r.raise_for_status()
        final_stale = c.get("/api/v1/stale-outputs").json()
        pp("重跑月汇总 monthly-0002 后的过期产出（期望为空）", final_stale)
        assert final_stale["count"] == 0

        # 7) 迟到的低版本作业记录不能替换当前产出
        # 先让 S09 的清洗分区 v2 成为当前版本，使随后提交的 v1 作业成为“迟到低版本”
        c.post(
            "/api/v1/partitions",
            json={"partition_key": CLEAN_S09, "partition_type": "clean_daily", "version": 2},
        )
        r = c.post(
            "/api/v1/job-runs",
            json={
                "run_id": "clean-late-2",
                "job_name": "daily_clean",
                "job_type": "clean",
                "finished_at": "2026-10-31T00:00:00Z",
                "inputs": [{"partition_key": STORE_DAY, "version": 2}],
                "outputs": [
                    {"partition_key": CLEAN_S09, "version": 1, "partition_type": "clean_daily"}
                ],
            },
        )
        pp("迟到低版本作业（v1 < 当前 v2，期望 409）", {"status": r.status_code, **r.json()})
        assert r.status_code == 409

        # 已存在产出的分区版本同样不能被迟到作业再次登记
        r = c.post(
            "/api/v1/job-runs",
            json={
                "run_id": "clean-late-1",
                "job_name": "daily_clean",
                "job_type": "clean",
                "finished_at": "2026-10-31T00:00:00Z",
                "inputs": [{"partition_key": STORE_DAY, "version": 1}],
                "outputs": [
                    {"partition_key": CLEAN_DAY, "version": 1, "partition_type": "clean_daily"}
                ],
            },
        )
        pp("迟到作业重复登记已有产出（期望 409）", {"status": r.status_code, **r.json()})
        assert r.status_code == 409

        # 8) 多产出原子性：输入不存在导致失败，作业与所有产出整体回滚
        before = c.get("/api/v1/job-runs").json()
        r = c.post(
            "/api/v1/job-runs",
            json={
                "run_id": "atomic-fail",
                "job_name": "monthly_summary",
                "job_type": "monthly_summary",
                "finished_at": "2026-11-04T00:00:00Z",
                "inputs": [
                    {"partition_key": {"dataset": "raw_sales", "store_id": "NOPE",
                                       "dt": "2026-10-01"}, "version": 1}
                ],
                "outputs": [
                    {"partition_key": {"dataset": "monthly_sales", "store_id": "X",
                                       "month": "2026-10"}, "version": 1,
                     "partition_type": "monthly_summary"},
                    {"partition_key": {"dataset": "monthly_sales", "store_id": "Y",
                                       "month": "2026-10"}, "version": 1,
                     "partition_type": "monthly_summary"},
                ],
            },
        )
        pp("原子性：非法作业登记（期望 422 且无任何写入）", {"status": r.status_code, **r.json()})
        assert r.status_code == 422
        after = c.get("/api/v1/job-runs").json()
        assert before == after, "失败登记不应留下部分数据"
        run_ids = {x["run_id"] for x in after}
        assert "atomic-fail" not in run_ids
        print("\n全部演示断言通过 ✔")
        return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except httpx.HTTPError as exc:
        print(f"\n请求失败：{exc}\n请先启动服务（uvicorn app.main:app 或 docker compose up）", file=sys.stderr)
        raise SystemExit(2)
