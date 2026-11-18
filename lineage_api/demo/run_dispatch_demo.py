#!/usr/bin/env python3
"""任务分派演示入口：把待重跑的销售清洗作业分派给外部执行器（真实 HTTP 调用）。

用法：
    1. 启动服务： uvicorn app.main:app --port 8000
       （或：   docker compose up --build）
    2. 运行演示： python demo/run_dispatch_demo.py --base-url http://127.0.0.1:8000
       （或：   docker compose --profile demo run --rm demo）

演示场景：
    S01 销售数据更正后，清洗作业需要重跑。工程师提交任务（固定作业信息、
    实际输入与预定产出版本），外部执行器领取有期限的凭证后回报完成：
      1) 并发领取只成功一个，不会重复分派；
      2) 错误凭证不得登记产出；
      3) 成功回报：任务状态与多产出血缘登记一并生效；
      4) 成功回执丢失后，原凭证重报返回原结果、不重复登记；
      5) 执行器宕机（租约过期）后被接管，旧凭证回报不得登记产出；
      6) 回报时输入已被替换 -> 拒绝登记，任务标记为需重新安排；
      7) 基于新输入重新提交任务后顺利完成，过期产出恢复健康。
"""

from __future__ import annotations

import argparse
import json
import sys
import time

import httpx

RAW = {"dataset": "raw_sales", "store_id": "S01", "dt": "2026-10-01"}
CLEAN = {"dataset": "clean_sales", "store_id": "S01", "dt": "2026-10-01"}


def pp(title: str, payload: object) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def register_raw(c: httpx.Client, version: int) -> None:
    r = c.post(
        "/api/v1/partitions",
        json={"partition_key": RAW, "partition_type": "raw_daily", "version": version},
    )
    r.raise_for_status()


def submit_clean_task(c: httpx.Client, task_id: str, in_ver: int, out_ver: int) -> None:
    r = c.post(
        "/api/v1/tasks",
        json={
            "task_id": task_id,
            "job_name": "daily_clean",
            "job_type": "clean",
            "inputs": [{"partition_key": RAW, "version": in_ver}],
            "outputs": [
                {"partition_key": CLEAN, "version": out_ver, "partition_type": "clean_daily"}
            ],
        },
    )
    r.raise_for_status()
    pp(f"提交任务 {task_id}（固定输入 raw v{in_ver} -> 预定产出 clean v{out_ver}）", r.json())


def claim(c: httpx.Client, task_id: str, executor: str, lease_seconds: int | None = None):
    body: dict = {"executor_id": executor}
    if lease_seconds is not None:
        body["lease_seconds"] = lease_seconds
    return c.post(f"/api/v1/tasks/{task_id}/claim", json=body)


def complete(c: httpx.Client, task_id: str, token: str):
    return c.post(f"/api/v1/tasks/{task_id}/complete", json={"claim_token": token})


def main() -> int:
    parser = argparse.ArgumentParser(description="任务分派演示")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    args = parser.parse_args()
    base = args.base_url.rstrip("/")

    with httpx.Client(base_url=base, timeout=10) as c:
        print(f"# 目标服务：{base}")
        pp("健康检查", c.get("/health").json())
        reset = c.post("/admin/reset")
        if reset.status_code != 200:
            raise RuntimeError(
                f"/admin/reset 返回 {reset.status_code}（可通过 LINEAGE_ENABLE_RESET=1 开启）"
            )
        print("# 已重置演示数据")

        # 0) 初始链路：raw v1 -> clean v1（直接登记，非任务分派）
        register_raw(c, 1)
        r = c.post(
            "/api/v1/job-runs",
            json={
                "run_id": "clean-init",
                "job_name": "daily_clean",
                "job_type": "clean",
                "finished_at": "2026-11-02T01:00:00Z",
                "inputs": [{"partition_key": RAW, "version": 1}],
                "outputs": [
                    {"partition_key": CLEAN, "version": 1, "partition_type": "clean_daily"}
                ],
            },
        )
        r.raise_for_status()

        # 1) 销售数据更正：raw 登记 v2，clean v1 过期 -> 需要重跑
        register_raw(c, 2)
        stale = c.get("/api/v1/stale-outputs").json()
        pp("更正后：过期产出（clean v1 待重跑）", stale)
        assert stale["count"] == 1

        # 2) 工程师提交重跑任务 T1：固定输入 raw v2、预定产出 clean v2
        submit_clean_task(c, "rerun-001", in_ver=2, out_ver=2)

        # 3) 并发领取只成功一个：执行器 B 抢到，执行器 A 再来已被拒
        r_a = claim(c, "rerun-001", "executor-a")
        r_b = claim(c, "rerun-001", "executor-b")
        pp("并发领取：A 成功 / B 被拒（期望 200 + 409）",
           {"A": r_a.status_code, "B": {"status": r_b.status_code, **r_b.json()}})
        assert r_a.status_code == 200 and r_b.status_code == 409
        token_a = r_a.json()["claim_token"]
        print(f"# A 获得凭证（有效期 {r_a.json()['lease_seconds']}s，{r_a.json()['expires_at']} 到期）")

        # 4) 错误凭证不得登记产出
        r = complete(c, "rerun-001", "forged-token")
        pp("伪造凭证回报（期望 409 CLAIM_TOKEN_INVALID）", {"status": r.status_code, **r.json()})
        assert r.status_code == 409

        # 5) A 凭有效凭证回报完成：任务状态与血缘登记一并生效
        r = complete(c, "rerun-001", token_a)
        assert r.status_code == 200
        pp("A 回报完成（任务 SUCCEEDED + 血缘已登记）", r.json())
        assert r.json()["replayed"] is False
        assert c.get("/api/v1/stale-outputs").json()["count"] == 0

        # 6) 成功回执丢失：A 用原凭证重报 -> 返回原结果，不重复登记
        runs_before = c.get("/api/v1/job-runs").json()
        r = complete(c, "rerun-001", token_a)
        pp("原凭证重报（期望 replayed=true，不重复登记）", r.json())
        assert r.status_code == 200 and r.json()["replayed"] is True
        assert r.json()["run_id"] == "task:rerun-001"
        assert c.get("/api/v1/job-runs").json() == runs_before

        # 7) 再次更正 raw v3 -> 提交 T2；执行器 C 领取后“宕机”，D 接管
        register_raw(c, 3)
        submit_clean_task(c, "rerun-002", in_ver=3, out_ver=3)
        token_c = claim(c, "rerun-002", "executor-c", lease_seconds=1).json()["claim_token"]
        print("# 执行器 C 领取后宕机（租约 1 秒），等待过期 ...")
        time.sleep(1.2)
        r = claim(c, "rerun-002", "executor-d")
        assert r.status_code == 200
        token_d = r.json()["claim_token"]
        pp("租约过期后 D 接管成功", r.json())

        # 8) 接管后的旧回报不得登记产出
        r = complete(c, "rerun-002", token_c)
        pp("C 的旧凭证回报（期望 409 CLAIM_TOKEN_SUPERSEDED）", {"status": r.status_code, **r.json()})
        assert r.status_code == 409 and r.json()["error_code"] == "CLAIM_TOKEN_SUPERSEDED"
        r = complete(c, "rerun-002", token_d)
        assert r.status_code == 200
        pp("D 回报完成", {"status": r.status_code, "run_id": r.json()["run_id"]})

        # 9) 更正 raw v4 -> 提交 T3；回报前又有新更正 raw v5 -> 输入失效
        register_raw(c, 4)
        submit_clean_task(c, "rerun-003", in_ver=4, out_ver=4)
        token_e = claim(c, "rerun-003", "executor-e").json()["claim_token"]
        register_raw(c, 5)  # 执行期间输入被替换
        r = complete(c, "rerun-003", token_e)
        pp("回报时输入已被替换（期望 409 INPUTS_STALE）", {"status": r.status_code, **r.json()})
        assert r.status_code == 409 and r.json()["error_code"] == "INPUTS_STALE"
        info = c.get("/api/v1/tasks/rerun-003").json()
        pp("任务已标记为需重新安排", info)
        assert info["status"] == "NEEDS_RESCHEDULE"
        # 旧任务不可再领取
        assert claim(c, "rerun-003", "executor-f").status_code == 409

        # 10) 工程师基于最新输入重新提交 T4（产出 clean v4 仍未被占用）并完成
        submit_clean_task(c, "rerun-004", in_ver=5, out_ver=4)
        token_f = claim(c, "rerun-004", "executor-f").json()["claim_token"]
        r = complete(c, "rerun-004", token_f)
        assert r.status_code == 200
        pp("重新安排的任务完成", {"status": r.status_code, "run_id": r.json()["run_id"]})

        # 11) 收尾：全部任务状态与最终血缘健康
        pp("全部任务状态", c.get("/api/v1/tasks").json())
        final = c.get("/api/v1/stale-outputs").json()
        pp("最终过期产出（期望为空）", final)
        assert final["count"] == 0
        lineage = c.get(
            "/api/v1/partitions/lineage",
            params={"partition_key": json.dumps(CLEAN), "version": 4},
        ).json()
        assert lineage["producer_run_id"] == "task:rerun-004" and lineage["stale"] is False

        print("\n全部演示断言通过 ✔")
        return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except httpx.HTTPError as exc:
        print(f"\n请求失败：{exc}\n请先启动服务（uvicorn app.main:app 或 docker compose up）", file=sys.stderr)
        raise SystemExit(2)
