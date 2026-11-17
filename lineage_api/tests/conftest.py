"""pytest 夹具：每个用例独立临时数据库 + FastAPI TestClient。"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.main import create_app  # noqa: E402


@pytest.fixture
def client(tmp_path):
    db_path = tmp_path / "test_lineage.db"
    app = create_app(str(db_path))
    with TestClient(app) as c:
        yield c


# 常用分区标识（dataset 区分原始/清洗/汇总数据集）
RAW = {"dataset": "raw_sales", "store_id": "S01", "dt": "2026-10-01"}
RAW_2 = {"dataset": "raw_sales", "store_id": "S02", "dt": "2026-10-01"}
CLEAN = {"dataset": "clean_sales", "store_id": "S01", "dt": "2026-10-01"}
CLEAN_2 = {"dataset": "clean_sales", "store_id": "S02", "dt": "2026-10-01"}
MONTH = {"dataset": "monthly_sales", "store_id": "S01", "month": "2026-10"}
MONTH_2 = {"dataset": "monthly_sales", "store_id": "S02", "month": "2026-10"}


def register_raw(client, key=RAW, version=1, ptype="raw_daily"):
    return client.post(
        "/api/v1/partitions",
        json={"partition_key": key, "partition_type": ptype, "version": version},
    )


def register_clean_run(client, run_id, in_ver, out_ver, key_in=RAW, key_out=CLEAN):
    return client.post(
        "/api/v1/job-runs",
        json={
            "run_id": run_id,
            "job_name": "daily_clean",
            "job_type": "clean",
            "finished_at": "2026-11-02T01:00:00Z",
            "inputs": [{"partition_key": key_in, "version": in_ver}],
            "outputs": [
                {"partition_key": key_out, "version": out_ver, "partition_type": "clean_daily"}
            ],
        },
    )
