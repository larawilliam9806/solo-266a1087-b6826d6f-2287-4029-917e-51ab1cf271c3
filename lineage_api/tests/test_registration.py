"""基础登记与校验规则。"""

import json

from conftest import RAW, register_raw


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_register_partition_and_current_flag(client):
    r = register_raw(client, RAW, 1)
    assert r.status_code == 201
    body = r.json()
    assert body["version"] == 1
    assert body["is_current"] is True
    assert body["current_version"] == 1

    r2 = register_raw(client, RAW, 2)
    assert r2.status_code == 201
    assert r2.json()["is_current"] is True

    # 旧版本仍可查询，且标记为非当前
    rows = client.get(
        "/api/v1/partitions", params={"partition_key": json.dumps(RAW)}
    ).json()
    assert [p["version"] for p in rows] == [1, 2]
    assert rows[0]["is_current"] is False
    assert rows[1]["is_current"] is True


def test_duplicate_version_rejected(client):
    assert register_raw(client, RAW, 1).status_code == 201
    r = register_raw(client, RAW, 1)
    assert r.status_code == 409
    assert r.json()["error_code"] == "VERSION_ALREADY_REGISTERED"


def test_version_must_be_incremental(client):
    assert register_raw(client, RAW, 2).status_code == 201
    r = register_raw(client, RAW, 1)
    assert r.status_code == 409
    assert r.json()["error_code"] == "VERSION_NOT_INCREMENTAL"
    # 被拒绝后没有产生任何新版本
    rows = client.get(
        "/api/v1/partitions", params={"partition_key": json.dumps(RAW)}
    ).json()
    assert [p["version"] for p in rows] == [2]


def test_partition_key_canonicalized(client):
    # 键顺序不同但语义相同，应视为同一分区
    r1 = client.post(
        "/api/v1/partitions",
        json={"partition_key": {"dt": "2026-10-01", "store_id": "S01", "dataset": "raw_sales"},
              "partition_type": "raw_daily", "version": 1},
    )
    r2 = register_raw(client, RAW, 1)
    assert r1.status_code == 201
    assert r2.status_code == 409
