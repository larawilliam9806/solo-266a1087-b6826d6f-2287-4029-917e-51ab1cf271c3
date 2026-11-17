"""HTTP 路由。"""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse

from . import service
from .models import (
    ErrorResponse,
    JobRunInfo,
    JobRunRegisterRequest,
    PartitionInfo,
    PartitionRegisterRequest,
    StaleResponse,
)
from .service import DomainError

router = APIRouter(prefix="/api/v1")


def get_conn(request: Request):
    # app.state.db 是可调用的会话工厂（见 main.create_app）
    return request.app.state.db()


def _parse_key(partition_key: str) -> dict[str, Any]:
    try:
        key = json.loads(partition_key)
    except json.JSONDecodeError as exc:
        raise DomainError(400, "INVALID_PARTITION_KEY", f"partition_key 必须是 JSON 对象字符串：{exc}")
    if not isinstance(key, dict) or not key:
        raise DomainError(400, "INVALID_PARTITION_KEY", "partition_key 必须是非空 JSON 对象")
    return key


class _Session:
    """轻量会话包装：每请求一个连接。"""

    def __init__(self, db_path: str | None) -> None:
        self._db_path = db_path

    def __call__(self):
        from .db import db_session

        return db_session(self._db_path)


@router.post(
    "/partitions",
    response_model=PartitionInfo,
    status_code=201,
    responses={409: {"model": ErrorResponse}},
    tags=["登记"],
    summary="登记一个分区版本（版本号由调用方递增分配）",
)
def register_partition(req: PartitionRegisterRequest, session=Depends(get_conn)):
    with session as conn:
        return service.register_partition(conn, req)


@router.get(
    "/partitions",
    response_model=list[PartitionInfo],
    tags=["查询"],
    summary="列出已登记分区版本，可按分区标识过滤",
)
def list_partitions(
    partition_key: str | None = Query(
        default=None, description='分区标识 JSON 字符串，如 \'{"store_id":"S01","dt":"2026-10-01"}\''
    ),
    session=Depends(get_conn),
):
    key = _parse_key(partition_key) if partition_key is not None else None
    with session as conn:
        return service.list_partitions(conn, key)


@router.get(
    "/partitions/lineage",
    tags=["查询"],
    summary="查询单个分区版本的血缘、是否过期及通往源头的依赖路径",
    responses={400: {"model": ErrorResponse}, 404: {"model": ErrorResponse}},
)
def partition_lineage(
    partition_key: str = Query(..., description="分区标识 JSON 字符串"),
    version: int = Query(..., ge=1),
    session=Depends(get_conn),
):
    key = _parse_key(partition_key)
    with session as conn:
        return service.get_partition_lineage(conn, key, version)


@router.post(
    "/job-runs",
    response_model=JobRunInfo,
    status_code=201,
    responses={409: {"model": ErrorResponse}, 422: {"model": ErrorResponse}},
    tags=["登记"],
    summary="登记一次作业完成记录（多产出原子提交，失败整体回滚）",
)
def register_job_run(req: JobRunRegisterRequest, session=Depends(get_conn)):
    with session as conn:
        return service.register_job_run(conn, req)


@router.get(
    "/job-runs",
    response_model=list[JobRunInfo],
    tags=["查询"],
    summary="列出作业完成记录",
)
def list_job_runs(session=Depends(get_conn)):
    with session as conn:
        return service.list_job_runs(conn)


@router.get(
    "/stale-outputs",
    response_model=StaleResponse,
    tags=["查询"],
    summary="列出当前过期产出及通往更正源头的依赖路径",
)
def stale_outputs(session=Depends(get_conn)):
    with session as conn:
        items = service.compute_stale_outputs(conn)
        return StaleResponse(stale_outputs=items, count=len(items))


def domain_error_handler(_request: Request, exc: DomainError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error_code=exc.error_code, message=exc.message
        ).model_dump(),
    )
