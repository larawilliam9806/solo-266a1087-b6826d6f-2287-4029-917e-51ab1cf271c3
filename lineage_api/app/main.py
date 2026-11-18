"""FastAPI 应用入口。

启动：uvicorn app.main:app --reload
"""

from __future__ import annotations

import os

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from . import __version__
from .db import DB_PATH, init_db
from .models import ErrorResponse
from .routes import _Session, domain_error_handler, router
from .service import DomainError


def create_app(db_path: str | None = None) -> FastAPI:
    app = FastAPI(
        title="分区血缘 API（Partition Lineage API）",
        version=__version__,
        description=(
            "登记门店原始日分区版本与清洗/月汇总作业记录，"
            "保留各版实际读取与产出的分区版本，"
            "查询过期产出及通往更正源头的依赖路径，用于安排重跑；"
            "并支持把待重跑作业作为任务分派给外部执行器"
            "（提交任务 -> 领取有期限凭证 -> 凭凭证回报完成，产出登记与任务状态原子生效）。"
        ),
    )
    effective_db = db_path or os.environ.get("LINEAGE_DB_PATH", DB_PATH)
    init_db(effective_db)
    app.state.db = _Session(effective_db)
    app.include_router(router)
    app.add_exception_handler(DomainError, domain_error_handler)

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content=ErrorResponse(
                error_code="VALIDATION_ERROR",
                message="; ".join(
                    f"{'.'.join(str(x) for x in e['loc'])}: {e['msg']}" for e in exc.errors()
                ),
            ).model_dump(),
        )

    @app.get("/health", tags=["运维"], summary="健康检查")
    def health() -> dict[str, str]:
        return {"status": "ok", "version": __version__, "db": effective_db}

    @app.get("/", tags=["运维"], summary="服务信息")
    def root() -> dict[str, str]:
        return {
            "service": "partition-lineage-api",
            "version": __version__,
            "docs": "/docs",
            "health": "/health",
        }

    # 演示/集成测试用的重置接口；生产环境可通过 LINEAGE_ENABLE_RESET=0 关闭
    if os.environ.get("LINEAGE_ENABLE_RESET", "1") == "1":
        @app.post("/admin/reset", tags=["运维"], summary="清空全部血缘数据（演示/测试用）")
        def reset() -> dict[str, str]:
            from .db import get_connection

            conn = get_connection(effective_db)
            try:
                conn.execute("BEGIN IMMEDIATE")
                for table in (
                    "task_leases",
                    "tasks",
                    "job_output_inputs",
                    "job_outputs",
                    "job_inputs",
                    "job_runs",
                    "partitions",
                ):
                    conn.execute(f"DELETE FROM {table}")
                conn.commit()
            finally:
                conn.close()
            return {"status": "reset"}

    return app


app = create_app()
