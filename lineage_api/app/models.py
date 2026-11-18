"""API 请求与响应模型。"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

# 分区版本必须为正整数（调用方递增分配，从 1 开始）。
Version = Field(ge=1)


class PartitionRegisterRequest(BaseModel):
    """登记一个分区版本（门店原始日分区，或作业产出的新版本）。"""

    partition_key: dict[str, Any] = Field(
        ..., description="分区标识，如 {\"store_id\": \"S01\", \"dt\": \"2026-11-01\"}"
    )
    partition_type: str = Field(..., min_length=1, description="分区类型，如 raw_daily / clean_daily")
    version: int = Version


class PartitionInfo(BaseModel):
    partition_key: dict[str, Any]
    partition_type: str
    version: int
    current_version: int = Field(..., description="该分区当前最大版本")
    is_current: bool
    registered_at: str


class InputRef(BaseModel):
    partition_key: dict[str, Any]
    version: int = Version


class OutputRef(BaseModel):
    partition_key: dict[str, Any]
    version: int = Version
    partition_type: str = Field(..., min_length=1)
    input_indices: list[int] | None = Field(
        default=None,
        description=(
            "该产出实际消费的输入在 inputs 中的下标列表；"
            "不填表示保守地消费作业的全部输入。"
            "多产出作业建议显式提供，以获得精确的产出级血缘与过期路径。"
        ),
    )

    @field_validator("input_indices")
    @classmethod
    def _validate_indices(cls, v: list[int] | None) -> list[int] | None:
        if v is None:
            return None
        if not v:
            raise ValueError("input_indices 不能为空列表；不填表示消费全部输入")
        if any(i < 0 for i in v):
            raise ValueError("input_indices 下标不能为负")
        if len(set(v)) != len(v):
            raise ValueError("input_indices 存在重复下标")
        return v


class _IOSpec(BaseModel):
    """作业输入/产出声明的公共部分（任务分派与直接登记共用校验规则）。"""

    inputs: list[InputRef] = Field(..., min_length=1)
    outputs: list[OutputRef] = Field(..., min_length=1)

    @field_validator("inputs", "outputs")
    @classmethod
    def _no_duplicate_refs(cls, refs: list[InputRef] | list[OutputRef]) -> list[Any]:
        seen: set[tuple[str, int]] = set()
        for ref in refs:
            key = (cls._key_str(ref.partition_key), ref.version)
            if key in seen:
                raise ValueError("同一分区版本在 inputs/outputs 中重复出现")
            seen.add(key)
        return refs

    @staticmethod
    def _key_str(partition_key: dict[str, Any]) -> str:
        import json

        return json.dumps(partition_key, sort_keys=True, ensure_ascii=False)


class JobRunRegisterRequest(_IOSpec):
    """登记一次清洗 / 月汇总等作业的完成记录（含实际读写的分区版本）。"""

    run_id: str = Field(..., min_length=1, description="作业运行唯一 ID，重复提交为幂等冲突")
    job_name: str = Field(..., min_length=1)
    job_type: str = Field(..., min_length=1)
    finished_at: str = Field(..., min_length=1)


class JobRunInfo(BaseModel):
    run_id: str
    job_name: str
    job_type: str
    finished_at: str
    inputs: list[InputRef]
    outputs: list[OutputRef]


class PathNode(BaseModel):
    """依赖路径上的一个节点（产出 -> 输入 方向上的一跳）。"""

    partition_key: dict[str, Any]
    version: int
    current_version: int
    partition_type: str
    is_raw: bool = Field(..., description="是否为无上游产出的源头分区（如门店原始日分区）")
    replaced: bool = Field(..., description="该版本是否已被同分区更高版本替换")
    producer_run_id: str | None = Field(..., description="产出该版本的作业运行 ID，源头分区为 null")


class StaleOutput(BaseModel):
    partition_key: dict[str, Any]
    partition_type: str
    version: int
    current_version: int
    producer_run_id: str
    producer_job_name: str
    reasons: list[Literal["input_replaced", "upstream_stale"]]
    # 每条路径从该过期产出开始，沿实际读取的输入向上，直到被替换的源头分区
    paths: list[list[PathNode]]


class StaleResponse(BaseModel):
    stale_outputs: list[StaleOutput]
    count: int


class ErrorResponse(BaseModel):
    error_code: str
    message: str
    details: dict[str, Any] | None = None


# --------------------------------------------------------------------------- #
# 任务分派（重跑作业 -> 外部执行器）
# --------------------------------------------------------------------------- #
TaskStatus = Literal["PENDING", "CLAIMED", "SUCCEEDED", "NEEDS_RESCHEDULE"]


class TaskSubmitRequest(_IOSpec):
    """提交一个待执行作业任务：固定作业信息、实际输入与预定产出版本。"""

    task_id: str = Field(..., min_length=1, description="任务唯一 ID，重复提交为幂等冲突")
    job_name: str = Field(..., min_length=1)
    job_type: str = Field(..., min_length=1)


class TaskClaimRequest(BaseModel):
    """执行器领取任务：返回有期限的完成凭证。"""

    executor_id: str = Field(..., min_length=1, description="执行器标识")
    lease_seconds: int | None = Field(
        default=None,
        ge=1,
        le=86400,
        description="凭证有效期（秒）；不填用服务端默认值 LINEAGE_TASK_LEASE_SECONDS",
    )


class LeaseInfo(BaseModel):
    executor_id: str
    claimed_at: str
    expires_at: str
    expired: bool = Field(..., description="按服务端当前时间判断是否已过期")


class TaskClaimResponse(BaseModel):
    task_id: str
    status: Literal["CLAIMED"]
    claim_token: str = Field(..., description="完成回报凭证，仅在本次响应中下发")
    executor_id: str
    claimed_at: str
    expires_at: str
    lease_seconds: int


class TaskCompleteRequest(BaseModel):
    """执行器回报完成：凭当前未过期凭证登记产出。"""

    claim_token: str = Field(..., min_length=1)
    finished_at: str | None = Field(
        default=None, min_length=1, description="作业完成时间（ISO 字符串）；不填取服务端当前时间"
    )


class TaskCompleteResponse(BaseModel):
    task_id: str
    status: Literal["SUCCEEDED"]
    run_id: str = Field(..., description="本次登记生成的作业运行 ID（task:<task_id>）")
    replayed: bool = Field(
        ..., description="true 表示成功回执丢失后的原凭证重报，直接返回原结果、未重复登记"
    )
    job_run: JobRunInfo


class TaskInfo(BaseModel):
    task_id: str
    status: TaskStatus
    job_name: str
    job_type: str
    inputs: list[InputRef]
    outputs: list[OutputRef]
    run_id: str | None = Field(..., description="成功后关联的作业运行 ID")
    lease: LeaseInfo | None = Field(..., description="最近一次领取信息（不含凭证）")
    status_detail: str | None = Field(..., description="状态诊断信息，如需重新安排的原因")
    created_at: str
    updated_at: str
