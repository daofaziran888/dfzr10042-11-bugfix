"""订单抽样路由：取样、对外文件、抽样报告、领取与签核。

这些路径要先于通用 ``/api/{module}`` 资源路由注册，否则
``/api/sampling/orders/{id}`` 会被 ``/{module}/{entry_id}`` 抢占并因
``orders`` 不是整数而返回 422。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.services import service_for
from app.services.sampling import (
    PUBLIC_EXPORT_FIELDS,
    REPORT_PERMISSION,
    SamplingError,
    build_sampling_report,
    export_rows_to_csv,
    registry,
    sample_orders,
)

router = APIRouter(prefix="/api/sampling", tags=["订单抽样"])

REPORT_PERMISSION_HEADER = "X-Report-Permission"


class OrderSamplingPayload(BaseModel):
    """订单抽样请求：服务端按数值金额排序取样并登记快照。"""

    sampling_id: str = Field(min_length=1)
    row_limit: int = Field(default=1, ge=1)
    created_at: str = ""
    rows: list[dict[str, Any]] = Field(default_factory=list)


class LateOrderPayload(BaseModel):
    """迟到订单：只登记到迟到池，不改动本轮固定快照。"""

    rows: list[dict[str, Any]] = Field(default_factory=list)


def _permissions(request: Request) -> set[str]:
    raw = request.headers.get(REPORT_PERMISSION_HEADER, "")
    return {item.strip() for item in raw.split(",") if item.strip()}


def _call(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except SamplingError as error:
        raise HTTPException(status_code=error.status_code, detail=error.message) from error


@router.post("/orders")
def create_order_sampling(payload: OrderSamplingPayload) -> dict[str, Any]:
    """按原交易数值金额降序取样，登记抽样快照，返回对外行与快照信息。"""
    result = _call(
        sample_orders,
        payload.sampling_id,
        payload.rows,
        row_limit=payload.row_limit,
        created_at=payload.created_at,
    )
    # 抽样快照同时登记到「分析快照」清单，保留清单视图与审计信息。
    snapshot = _call(registry.snapshot, payload.sampling_id)
    snapshot_service = service_for("data_snapshot")
    entry, _ = snapshot_service.register_sampling_snapshot(
        snapshot, created_at=payload.created_at
    )
    result["snapshot_entry"] = entry
    return result


@router.get("/orders/{sampling_id}/export")
def export_order_sampling(sampling_id: str) -> StreamingResponse:
    """对外导出：文件只包含订单号和商品组，不含金额与退款。"""
    snapshot = _call(registry.snapshot, sampling_id)
    return StreamingResponse(
        iter([export_rows_to_csv(snapshot.public_rows(), PUBLIC_EXPORT_FIELDS)]),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{sampling_id}.csv"',
            "X-Snapshot-Id": snapshot.snapshot_id,
        },
    )


@router.get("/orders/{sampling_id}/report")
def get_sampling_report(
    sampling_id: str,
    request: Request,
    include_refund: bool = Query(default=False),
    version: int | None = Query(default=None),
) -> dict[str, Any]:
    """抽样报告：用快照订单原值计算；退款字段需要报告权限。"""
    return _call(
        build_sampling_report,
        sampling_id,
        include_refund=include_refund,
        permissions=_permissions(request),
        version=version,
    )


@router.post("/orders/{sampling_id}/late")
def register_late_orders(sampling_id: str, payload: LateOrderPayload) -> dict[str, Any]:
    """登记迟到订单；本轮固定快照、报告与领取范围保持同一版本。"""
    return _call(registry.ingest_late_orders, sampling_id, payload.rows)


@router.post("/orders/{sampling_id}/claim")
def claim_sampling_order(sampling_id: str) -> dict[str, Any]:
    """领取本轮快照中金额最大的待算订单（订单号与金额同版本）。"""
    return _call(registry.claim, sampling_id)


@router.post("/orders/{sampling_id}/sign")
def sign_sampling_order(sampling_id: str) -> dict[str, Any]:
    """签核领取结果；并发签核只生效一次。"""
    return _call(registry.sign, sampling_id)


@router.get("/orders/{sampling_id}")
def get_order_sampling(sampling_id: str, request: Request) -> dict[str, Any]:
    """读取抽样状态：固定快照版本、对外行、领取与签核状态。

    退款数值只在持有报告权限时回显，无权限时只给订单号和金额。
    """
    record = _call(registry.record, sampling_id)
    snapshot = _call(registry.snapshot, sampling_id)
    can_read_refund = REPORT_PERMISSION in _permissions(request)
    result = {
        "sampling_id": sampling_id,
        "snapshot_id": snapshot.snapshot_id,
        "snapshot_version": snapshot.version,
        "public_rows": snapshot.public_rows(),
        "claimed_order": record.claimed_order_id,
        "claimed_amount": record.claimed_amount,
        "claimed_version": record.claimed_version,
        "signed": record.signed,
        "late_order_count": len(record.late_rows),
    }
    if can_read_refund:
        result["claimed_refund"] = record.claimed_refund
    return result
