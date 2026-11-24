"""订单抽样领域逻辑：快照登记、对外取样、抽样报告计算与领取签核。

业务边界（对外 / 对内）：

- 对外导出只包含订单号和商品组，金额与退款仅用于内部计算；
- 报告内部按**抽样快照**里的订单原值复核订单金额与退款小计；
- 同一抽样的订单号和金额必须来自同一个快照版本，迟到订单不能替换
  本轮已固定的快照；
- 退款字段只允许持有报告权限（``report:read``）的人读取；
- 并发签核只能生效一次。

排序根因在导出入口：未按原交易的数值金额完成排序便取样。这里统一走
``DataExport.select_rows``（先按数值金额排序取样，再裁剪字段），保证只含
订单号和商品组的文件与抽样报告引用同一批订单。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any

from app.models import DataExport, DataSnapshot, OlapQuery, sortable_value


# ============================================================
# 字段约定
# ============================================================

ORDER_ID_FIELD = "订单号"
AMOUNT_FIELD = "金额"
REFUND_FIELD = "退款"
GROUP_FIELD = "商品组"

#: 对外导出允许出现的字段，顺序即 CSV 列顺序。
PUBLIC_EXPORT_FIELDS = [ORDER_ID_FIELD, GROUP_FIELD]

#: 报告内部复核字段，任何对外输出都必须剔除。
INTERNAL_FIELDS = [AMOUNT_FIELD, REFUND_FIELD]

#: 读取退款字段所需的权限。
REPORT_PERMISSION = "report:read"


class SamplingError(Exception):
    """抽样业务错误：以可读消息返回给调用方。"""

    def __init__(self, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def _to_number(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError as exc:
        raise SamplingError(f"字段「{field_name}」不是数值：{value!r}") from exc


# ============================================================
# 快照登记
# ============================================================

@dataclass
class SamplingSnapshot:
    """抽样快照：保存取样时的完整订单行（含金额与退款）。

    快照一经登记不可原地修改；订单行是深拷贝，之后源数据的变化不会
    污染已登记的版本。
    """

    snapshot_id: str
    sampling_id: str
    version: int
    sampled_count: int
    rows: list[dict[str, Any]] = field(default_factory=list)
    created_at: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "sampling_id": self.sampling_id,
            "version": self.version,
            "sampled_count": self.sampled_count,
            "created_at": self.created_at,
        }

    def public_rows(self) -> list[dict[str, Any]]:
        """对外视图：只保留订单号和商品组。"""
        return [
            {key: row.get(key) for key in PUBLIC_EXPORT_FIELDS}
            for row in self.rows
        ]

    def to_data_snapshot(self) -> DataSnapshot:
        """转成分析快照模型，完整订单行（含金额与退款）随快照留存。"""
        return DataSnapshot(
            snapshot_id=self.snapshot_id,
            analysis_plan_id=self.sampling_id,
            data_rows=[dict(row) for row in self.rows],
            schema_summary=", ".join(PUBLIC_EXPORT_FIELDS),
            reason=f"抽样 {self.sampling_id} 第 {self.version} 版快照",
            retention_days=90,
            dependency_version=f"v{self.version}",
            created_at=self.created_at,
        )

    def report_rows(self) -> list[dict[str, Any]]:
        """报告内部视图：订单号 + 快照金额 + 退款，用于可复核计算。"""
        return [
            {
                ORDER_ID_FIELD: row.get(ORDER_ID_FIELD),
                AMOUNT_FIELD: _to_number(row.get(AMOUNT_FIELD), AMOUNT_FIELD),
                REFUND_FIELD: _to_number(row.get(REFUND_FIELD), REFUND_FIELD),
            }
            for row in self.rows
        ]


# ============================================================
# 仓库：快照 + 领取状态
# ============================================================

@dataclass
class SamplingRecord:
    """一次抽样的服务端状态：本轮固定快照 + 领取/签核状态。"""

    sampling_id: str
    latest_version: int = 0
    #: 本轮对外文件、报告与领取共同使用的快照版本。
    pinned_version: int = 0
    pinned_snapshot_id: str | None = None
    claimed: bool = False
    claimed_order_id: str | None = None
    claimed_amount: float | None = None
    claimed_refund: float | None = None
    claimed_version: int = 0
    signed: bool = False
    late_rows: list[dict[str, Any]] = field(default_factory=list)


class SamplingRegistry:
    """进程内抽样登记表，所有写操作都在同一把锁内完成。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._snapshots: dict[tuple[str, int], SamplingSnapshot] = {}
        self._records: dict[str, SamplingRecord] = {}

    # ---------- 快照登记 ----------

    def register_snapshot(
        self,
        sampling_id: str,
        rows: list[dict[str, Any]],
        *,
        created_at: str = "",
        pin: bool = True,
    ) -> SamplingSnapshot:
        """登记一版抽样快照，版本号在抽样内单调递增。

        ``pin=True`` 时把这一版设为本轮固定版本；如果本轮已经固定过，
        新版本只做归档，不会改变已固定的订单号/金额来源。
        """
        with self._lock:
            record = self._records.setdefault(sampling_id, SamplingRecord(sampling_id))
            version = record.latest_version + 1
            snapshot = SamplingSnapshot(
                snapshot_id=f"SNP_{sampling_id}_v{version}",
                sampling_id=sampling_id,
                version=version,
                sampled_count=len(rows),
                rows=[dict(row) for row in rows],
                created_at=created_at,
            )
            self._snapshots[(sampling_id, version)] = snapshot
            record.latest_version = version
            if pin and record.pinned_version == 0:
                record.pinned_version = version
                record.pinned_snapshot_id = snapshot.snapshot_id
            return snapshot

    def snapshot(self, sampling_id: str, version: int | None = None) -> SamplingSnapshot:
        """读取指定版本的快照；不传版本时使用本轮固定版本。"""
        with self._lock:
            record = self._records.get(sampling_id)
            if record is None:
                raise SamplingError(f"抽样 {sampling_id} 不存在", status_code=404)
            resolved = record.pinned_version if version is None else version
            if resolved == 0:
                raise SamplingError(f"抽样 {sampling_id} 还没有登记快照", status_code=409)
            snapshot = self._snapshots.get((sampling_id, resolved))
            if snapshot is None:
                raise SamplingError(
                    f"抽样 {sampling_id} 没有版本 v{resolved} 的快照", status_code=404
                )
            return snapshot

    def record(self, sampling_id: str) -> SamplingRecord:
        with self._lock:
            record = self._records.get(sampling_id)
            if record is None:
                raise SamplingError(f"抽样 {sampling_id} 不存在", status_code=404)
            return record

    def create_round(
        self,
        sampling_id: str,
        rows: list[dict[str, Any]],
        *,
        created_at: str = "",
    ) -> SamplingSnapshot:
        """登记一轮抽样的固定快照；已固定过的抽样拒绝重新取样。

        检查与登记在同一把锁里完成，两个并发的取请求只有一个能固定
        版本，另一个收到 409，不会出现两版快照并存导致的混用。
        """
        with self._lock:
            existing = self._records.get(sampling_id)
            if existing is not None and existing.pinned_version:
                raise SamplingError(
                    f"抽样 {sampling_id} 已固定快照 {existing.pinned_snapshot_id}，不能重复取样",
                    status_code=409,
                )
            return self.register_snapshot(sampling_id, rows, created_at=created_at)

    # ---------- 迟到订单 ----------

    def ingest_late_orders(
        self,
        sampling_id: str,
        rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """登记迟到订单，只写入迟到池，不改动本轮固定快照。

        迟到订单在同额次序上优先于本轮已取样订单，因此“如果此刻重新
        取样”可能选中另一张订单。这个结果只用于审计与提示冲突：本轮
        导出的文件、报告和领取仍以固定版本为准，订单号和金额不会被
        迟到订单替换。
        """
        with self._lock:
            record = self.record(sampling_id)
            pinned = self.snapshot(sampling_id)
            record.late_rows.extend(dict(row) for row in rows)
            # 迟到行排在前面：同额时后到的订单在重新取样中会排到最前。
            naive_rows = [*record.late_rows, *pinned.rows]
            naive_top, naive_amount = top_order_key(naive_rows)
            pinned_top, pinned_amount = top_order_key(pinned.rows)
            return {
                "sampling_id": sampling_id,
                "late_order_count": len(rows),
                "pinned_snapshot_id": pinned.snapshot_id,
                "pinned_snapshot_version": pinned.version,
                "pinned_top_order": pinned_top,
                "pinned_top_amount": pinned_amount,
                "resample_top_order": naive_top,
                "resample_top_amount": naive_amount,
                "conflict": naive_top != pinned_top,
                "claimed_order": record.claimed_order_id,
            }

    # ---------- 领取 / 签核 ----------

    def claim(self, sampling_id: str) -> dict[str, Any]:
        """按本轮固定快照的排序次序领取金额最大的待算订单。

        订单号和金额在锁内从同一个快照版本取出：迟到订单不会替换本轮
        已经固定的快照，也不会出现“订单号来自新版本、金额来自旧版本”。
        """
        with self._lock:
            record = self.record(sampling_id)
            if record.claimed:
                raise SamplingError(
                    f"抽样 {sampling_id} 已领取订单 {record.claimed_order_id}，不能重复领取",
                    status_code=409,
                )
            snapshot = self.snapshot(sampling_id)
            report_rows = snapshot.report_rows()
            if not report_rows:
                raise SamplingError(f"抽样 {sampling_id} 的快照没有可领取订单", status_code=409)
            top = max(
                report_rows,
                key=lambda row: sortable_value(row[AMOUNT_FIELD]),
            )
            record.claimed = True
            record.claimed_order_id = str(top[ORDER_ID_FIELD])
            record.claimed_amount = top[AMOUNT_FIELD]
            record.claimed_refund = top[REFUND_FIELD]
            record.claimed_version = snapshot.version
            return {
                "sampling_id": sampling_id,
                "snapshot_id": snapshot.snapshot_id,
                "snapshot_version": snapshot.version,
                ORDER_ID_FIELD: record.claimed_order_id,
                AMOUNT_FIELD: record.claimed_amount,
                REFUND_FIELD: record.claimed_refund,
            }

    def sign(self, sampling_id: str) -> dict[str, Any]:
        """签核领取结果；并发签核只能生效一次。"""
        with self._lock:
            record = self.record(sampling_id)
            if not record.claimed:
                raise SamplingError(f"抽样 {sampling_id} 尚未领取，不能签核", status_code=409)
            if record.signed:
                raise SamplingError(f"抽样 {sampling_id} 已签核，不能重复签核", status_code=409)
            record.signed = True
            return {
                "sampling_id": sampling_id,
                "signed": True,
                "snapshot_version": record.claimed_version,
                ORDER_ID_FIELD: record.claimed_order_id,
                AMOUNT_FIELD: record.claimed_amount,
                REFUND_FIELD: record.claimed_refund,
            }

    def reset(self) -> None:
        with self._lock:
            self._snapshots.clear()
            self._records.clear()


registry = SamplingRegistry()


# ============================================================
# 对外取样
# ============================================================

def build_export_rows(
    rows: list[dict[str, Any]],
    *,
    row_limit: int,
    include_amount: bool = False,
) -> list[dict[str, Any]]:
    """按原交易数值金额降序取前 N 个订单，再决定对外字段。

    对外（``include_amount=False``）只留订单号和商品组；报告内部复核
    （``include_amount=True``）额外带上金额与退款。排序与取样始终使用
    原始行的数值金额，不受字段裁剪影响。
    """
    if row_limit < 0:
        raise SamplingError("取样数量不能为负数")
    export = DataExport(
        task_id="SAMPLING",
        source_query=OlapQuery(query_id="SAMPLING", query_name="订单抽样"),
        field_selection=(
            [ORDER_ID_FIELD, GROUP_FIELD, AMOUNT_FIELD, REFUND_FIELD]
            if include_amount
            else list(PUBLIC_EXPORT_FIELDS)
        ),
        row_limit=row_limit,
        order_by=f"-{AMOUNT_FIELD}",
    )
    return export.build_row_subset(rows)


def sample_orders(
    sampling_id: str,
    rows: list[dict[str, Any]],
    *,
    row_limit: int,
    created_at: str = "",
) -> dict[str, Any]:
    """抽样入口：按金额取最大 N 个订单，登记快照并返回对外/内部视图。

    对外文件行与快照行来自同一次排序：先用数值金额排序取样，再分别
    裁剪字段，避免只含订单号和商品组的文件引用到错误的订单。同一
    抽样只允许固定一版快照，重复取样直接拒绝，防止订单号和金额跨版本
    混用。
    """
    public_rows = build_export_rows(rows, row_limit=row_limit, include_amount=False)
    detail_rows = build_export_rows(rows, row_limit=row_limit, include_amount=True)
    if [row[ORDER_ID_FIELD] for row in public_rows] != [
        row[ORDER_ID_FIELD] for row in detail_rows
    ]:
        raise SamplingError("对外订单号与快照订单号不一致，取样结果不可用")

    snapshot = registry.create_round(sampling_id, detail_rows, created_at=created_at)
    return {
        "sampling_id": sampling_id,
        "row_limit": row_limit,
        "export_columns": list(PUBLIC_EXPORT_FIELDS),
        "public_rows": public_rows,
        "detail_rows": detail_rows,
        "snapshot": snapshot.as_dict(),
    }


# ============================================================
# 抽样报告
# ============================================================

def build_sampling_report(
    sampling_id: str,
    *,
    include_refund: bool = False,
    permissions: set[str] | frozenset[str] | None = None,
    version: int | None = None,
) -> dict[str, Any]:
    """用抽样快照的订单原值计算抽样指标。

    指标恒为：订单金额合计、订单数；持有 ``report:read`` 权限并显式
    请求时才返回退款小计与净额。金额/退款只出现在报告内部计算里，对外
    列表只返回订单号与商品组。
    """
    can_read_refund = include_refund and REPORT_PERMISSION in (permissions or set())
    if include_refund and not can_read_refund:
        raise SamplingError(
            f"缺少「{REPORT_PERMISSION}」权限，不能读取退款字段",
            status_code=403,
        )

    snapshot = registry.snapshot(sampling_id, version)
    report_rows = snapshot.report_rows()
    public_rows = snapshot.public_rows()
    if [row[ORDER_ID_FIELD] for row in report_rows] != [
        row[ORDER_ID_FIELD] for row in public_rows
    ]:
        raise SamplingError("报告订单与对外订单不一致：订单号和金额必须来自同一快照")

    gross = sum(row[AMOUNT_FIELD] for row in report_rows)
    refund = sum(row[REFUND_FIELD] for row in report_rows)
    detail = []
    for row in report_rows:
        item = {
            ORDER_ID_FIELD: row[ORDER_ID_FIELD],
            AMOUNT_FIELD: row[AMOUNT_FIELD],
        }
        if can_read_refund:
            item[REFUND_FIELD] = row[REFUND_FIELD]
            item["净额"] = round(row[AMOUNT_FIELD] - row[REFUND_FIELD], 2)
        detail.append(item)

    metrics: dict[str, Any] = {
        "订单金额合计": round(gross, 2),
        "订单数": len(report_rows),
    }
    if can_read_refund:
        metrics["退款小计"] = round(refund, 2)
        metrics["净额"] = round(gross - refund, 2)

    return {
        "sampling_id": sampling_id,
        "snapshot_id": snapshot.snapshot_id,
        "snapshot_version": snapshot.version,
        "public_rows": public_rows,
        "detail_rows": detail,
        "metrics": metrics,
        "include_refund": can_read_refund,
    }


# ============================================================
# 工具
# ============================================================

def top_order_key(rows: list[dict[str, Any]]) -> tuple[str, float]:
    """按金额降序 + 稳定次序给出第一张订单的 (订单号, 金额)。"""
    ordered = sorted(
        rows,
        key=lambda row: sortable_value(row.get(AMOUNT_FIELD)),
        reverse=True,
    )
    if not ordered:
        return "", 0.0
    top = ordered[0]
    return str(top.get(ORDER_ID_FIELD)), _to_number(top.get(AMOUNT_FIELD), AMOUNT_FIELD)


def export_rows_to_csv(rows: list[dict[str, Any]], columns: list[str] | None = None) -> str:
    """把对外行渲染成 UTF-8-BOM CSV 文本，列顺序固定为订单号、商品组。"""
    import csv
    import io

    selected_columns = columns or PUBLIC_EXPORT_FIELDS
    buffer = io.StringIO()
    buffer.write("﻿")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(selected_columns)
    for row in rows:
        writer.writerow([row.get(column, "") for column in selected_columns])
    return buffer.getvalue()
