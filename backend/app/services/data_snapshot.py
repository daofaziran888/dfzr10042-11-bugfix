"""分析快照领域服务：登记抽样快照，供导出、报告与领取复用同一版本。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.sampling import SamplingSnapshot


class DataSnapshotService(BaseModuleService):
    MODULE = "data_snapshot"
    ENTITY = "数据快照"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["快照编号", "分析方案", "数据行数", "Schema摘要", "快照原因", "保留天数", "依赖版本", "创建时间"]
    REQUIRED_FIELDS = ["快照编号", "分析方案", "数据行数"]
    STATUS_ORDER = ["创建中", "可用", "标记中", "已过期"]
    ACTION_RULES = {"创建快照": "可用", "标记版本": "标记中", "清理过期": "已过期"}
    NEGATIVE_ACTIONS = []

    def register_sampling_snapshot(
        self,
        snapshot: SamplingSnapshot,
        *,
        created_at: str = "",
    ) -> tuple[dict[str, Any], list[str]]:
        """把抽样快照登记到分析快照清单，返回登记结果与缺失字段。

        同一快照编号重复登记直接返回已有记录，避免报告与导出各自引用
        不同版本。完整订单行（含金额与退款）只保留在抽样快照存储里，
        清单记录行数、版本和 Schema 摘要。
        """
        from app.store import store

        payload = snapshot.to_data_snapshot()
        rows = store.rows(self.MODULE)
        for row in rows:
            if row.get("快照编号") == payload.snapshot_id:
                return row, []

        values = {
            "快照编号": payload.snapshot_id,
            "分析方案": payload.analysis_plan_id,
            "数据行数": str(len(payload.data_rows)),
            "Schema摘要": payload.schema_summary,
            "快照原因": payload.reason,
            "保留天数": str(payload.retention_days),
            "依赖版本": payload.dependency_version,
            "创建时间": created_at or payload.created_at,
        }
        entry, missing = self.create_entry(values)
        if entry is not None:
            entry["sampling_id"] = snapshot.sampling_id
            entry["snapshot_version"] = snapshot.version
        return entry or {}, missing
