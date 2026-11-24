"""订单抽样：数值排序、快照登记、抽样报告、领取与签核的回归测试。"""
from __future__ import annotations

import csv
import io
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.models import DataExport, OlapQuery
from app.services.data_snapshot import DataSnapshotService
from app.services.sampling import (
    AMOUNT_FIELD,
    GROUP_FIELD,
    ORDER_ID_FIELD,
    REFUND_FIELD,
    SamplingError,
    build_export_rows,
    build_sampling_report,
    export_rows_to_csv,
    registry,
    sample_orders,
)

ORDERS = [
    {ORDER_ID_FIELD: "A", GROUP_FIELD: "手机", AMOUNT_FIELD: 9, REFUND_FIELD: 2},
    {ORDER_ID_FIELD: "B", GROUP_FIELD: "家电", AMOUNT_FIELD: 10, REFUND_FIELD: 0},
]


@pytest.fixture(autouse=True)
def clean_registry():
    registry.reset()
    yield
    registry.reset()


def _export(order_by: str, fields: list[str], row_limit: int = 10) -> list[dict]:
    return DataExport(
        task_id="T",
        source_query=OlapQuery(query_id="Q", query_name="q"),
        field_selection=fields,
        row_limit=row_limit,
        order_by=order_by,
    ).build_row_subset([dict(row) for row in ORDERS])


# ============================================================
# 排序根因：数值金额排序 + 先排序后裁剪字段
# ============================================================

def test_amount_sorted_numerically_not_lexically():
    assert [row[ORDER_ID_FIELD] for row in _export("-金额", fields=[])] == ["B", "A"]


def test_sort_still_works_when_amount_column_is_omitted():
    rows = _export("金额 DESC", fields=[ORDER_ID_FIELD, GROUP_FIELD])
    assert [row[ORDER_ID_FIELD] for row in rows] == ["B", "A"]
    assert all(set(row) == {ORDER_ID_FIELD, GROUP_FIELD} for row in rows)


def test_sample_uses_original_numeric_amount_before_field_clip():
    rows = build_export_rows(ORDERS, row_limit=1, include_amount=False)
    assert rows == [{ORDER_ID_FIELD: "B", GROUP_FIELD: "家电"}]


# ============================================================
# 抽样与报告
# ============================================================

def test_sample_keeps_only_order_id_and_group():
    result = sample_orders("S1", ORDERS, row_limit=1)
    assert result["public_rows"] == [{ORDER_ID_FIELD: "B", GROUP_FIELD: "家电"}]
    assert result["export_columns"] == [ORDER_ID_FIELD, GROUP_FIELD]


def test_snapshot_stores_full_order_values():
    result = sample_orders("S1", ORDERS, row_limit=1)
    assert result["detail_rows"][0][AMOUNT_FIELD] == 10
    assert result["detail_rows"][0][REFUND_FIELD] == 0
    assert result["snapshot"]["sampled_count"] == 1


def test_same_sampling_cannot_restart_with_another_snapshot():
    sample_orders("S1", ORDERS, row_limit=1)
    with pytest.raises(SamplingError) as excinfo:
        sample_orders("S1", ORDERS[:1], row_limit=1)
    assert excinfo.value.status_code == 409
    assert registry.snapshot("S1").version == 1


def test_report_computes_from_snapshot_values():
    sample_orders("S1", ORDERS, row_limit=1)
    report = build_sampling_report(
        "S1", include_refund=True, permissions={"report:read"}
    )
    assert report["metrics"]["订单金额合计"] == 10
    assert report["metrics"]["退款小计"] == 0
    assert report["metrics"]["净额"] == 10
    assert report["public_rows"] == [{ORDER_ID_FIELD: "B", GROUP_FIELD: "家电"}]


def test_report_refuses_refund_without_permission():
    sample_orders("S1", ORDERS, row_limit=1)
    with pytest.raises(SamplingError) as excinfo:
        build_sampling_report("S1", include_refund=True, permissions=set())
    assert excinfo.value.status_code == 403
    # 没有权限时报告仍可给出金额，但不泄露退款。
    report = build_sampling_report("S1", include_refund=False)
    assert AMOUNT_FIELD not in report["public_rows"][0]
    assert "退款小计" not in report["metrics"]


def test_exported_csv_only_contains_public_columns():
    sample_orders("S1", ORDERS, row_limit=1)
    text = export_rows_to_csv(registry.snapshot("S1").public_rows())
    rows = list(csv.reader(io.StringIO(text.lstrip("﻿"))))
    assert rows[0] == [ORDER_ID_FIELD, GROUP_FIELD]
    assert rows[1] == ["B", "家电"]
    assert AMOUNT_FIELD not in text and REFUND_FIELD not in text


def test_snapshot_is_registered_in_snapshot_module():
    sample_orders("S1", ORDERS, row_limit=1)
    snapshot = registry.snapshot("S1")
    entry, missing = DataSnapshotService().register_sampling_snapshot(snapshot)
    assert missing == []
    assert entry["快照编号"] == snapshot.snapshot_id
    assert entry["数据行数"] == "1"
    # 重复登记返回同一条记录，不会产生两个版本。
    again, _ = DataSnapshotService().register_sampling_snapshot(snapshot)
    assert again["id"] == entry["id"]


# ============================================================
# 迟到订单、同版本领取与并发签核
# ============================================================

def test_late_order_conflicts_with_claim_but_does_not_replace_it():
    sample_orders("S1", ORDERS, row_limit=1)
    claim = registry.claim("S1")
    assert (claim[ORDER_ID_FIELD], claim[AMOUNT_FIELD], claim[REFUND_FIELD]) == ("B", 10, 0)

    late_c = {ORDER_ID_FIELD: "C", GROUP_FIELD: "食品", AMOUNT_FIELD: 10, REFUND_FIELD: 4}
    audit = registry.ingest_late_orders("S1", [late_c])
    assert audit["pinned_top_order"] == "B"
    assert audit["resample_top_order"] == "C"
    assert audit["conflict"] is True

    # 本轮领取、报告、导出仍取自固定快照 v1，订单号与金额同版本。
    state = registry.record("S1")
    assert state.claimed_order_id == "B"
    assert state.claimed_amount == 10
    assert state.claimed_refund == 0
    assert state.claimed_version == registry.snapshot("S1").version
    report = build_sampling_report(
        "S1", include_refund=True, permissions={"report:read"}
    )
    assert report["snapshot_version"] == 1
    assert report["metrics"]["订单金额合计"] == 10
    assert report["metrics"]["退款小计"] == 0


def test_late_order_does_not_change_order_id_and_amount_source():
    sample_orders("S1", ORDERS, row_limit=1)
    registry.ingest_late_orders(
        "S1", [{ORDER_ID_FIELD: "C", AMOUNT_FIELD: 10, REFUND_FIELD: 4}]
    )
    claim = registry.claim("S1")
    assert claim[ORDER_ID_FIELD] == "B"
    assert claim[AMOUNT_FIELD] == 10
    assert claim["snapshot_version"] == 1


def test_claim_is_idempotent_guarded():
    sample_orders("S1", ORDERS, row_limit=1)
    registry.claim("S1")
    with pytest.raises(SamplingError) as excinfo:
        registry.claim("S1")
    assert excinfo.value.status_code == 409


def test_concurrent_sign_only_takes_effect_once():
    sample_orders("S1", ORDERS, row_limit=1)
    registry.claim("S1")

    def sign(_: int) -> bool:
        try:
            registry.sign("S1")
            return True
        except SamplingError as error:
            assert error.status_code == 409
            return False

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(sign, range(16)))

    assert sum(results) == 1
    assert registry.record("S1").signed is True
