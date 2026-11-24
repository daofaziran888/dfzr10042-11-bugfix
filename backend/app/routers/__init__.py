"""业务路由汇总：通用资源路由 + 固定前缀的抽样路由。

``sampling`` 必须先注册：通用资源路由含动态段 ``/{module}``，
``/api/sampling/...`` 若后注册会被 ``/{module}/{entry_id}`` 抢先匹配。
"""
from __future__ import annotations

from app.routers import resource, sampling

ROUTERS = [sampling, resource]
