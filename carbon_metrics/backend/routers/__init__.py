"""
API 路由模块
"""
from .metrics import router as metrics_router
from .quality import router as quality_router
from .equipment import router as equipment_router

__all__ = ["metrics_router", "quality_router", "equipment_router"]
