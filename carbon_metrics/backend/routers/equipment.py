"""
设备管理 API 路由
提供设备列表查询接口
"""
from typing import Optional
from fastapi import APIRouter, Query
import logging

from ..db import get_db

router = APIRouter(prefix="/equipment", tags=["设备管理"])
logger = logging.getLogger(__name__)


@router.get("/ids")
def list_equipment_ids(
    equipment_type: Optional[str] = Query(None, description="设备类型筛选"),
):
    """
    获取设备ID列表

    可按 equipment_type 筛选，返回去重后的设备ID列表
    """
    db = get_db()
    with db.cursor() as cur:
        if equipment_type:
            cur.execute(
                "SELECT DISTINCT equipment_id, equipment_type "
                "FROM equipment_registry "
                "WHERE equipment_type = %s "
                "ORDER BY equipment_id",
                (equipment_type,),
            )
        else:
            cur.execute(
                "SELECT DISTINCT equipment_id, equipment_type "
                "FROM equipment_registry "
                "ORDER BY equipment_type, equipment_id"
            )
        rows = cur.fetchall()

    return {
        "items": [
            {"equipment_id": r["equipment_id"], "equipment_type": r["equipment_type"]}
            for r in rows
        ],
        "total": len(rows),
    }
