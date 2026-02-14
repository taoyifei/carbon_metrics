"""
设备管理 API 路由
提供设备列表查询接口
"""
from typing import Optional
from fastapi import APIRouter, Query, HTTPException
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
    try:
        with db.cursor() as cur:
            if equipment_type:
                cur.execute(
                    "SELECT DISTINCT equipment_id, equipment_type "
                    "FROM agg_hour "
                    "WHERE equipment_id IS NOT NULL "
                    "  AND equipment_type = %s "
                    "ORDER BY equipment_id",
                    (equipment_type,),
                )
            else:
                cur.execute(
                    "SELECT DISTINCT equipment_id, equipment_type "
                    "FROM agg_hour "
                    "WHERE equipment_id IS NOT NULL "
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
    except Exception as e:
        logger.exception("设备列表查询失败")
        raise HTTPException(status_code=500, detail=f"设备列表查询失败: {e}")


@router.get("/sub-scopes")
def list_sub_equipment_scopes(
    building_id: Optional[str] = Query(None, description="机楼筛选"),
    system_id: Optional[str] = Query(None, description="系统筛选"),
    equipment_type: Optional[str] = Query(None, description="设备类型筛选"),
    equipment_id: Optional[str] = Query(None, description="设备ID筛选"),
):
    """
    获取主备口径可用范围

    返回当前筛选范围下 main / backup / null 三类子设备口径是否存在数据。
    """
    db = get_db()
    try:
        conditions = []
        params = []
        if building_id:
            conditions.append("building_id = %s")
            params.append(building_id)
        if system_id:
            conditions.append("system_id = %s")
            params.append(system_id)
        if equipment_type:
            conditions.append("equipment_type = %s")
            params.append(equipment_type)
        if equipment_id:
            conditions.append("equipment_id = %s")
            params.append(equipment_id)

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        sql = f"""
            SELECT
                SUM(CASE WHEN LOWER(TRIM(sub_equipment_id)) = 'main' THEN 1 ELSE 0 END) AS main_count,
                SUM(CASE WHEN LOWER(TRIM(sub_equipment_id)) = 'backup' THEN 1 ELSE 0 END) AS backup_count,
                SUM(
                    CASE
                        WHEN sub_equipment_id IS NULL
                             OR TRIM(sub_equipment_id) = ''
                             OR UPPER(TRIM(sub_equipment_id)) = '__NULL__'
                        THEN 1 ELSE 0
                    END
                ) AS null_count
            FROM agg_hour
            WHERE {where_clause}
        """
        with db.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone() or {}

        main_count = int(row.get("main_count") or 0)
        backup_count = int(row.get("backup_count") or 0)
        null_count = int(row.get("null_count") or 0)

        available_scopes = []
        if main_count > 0:
            available_scopes.append("main")
        if backup_count > 0:
            available_scopes.append("backup")
        if null_count > 0:
            available_scopes.append("null")

        return {
            "available_scopes": available_scopes,
            "counts": {
                "main": main_count,
                "backup": backup_count,
                "null": null_count,
            },
        }
    except Exception as e:
        logger.exception("主备口径范围查询失败")
        raise HTTPException(status_code=500, detail=f"主备口径范围查询失败: {e}")
