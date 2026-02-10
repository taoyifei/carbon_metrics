"""
数据质量 API 路由
提供数据质量查询、异常问题查询等接口
"""
from datetime import datetime
from typing import Optional, Literal
import logging
from fastapi import APIRouter, Query, HTTPException

from ..services import QualityService
from ..models import (
    QualitySummary,
    QualityRecord,
    DataIssue,
    QualityTrend,
    PaginatedQualityResponse,
    PaginatedIssueResponse,
)

router = APIRouter(prefix="/quality", tags=["数据质量"])
logger = logging.getLogger(__name__)


@router.get("/summary", response_model=QualitySummary)
def get_quality_summary(
    time_start: datetime = Query(..., description="开始时间"),
    time_end: datetime = Query(..., description="结束时间"),
    building_id: Optional[str] = Query(None, description="机楼筛选"),
    system_id: Optional[str] = Query(None, description="系统筛选"),
    equipment_type: Optional[str] = Query(None, description="设备类型筛选"),
    quality_level: Optional[Literal["good", "warning", "poor"]] = Query(
        None, description="质量等级筛选"
    ),
    granularity: Literal["hour", "day"] = Query("hour", description="时间粒度"),
):
    """
    获取数据质量汇总统计

    返回指定时间范围内的质量统计信息，包括：
    - 各质量等级的记录数
    - 平均质量评分
    - 平均完整率
    - 各类问题的总数
    """
    if time_start >= time_end:
        raise HTTPException(status_code=400, detail="开始时间必须小于结束时间")

    try:
        service = QualityService()
        result = service.get_summary(
            time_start=time_start,
            time_end=time_end,
            building_id=building_id,
            system_id=system_id,
            equipment_type=equipment_type,
            quality_level=quality_level,
            granularity=granularity,
        )
        return QualitySummary(**result)
    except Exception:
        logger.exception("Failed to query quality data")
        raise HTTPException(status_code=500, detail="查询失败")


@router.get("/list", response_model=PaginatedQualityResponse)
def get_quality_list(
    time_start: datetime = Query(..., description="开始时间"),
    time_end: datetime = Query(..., description="结束时间"),
    building_id: Optional[str] = Query(None, description="机楼筛选"),
    system_id: Optional[str] = Query(None, description="系统筛选"),
    equipment_type: Optional[str] = Query(None, description="设备类型筛选"),
    equipment_id: Optional[str] = Query(None, description="设备ID筛选"),
    quality_level: Optional[Literal["good", "warning", "poor"]] = Query(
        None, description="质量等级筛选"
    ),
    granularity: Literal["hour", "day"] = Query("hour", description="时间粒度"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
):
    """
    获取数据质量明细列表（分页）

    返回每条聚合记录的质量详情，包括：
    - 质量评分和等级
    - 完整率
    - 各类问题数量
    """
    if time_start >= time_end:
        raise HTTPException(status_code=400, detail="开始时间必须小于结束时间")

    try:
        service = QualityService()
        result = service.get_list(
            time_start=time_start,
            time_end=time_end,
            building_id=building_id,
            system_id=system_id,
            equipment_type=equipment_type,
            equipment_id=equipment_id,
            quality_level=quality_level,
            granularity=granularity,
            page=page,
            page_size=page_size,
        )
        return PaginatedQualityResponse(
            items=[QualityRecord(**item) for item in result["items"]],
            total=result["total"],
            page=result["page"],
            page_size=result["page_size"],
            total_pages=result["total_pages"],
        )
    except Exception:
        logger.exception("Failed to query quality data")
        raise HTTPException(status_code=500, detail="查询失败")


@router.get("/issues", response_model=PaginatedIssueResponse)
def get_quality_issues(
    time_start: datetime = Query(..., description="开始时间"),
    time_end: datetime = Query(..., description="结束时间"),
    issue_type: Optional[Literal["gap", "negative", "jump", "out_of_range"]] = Query(
        None, description="问题类型筛选"
    ),
    building_id: Optional[str] = Query(None, description="机楼筛选"),
    system_id: Optional[str] = Query(None, description="系统筛选"),
    equipment_type: Optional[str] = Query(None, description="设备类型筛选"),
    severity: Optional[Literal["high", "medium", "low"]] = Query(
        None, description="严重程度筛选"
    ),
    granularity: Literal["hour", "day"] = Query("hour", description="时间粒度"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
):
    """
    获取数据异常问题列表

    问题类型：
    - gap: 时间缺口
    - negative: 负值
    - jump: 异常跳变
    - out_of_range: 超量程
    """
    if time_start >= time_end:
        raise HTTPException(status_code=400, detail="开始时间必须小于结束时间")

    try:
        service = QualityService()
        result = service.get_issues(
            time_start=time_start,
            time_end=time_end,
            issue_type=issue_type,
            building_id=building_id,
            system_id=system_id,
            equipment_type=equipment_type,
            severity=severity,
            granularity=granularity,
            page=page,
            page_size=page_size,
        )
        return PaginatedIssueResponse(
            items=[DataIssue(**item) for item in result["items"]],
            total=result["total"],
            page=result["page"],
            page_size=result["page_size"],
            total_pages=result["total_pages"],
        )
    except Exception:
        logger.exception("Failed to query quality data")
        raise HTTPException(status_code=500, detail="查询失败")


@router.get("/equipment/{equipment_id}/trend")
def get_equipment_quality_trend(
    equipment_id: str,
    time_start: datetime = Query(..., description="开始时间"),
    time_end: datetime = Query(..., description="结束时间"),
    metric_name: Optional[str] = Query(None, description="指标名称筛选"),
    granularity: Literal["hour", "day"] = Query("hour", description="时间粒度"),
):
    """
    获取单个设备的质量趋势

    返回该设备在指定时间范围内的质量评分变化趋势
    """
    if time_start >= time_end:
        raise HTTPException(status_code=400, detail="开始时间必须小于结束时间")

    if not equipment_id or not equipment_id.strip():
        raise HTTPException(status_code=400, detail="设备ID不能为空")

    try:
        service = QualityService()
        result = service.get_equipment_trend(
            equipment_id=equipment_id,
            time_start=time_start,
            time_end=time_end,
            metric_name=metric_name,
            granularity=granularity,
        )
        return {"equipment_id": equipment_id, "trend": result}
    except Exception:
        logger.exception("Failed to query quality data")
        raise HTTPException(status_code=500, detail="查询失败")


@router.get("/raw-report")
def get_raw_quality_report():
    """
    获取原始数据质量报告

    返回 data_quality_deep_report.csv 的结构化数据
    """
    try:
        service = QualityService()
        items = service.get_raw_report()
        return {"items": items, "total": len(items)}
    except Exception:
        logger.exception("Failed to read quality report")
        raise HTTPException(status_code=500, detail="读取质量报告失败")
