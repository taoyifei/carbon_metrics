"""
指标计算 API 路由
"""
from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, Query, HTTPException

from ..services import MetricCalculator
from ..metrics.base import CalculationResult
from ..models import (
    MetricBatchRequest,
    MetricBatchResponse,
    MetricResult,
    MetricCoverageOverview,
    MetricTrace,
    DataSource,
    MetricBreakdown,
)

router = APIRouter(prefix="/metrics", tags=["指标计算"])


def _to_metric_result(
    time_start: datetime,
    time_end: datetime,
    result: CalculationResult,
) -> MetricResult:
    trace = MetricTrace(
        formula=result.formula,
        formula_with_values=result.formula_with_values,
        data_source=DataSource(
            table="agg_hour",
            field=result.data_source_field,
            condition=result.data_source_condition,
            time_range=[str(time_start), str(time_end)],
            total_records=result.input_records,
            valid_records=result.valid_records,
        ),
        sql=result.sql_executed,
    )

    breakdown = [MetricBreakdown(**b) for b in result.breakdown]

    return MetricResult(
        metric_name=result.metric_name,
        value=result.value,
        unit=result.unit,
        status=result.status,
        quality_score=result.quality_score,
        trace=trace,
        quality_issues=result.quality_issues,
        breakdown=breakdown,
    )


@router.get("/list")
def list_metrics():
    """获取所有可用指标列表"""
    return {"metrics": MetricCalculator.list_metrics()}


@router.get("/calculate", response_model=MetricResult)
def calculate_metric(
    metric_name: str = Query(..., description="指标名称"),
    time_start: datetime = Query(..., description="开始时间"),
    time_end: datetime = Query(..., description="结束时间"),
    building_id: Optional[str] = Query(None, description="机楼筛选"),
    system_id: Optional[str] = Query(None, description="系统筛选"),
    equipment_type: Optional[str] = Query(None, description="设备类型"),
    equipment_id: Optional[str] = Query(None, description="设备ID"),
    sub_equipment_id: Optional[str] = Query(None, description="子设备ID"),
):
    """计算指定指标"""
    if time_start >= time_end:
        raise HTTPException(status_code=400, detail="开始时间必须小于结束时间")

    calculator = MetricCalculator()
    result = calculator.calculate(
        metric_name=metric_name,
        time_start=time_start,
        time_end=time_end,
        building_id=building_id,
        system_id=system_id,
        equipment_type=equipment_type,
        equipment_id=equipment_id,
        sub_equipment_id=sub_equipment_id,
        include_dependency_diagnostics=True,
    )
    return _to_metric_result(time_start, time_end, result)


@router.get("/coverage", response_model=MetricCoverageOverview)
def get_metric_coverage(
    time_start: datetime = Query(..., description="开始时间"),
    time_end: datetime = Query(..., description="结束时间"),
    metric_names: Optional[List[str]] = Query(None, description="指标名称列表，可选"),
    building_id: Optional[str] = Query(None, description="楼栋筛选"),
    system_id: Optional[str] = Query(None, description="系统筛选"),
    equipment_type: Optional[str] = Query(None, description="设备类型"),
    equipment_id: Optional[str] = Query(None, description="设备ID"),
    sub_equipment_id: Optional[str] = Query(None, description="子设备ID"),
):
    """获取指标在当前范围下的数据覆盖概览"""
    if time_start >= time_end:
        raise HTTPException(status_code=400, detail="开始时间必须小于结束时间")

    calculator = MetricCalculator()
    return calculator.coverage_overview(
        metric_names=metric_names,
        time_start=time_start,
        time_end=time_end,
        building_id=building_id,
        system_id=system_id,
        equipment_type=equipment_type,
        equipment_id=equipment_id,
        sub_equipment_id=sub_equipment_id,
    )


@router.post("/calculate_batch", response_model=MetricBatchResponse)
def calculate_metric_batch(payload: MetricBatchRequest):
    """批量计算指标"""
    if payload.time_start >= payload.time_end:
        raise HTTPException(status_code=400, detail="开始时间必须小于结束时间")

    calculator = MetricCalculator()
    metric_names = payload.metric_names or MetricCalculator.list_metrics()

    results = calculator.calculate_batch(
        metric_names=metric_names,
        time_start=payload.time_start,
        time_end=payload.time_end,
        building_id=payload.building_id,
        system_id=payload.system_id,
        equipment_type=payload.equipment_type,
        equipment_id=payload.equipment_id,
        sub_equipment_id=payload.sub_equipment_id,
        log_result=False,
        include_dependency_diagnostics=False,
    )

    items = []
    for result in results:
        items.append(_to_metric_result(payload.time_start, payload.time_end, result))

    return MetricBatchResponse(items=items, total=len(items))
