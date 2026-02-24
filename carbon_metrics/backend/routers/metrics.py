"""
指标计算 API 路由
"""
import copy
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, List, Tuple
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
logger = logging.getLogger(__name__)


@dataclass
class _MetricCacheEntry:
    expire_at: float
    data_version: Tuple[Any, ...]
    value: Any


_CACHE_LOCK = threading.Lock()
_METRIC_API_CACHE: dict[Tuple[Any, ...], _MetricCacheEntry] = {}
_MAX_CACHE_SIZE = 512

_DATA_VERSION_CACHE_LOCK = threading.Lock()
_data_version_cache: Optional[tuple] = None  # (expire_at, version_tuple)
_DATA_VERSION_CACHE_TTL = 3.0  # seconds


def _resolve_metric_cache_ttl_seconds() -> int:
    raw = os.getenv("METRIC_API_CACHE_TTL_SECONDS", "30").strip()
    try:
        ttl = int(raw)
    except (TypeError, ValueError):
        logger.warning("Invalid METRIC_API_CACHE_TTL_SECONDS=%r, fallback to 30", raw)
        return 30
    return max(0, ttl)


def _prune_metric_api_cache(now_ts: float) -> None:
    if len(_METRIC_API_CACHE) <= _MAX_CACHE_SIZE:
        return
    expired_keys = [
        key
        for key, entry in _METRIC_API_CACHE.items()
        if entry.expire_at <= now_ts
    ]
    for key in expired_keys:
        _METRIC_API_CACHE.pop(key, None)
    if len(_METRIC_API_CACHE) <= _MAX_CACHE_SIZE:
        return
    # 超限时按过期时间淘汰最旧条目，避免缓存无界增长
    oldest = sorted(
        _METRIC_API_CACHE.items(),
        key=lambda item: item[1].expire_at,
    )
    for key, _ in oldest[: max(1, len(_METRIC_API_CACHE) - _MAX_CACHE_SIZE)]:
        _METRIC_API_CACHE.pop(key, None)


def _load_data_version_from_db(calculator: MetricCalculator) -> Tuple[Any, ...] | None:
    sql = """
        SELECT
            (SELECT COALESCE(MAX(id), 0) FROM agg_hour) AS agg_hour_max_id,
            (SELECT COALESCE(DATE_FORMAT(MAX(bucket_time), '%%Y-%%m-%%d %%H:%%i:%%s'), '') FROM agg_hour) AS agg_hour_max_bucket,
            (SELECT COALESCE(MAX(id), 0) FROM agg_hour_quality) AS quality_max_id,
            (SELECT COALESCE(DATE_FORMAT(MAX(bucket_time), '%%Y-%%m-%%d %%H:%%i:%%s'), '') FROM agg_hour_quality) AS quality_max_bucket
    """
    try:
        with calculator.db.cursor() as cursor:
            cursor.execute(sql)
            row = cursor.fetchone() or {}
    except Exception as exc:
        logger.warning("metric cache disabled for this request: failed to query data version: %s", exc)
        return None
    return (
        int(row.get("agg_hour_max_id") or 0),
        str(row.get("agg_hour_max_bucket") or ""),
        int(row.get("quality_max_id") or 0),
        str(row.get("quality_max_bucket") or ""),
    )


def _load_data_version(calculator: MetricCalculator) -> Tuple[Any, ...] | None:
    global _data_version_cache
    now = time.time()
    with _DATA_VERSION_CACHE_LOCK:
        if _data_version_cache is not None and _data_version_cache[0] > now:
            return _data_version_cache[1]
    # cache miss — query DB
    version = _load_data_version_from_db(calculator)
    if version is not None:
        with _DATA_VERSION_CACHE_LOCK:
            _data_version_cache = (now + _DATA_VERSION_CACHE_TTL, version)
    return version


def _cache_get(cache_key: Tuple[Any, ...], data_version: Tuple[Any, ...] | None) -> Any | None:
    ttl = _resolve_metric_cache_ttl_seconds()
    if ttl <= 0 or data_version is None:
        return None
    now_ts = time.time()
    with _CACHE_LOCK:
        entry = _METRIC_API_CACHE.get(cache_key)
        if not entry:
            return None
        if entry.expire_at <= now_ts or entry.data_version != data_version:
            _METRIC_API_CACHE.pop(cache_key, None)
            return None
        return copy.deepcopy(entry.value)


def _cache_set(cache_key: Tuple[Any, ...], data_version: Tuple[Any, ...] | None, value: Any) -> None:
    ttl = _resolve_metric_cache_ttl_seconds()
    if ttl <= 0 or data_version is None:
        return
    now_ts = time.time()
    entry = _MetricCacheEntry(
        expire_at=now_ts + ttl,
        data_version=data_version,
        value=copy.deepcopy(value),
    )
    with _CACHE_LOCK:
        _METRIC_API_CACHE[cache_key] = entry
        _prune_metric_api_cache(now_ts)


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
    data_version = _load_data_version(calculator)
    cache_key = (
        "calculate",
        metric_name,
        time_start.isoformat(),
        time_end.isoformat(),
        building_id or "",
        system_id or "",
        equipment_type or "",
        equipment_id or "",
        sub_equipment_id or "",
        True,  # include_dependency_diagnostics
    )
    cached = _cache_get(cache_key, data_version)
    if cached is not None:
        return cached

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
    payload = _to_metric_result(time_start, time_end, result)
    # 失败通常是瞬时错误，不缓存失败态，避免放大短时故障影响
    if result.status != "failed":
        _cache_set(cache_key, data_version, payload)
    return payload


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
    data_version = _load_data_version(calculator)
    cache_key = (
        "coverage",
        tuple(metric_names) if metric_names else (),
        time_start.isoformat(),
        time_end.isoformat(),
        building_id or "",
        system_id or "",
        equipment_type or "",
        equipment_id or "",
        sub_equipment_id or "",
    )
    cached = _cache_get(cache_key, data_version)
    if cached is not None:
        return cached

    payload = calculator.coverage_overview(
        metric_names=metric_names,
        time_start=time_start,
        time_end=time_end,
        building_id=building_id,
        system_id=system_id,
        equipment_type=equipment_type,
        equipment_id=equipment_id,
        sub_equipment_id=sub_equipment_id,
    )
    _cache_set(cache_key, data_version, payload)
    return payload


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
