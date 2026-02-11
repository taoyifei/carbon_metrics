"""
指标计算服务
统一管理所有指标的计算
"""
import os
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from collections import Counter
from typing import Any, Dict, List, Optional, Type

from ..db import Database, get_db
from ..metrics.base import BaseMetric, MetricContext, CalculationResult
from ..metrics.energy import (
    TotalEnergyMetric,
    ChillerEnergyRatioMetric,
    PumpEnergyRatioMetric,
    TowerEnergyRatioMetric,
)
from ..metrics.temperature import (
    ChilledSupplyTempMetric,
    ChilledReturnTempMetric,
    CoolingSupplyTempMetric,
    CoolingReturnTempMetric,
    ChilledWaterDeltaTMetric,
)
from ..metrics.flow import ChilledFlowMetric, CoolingFlowMetric, CoolingCapacityMetric
from ..metrics.chiller import (
    ChillerAvgLoadMetric,
    ChillerMaxLoadMetric,
    ChillerLoadCvMetric,
    ChillerCopMetric,
)
from ..metrics.pump import ChilledPumpFrequencyMetric, CoolingPumpFrequencyMetric
from ..metrics.tower import CoolingWaterDeltaTMetric, TowerFanPowerMetric
from ..metrics.stability import ChillerRuntimeRatioMetric, TowerFanRuntimeRatioMetric
from ..metrics.maintenance import ChillerOverloadRiskMetric

# 配置计算日志
_log_dir = os.path.join(os.path.dirname(__file__), "..", "..", "logs")
os.makedirs(_log_dir, exist_ok=True)

calc_logger = logging.getLogger("metric_calc")
calc_logger.setLevel(logging.INFO)
_log_file = os.path.normcase(os.path.abspath(
    os.path.join(_log_dir, "metric_calculations.log")
))
_has_file_handler = any(
    isinstance(h, RotatingFileHandler)
    and os.path.normcase(os.path.abspath(getattr(h, "baseFilename", ""))) == _log_file
    for h in calc_logger.handlers
)
if not _has_file_handler:
    _handler = RotatingFileHandler(
        _log_file,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    _handler.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
    calc_logger.addHandler(_handler)


class MetricCalculator:
    """指标计算器"""

    # 注册的指标类
    METRICS: Dict[str, Type[BaseMetric]] = {
        # 能耗结构
        "系统总电量": TotalEnergyMetric,
        "冷机能耗占比": ChillerEnergyRatioMetric,
        "水泵能耗占比": PumpEnergyRatioMetric,
        "风机能耗占比": TowerEnergyRatioMetric,
        # 温度
        "冷冻水供水温度": ChilledSupplyTempMetric,
        "冷冻水回水温度": ChilledReturnTempMetric,
        "冷却水供水温度": CoolingSupplyTempMetric,
        "冷却水回水温度": CoolingReturnTempMetric,
        "冷冻水温差": ChilledWaterDeltaTMetric,
        # 流量
        "冷冻水流量": ChilledFlowMetric,
        "冷却水流量": CoolingFlowMetric,
        "制冷量": CoolingCapacityMetric,
        # 冷机效率
        "冷机平均负载率": ChillerAvgLoadMetric,
        "冷机最大负载率": ChillerMaxLoadMetric,
        "冷机负载波动系数": ChillerLoadCvMetric,
        "冷机COP": ChillerCopMetric,
        # 水泵效率
        "冷冻泵工作频率": ChilledPumpFrequencyMetric,
        "冷却泵工作频率": CoolingPumpFrequencyMetric,
        # 冷却塔效率
        "冷却水温差": CoolingWaterDeltaTMetric,
        "冷却塔风机功率": TowerFanPowerMetric,
        # 运行稳定性
        "冷机运行时长占比": ChillerRuntimeRatioMetric,
        "风机运行时长占比": TowerFanRuntimeRatioMetric,
        # 预测性维护
        "过载风险指数": ChillerOverloadRiskMetric,
    }

    def __init__(self, db: Database = None):
        self.db = db or get_db()

    @staticmethod
    def _resolve_calc_workers() -> int:
        raw_value = os.getenv("METRIC_CALC_WORKERS", "").strip()
        default_workers = 4
        if not raw_value:
            return default_workers
        try:
            workers = int(raw_value)
        except ValueError:
            calc_logger.warning(
                "METRIC_CALC_WORKERS=%r invalid, fallback to %s",
                raw_value,
                default_workers,
            )
            return default_workers
        return max(1, min(workers, 16))

    def calculate(
        self,
        metric_name: str,
        time_start: datetime,
        time_end: datetime,
        building_id: Optional[str] = None,
        system_id: Optional[str] = None,
        equipment_type: Optional[str] = None,
        equipment_id: Optional[str] = None,
        sub_equipment_id: Optional[str] = None,
        log_result: bool = True,
        query_cache: Optional[Dict] = None,
        include_dependency_diagnostics: bool = False,
    ) -> CalculationResult:
        """计算指定指标"""

        if metric_name not in self.METRICS:
            return CalculationResult(
                metric_name=metric_name,
                value=None,
                unit="",
                status="failed",
                formula="",
                quality_issues=[{"type": "error", "description": f"未知指标: {metric_name}"}],
            )

        ctx = MetricContext(
            time_start=time_start,
            time_end=time_end,
            building_id=building_id,
            system_id=system_id,
            equipment_type=equipment_type,
            equipment_id=equipment_id,
            sub_equipment_id=sub_equipment_id,
        )

        metric_class = self.METRICS[metric_name]
        metric = metric_class(
            self.db,
            query_cache=query_cache,
            include_dependency_diagnostics=include_dependency_diagnostics,
        )
        result = metric.calculate(ctx)

        # 记录计算日志
        filters = ",".join(
            f"{k}={v}" for k, v in [
                ("building_id", building_id),
                ("system_id", system_id),
                ("equipment_type", equipment_type),
                ("equipment_id", equipment_id),
                ("sub_equipment_id", sub_equipment_id),
            ] if v
        )
        if log_result:
            calc_logger.info(
                f"指标={metric_name} | 值={result.value} {result.unit} | "
                f"状态={result.status} | "
                f"时间={time_start}~{time_end} | "
                f"筛选={filters or '无'} | "
                f"公式={result.formula_with_values} | "
                f"SQL={result.sql_executed} | "
                f"记录数={result.input_records} | "
                f"质量={result.quality_score}"
            )

        return result

    def calculate_batch(
        self,
        metric_names: List[str],
        time_start: datetime,
        time_end: datetime,
        building_id: Optional[str] = None,
        system_id: Optional[str] = None,
        equipment_type: Optional[str] = None,
        equipment_id: Optional[str] = None,
        sub_equipment_id: Optional[str] = None,
        log_result: bool = False,
        include_dependency_diagnostics: bool = False,
    ) -> List[CalculationResult]:
        """Batch compute metrics and keep result order."""
        if not metric_names:
            return []

        metrics = list(metric_names)
        workers = min(self._resolve_calc_workers(), len(metrics))

        def run_single(name: str) -> CalculationResult:
            return self.calculate(
                metric_name=name,
                time_start=time_start,
                time_end=time_end,
                building_id=building_id,
                system_id=system_id,
                equipment_type=equipment_type,
                equipment_id=equipment_id,
                sub_equipment_id=sub_equipment_id,
                log_result=log_result,
                query_cache=None,
                include_dependency_diagnostics=include_dependency_diagnostics,
            )

        if workers <= 1:
            shared_cache: Dict[str, Any] = {}
            return [
                self.calculate(
                    metric_name=name,
                    time_start=time_start,
                    time_end=time_end,
                    building_id=building_id,
                    system_id=system_id,
                    equipment_type=equipment_type,
                    equipment_id=equipment_id,
                    sub_equipment_id=sub_equipment_id,
                    log_result=log_result,
                    query_cache=shared_cache,
                    include_dependency_diagnostics=include_dependency_diagnostics,
                )
                for name in metrics
            ]

        with ThreadPoolExecutor(max_workers=workers) as executor:
            return list(executor.map(run_single, metrics))

    @staticmethod
    def _extract_missing_dependencies(result: CalculationResult) -> List[str]:
        """从指标结果里提取缺失依赖 metric_name 列表"""
        dependencies: List[str] = []
        for issue in result.quality_issues or []:
            if not isinstance(issue, dict):
                continue
            if issue.get("type") != "missing_dependency":
                continue
            details = issue.get("details") or {}
            missing = details.get("missing_metrics")
            if isinstance(missing, list):
                dependencies.extend(str(name) for name in missing if name)
            elif isinstance(missing, str) and missing:
                dependencies.append(missing)
        return list(dict.fromkeys(dependencies))

    def _query_available_metric_counts(
        self,
        time_start: datetime,
        time_end: datetime,
        building_id: Optional[str] = None,
        system_id: Optional[str] = None,
        equipment_type: Optional[str] = None,
        equipment_id: Optional[str] = None,
        sub_equipment_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """查询当前筛选范围内原始 metric_name 覆盖计数"""
        conditions = ["bucket_time >= %s", "bucket_time < %s"]
        params: List[Any] = [time_start, time_end]

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
        BaseMetric._append_sub_equipment_condition(
            conditions,
            params,
            sub_equipment_id,
        )

        where_clause = " AND ".join(conditions)
        sql = f"""
            SELECT metric_name, COUNT(*) AS cnt
            FROM agg_hour
            WHERE {where_clause}
            GROUP BY metric_name
            ORDER BY cnt DESC
        """
        with self.db.cursor() as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall()

        return {
            str(row["metric_name"]): int(row["cnt"] or 0)
            for row in rows
            if row.get("metric_name") is not None
        }

    def coverage_overview(
        self,
        time_start: datetime,
        time_end: datetime,
        metric_names: Optional[List[str]] = None,
        building_id: Optional[str] = None,
        system_id: Optional[str] = None,
        equipment_type: Optional[str] = None,
        equipment_id: Optional[str] = None,
        sub_equipment_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """返回指标在当前时间范围和筛选条件下的可计算覆盖概览"""
        metrics_to_check = metric_names or self.list_metrics()
        items: List[Dict[str, Any]] = []
        missing_counter: Counter[str] = Counter()
        # Step 1 优化: 串行计算 + 共享缓存，相同 SQL 只查一次
        shared_cache: Dict[str, Any] = {}
        results = [
            self.calculate(
                metric_name=name,
                time_start=time_start,
                time_end=time_end,
                building_id=building_id,
                system_id=system_id,
                equipment_type=equipment_type,
                equipment_id=equipment_id,
                sub_equipment_id=sub_equipment_id,
                log_result=False,
                query_cache=shared_cache,
                include_dependency_diagnostics=False,
            )
            for name in metrics_to_check
        ]

        for metric_name, result in zip(metrics_to_check, results):
            missing_dependencies = self._extract_missing_dependencies(result)
            missing_counter.update(missing_dependencies)
            issue_types = sorted({
                str(issue.get("type"))
                for issue in (result.quality_issues or [])
                if isinstance(issue, dict) and issue.get("type")
            })

            items.append({
                "metric_name": metric_name,
                "status": result.status,
                "has_value": result.value is not None,
                "quality_score": float(result.quality_score or 0.0),
                "issue_count": len(result.quality_issues or []),
                "input_records": int(result.input_records or 0),
                "valid_records": int(result.valid_records or 0),
                "issue_types": issue_types,
                "missing_dependencies": missing_dependencies,
            })

        total = len(items)
        success_count = sum(1 for item in items if item["status"] == "success")
        partial_count = sum(1 for item in items if item["status"] == "partial")
        no_data_count = sum(1 for item in items if item["status"] == "no_data")
        failed_count = sum(1 for item in items if item["status"] == "failed")
        calculable_count = success_count + partial_count
        calculable_rate = round((calculable_count / total) * 100, 2) if total else 0.0

        # 原子 metric 覆盖统计（用于 coverage banner 展示）
        available_metric_counts = self._query_available_metric_counts(
            time_start=time_start,
            time_end=time_end,
            building_id=building_id,
            system_id=system_id,
            equipment_type=equipment_type,
            equipment_id=equipment_id,
            sub_equipment_id=sub_equipment_id,
        )

        # 业务指标输入记录统计（复用已算结果，不额外查询）
        metric_input_counts = {
            item["metric_name"]: item["input_records"]
            for item in items
            if item["input_records"] > 0
        }

        return {
            "time_start": time_start,
            "time_end": time_end,
            "summary": {
                "total_metrics": total,
                "calculable_count": calculable_count,
                "success_count": success_count,
                "partial_count": partial_count,
                "no_data_count": no_data_count,
                "failed_count": failed_count,
                "calculable_rate": calculable_rate,
            },
            "available_metric_counts": available_metric_counts,
            "metric_input_counts": metric_input_counts,
            "missing_dependencies": [name for name, _ in missing_counter.most_common()],
            "missing_dependency_counts": {
                name: count for name, count in missing_counter.most_common()
            },
            "calculable_metrics": [
                item["metric_name"]
                for item in items
                if item["status"] in {"success", "partial"}
            ],
            "no_data_metrics": [
                item["metric_name"]
                for item in items
                if item["status"] == "no_data"
            ],
            "failed_metrics": [
                item["metric_name"]
                for item in items
                if item["status"] == "failed"
            ],
            "items": items,
        }

    @classmethod
    def list_metrics(cls) -> list:
        """列出所有可用指标"""
        return list(cls.METRICS.keys())
