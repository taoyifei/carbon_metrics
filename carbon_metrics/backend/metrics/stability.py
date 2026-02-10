"""
运行稳定性指标计算
"""
from typing import Any, List, Optional, Tuple

from .base import BaseMetric, MetricContext, CalculationResult


class _RuntimeRatioMetric(BaseMetric):
    """运行时长占比基类"""

    @property
    def unit(self) -> str:
        return "%"

    @property
    def _equipment_type(self) -> Optional[str]:
        return None

    @property
    def _equipment_types(self) -> Optional[List[str]]:
        return None

    @property
    def _metric_candidates(self) -> List[str]:
        # 兼容两种命名：runtime（新）/ run_status（历史）
        return ["runtime", "run_status"]

    def _resolve_scope(
        self,
        ctx: MetricContext,
    ) -> Tuple[Optional[str], Optional[List[str]]]:
        if ctx.equipment_type:
            return ctx.equipment_type, None
        if self._equipment_type:
            return self._equipment_type, None
        return None, self._equipment_types

    @staticmethod
    def _build_runtime_where(
        ctx: MetricContext,
        metric_name: str,
        equipment_type: Optional[str],
        equipment_types: Optional[List[str]],
    ) -> Tuple[str, List[Any]]:
        conditions = [
            "metric_name = %s",
            "bucket_time >= %s",
            "bucket_time < %s",
        ]
        params: List[Any] = [metric_name, ctx.time_start, ctx.time_end]
        if equipment_type:
            conditions.append("equipment_type = %s")
            params.append(equipment_type)
        elif equipment_types:
            placeholders = ", ".join(["%s"] * len(equipment_types))
            conditions.append(f"equipment_type IN ({placeholders})")
            params.extend(equipment_types)
        if ctx.building_id:
            conditions.append("building_id = %s")
            params.append(ctx.building_id)
        if ctx.system_id:
            conditions.append("system_id = %s")
            params.append(ctx.system_id)
        if ctx.equipment_id:
            conditions.append("equipment_id = %s")
            params.append(ctx.equipment_id)
        if ctx.sub_equipment_id:
            conditions.append("sub_equipment_id = %s")
            params.append(ctx.sub_equipment_id)
        return " AND ".join(conditions), params

    @staticmethod
    def _scope_condition_text(
        equipment_type: Optional[str],
        equipment_types: Optional[List[str]],
    ) -> str:
        if equipment_type:
            return f"equipment_type='{equipment_type}'"
        if equipment_types:
            joined = "', '".join(equipment_types)
            return f"equipment_type IN ('{joined}')"
        return "equipment_type=*"

    def calculate(self, ctx: MetricContext) -> CalculationResult:
        scope_equipment_type, scope_equipment_types = self._resolve_scope(ctx)

        try:
            with self.db.cursor() as cursor:
                selected_metric: Optional[str] = None
                selected_row = None
                selected_sql = ""
                for metric_name in self._metric_candidates:
                    where, params = self._build_runtime_where(
                        ctx, metric_name, scope_equipment_type, scope_equipment_types)
                    sql = f"""
                        SELECT
                            SUM(agg_delta) AS total_runtime,
                            COUNT(DISTINCT equipment_id) AS device_count,
                            COUNT(*) AS record_count
                        FROM agg_hour
                        WHERE {where}
                    """
                    cursor.execute(sql, params)
                    row = cursor.fetchone()
                    if row and int(row["record_count"] or 0) > 0:
                        selected_metric = metric_name
                        selected_row = row
                        selected_sql = sql.strip()
                        break

                if not selected_metric or not selected_row:
                    missing_issues = self._build_missing_dependency_issues(
                        cursor, ctx, self._metric_candidates,
                        equipment_type=scope_equipment_type,
                        equipment_types=scope_equipment_types)
                    return CalculationResult(
                        metric_name=self.metric_name, value=None,
                        unit=self.unit, status="no_data",
                        formula=self.formula,
                        quality_score=0.0,
                        quality_issues=missing_issues,
                    )

                total_runtime = float(selected_row["total_runtime"] or 0)
                device_count = max(1, int(selected_row["device_count"] or 0))

                delta = ctx.time_end - ctx.time_start
                period_hours = max(0.0, delta.total_seconds() / 3600)
                max_runtime = period_hours * device_count

                if max_runtime == 0:
                    ratio = 0.0
                else:
                    ratio = round(total_runtime / max_runtime * 100, 2)

                quality_score, quality_issues = self._check_quality_from_table(
                    cursor, ctx, [selected_metric],
                    equipment_type=scope_equipment_type,
                    equipment_types=scope_equipment_types)

                calc_issues = []
                if selected_metric != "runtime":
                    calc_issues.append({
                        "type": "fallback_data_source",
                        "description": "runtime 缺失，已回退使用 run_status 增量口径",
                        "details": {"metric_used": selected_metric},
                    })
                all_issues = quality_issues + calc_issues

                formula_with_values = (
                    f"= {round(total_runtime, 1)}h"
                    f" / ({round(period_hours, 2)}h × {device_count}台)"
                    f" = {ratio}%"
                )

                return CalculationResult(
                    metric_name=self.metric_name, value=ratio,
                    unit=self.unit, status=self._status_from_issues(all_issues),
                    formula=self.formula,
                    formula_with_values=formula_with_values,
                    sql_executed=selected_sql,
                    input_records=int(selected_row["record_count"] or 0),
                    valid_records=int(selected_row["record_count"] or 0),
                    data_source_field="agg_delta",
                    data_source_condition=(
                        f"metric_name='{selected_metric}', "
                        f"{self._scope_condition_text(scope_equipment_type, scope_equipment_types)}"
                    ),
                    quality_score=quality_score,
                    quality_issues=all_issues,
                )
        except Exception as e:
            return CalculationResult(
                metric_name=self.metric_name, value=None,
                unit=self.unit, status="failed",
                formula=self.formula,
                quality_issues=[{
                    "type": "error", "description": str(e),
                }],
            )


class ChillerRuntimeRatioMetric(_RuntimeRatioMetric):
    """冷机运行时长占比"""

    @property
    def metric_name(self) -> str:
        return "冷机运行时长占比"

    @property
    def formula(self) -> str:
        return "冷机运行时长占比 = 冷机运行时长 / (评估周期 × 设备数) × 100%"

    @property
    def _equipment_type(self) -> Optional[str]:
        return "chiller"


class TowerFanRuntimeRatioMetric(_RuntimeRatioMetric):
    """风机运行时长占比"""

    @property
    def metric_name(self) -> str:
        return "风机运行时长占比"

    @property
    def formula(self) -> str:
        return "风机运行时长占比 = 风机运行时长 / (评估周期 × 设备数) × 100%"

    @property
    def _equipment_types(self) -> Optional[List[str]]:
        return ["tower_fan", "cooling_tower", "cooling_tower_closed"]
