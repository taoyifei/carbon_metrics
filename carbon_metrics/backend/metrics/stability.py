"""运行稳定性指标计算。"""
from __future__ import annotations

from typing import Any, List, Optional, Tuple

from .base import BaseMetric, MetricContext, CalculationResult


class _RuntimeRatioMetric(BaseMetric):
    """Runtime ratio metric base class."""
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
        # 兼容 runtime（新）与 run_status（历史）
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

    @classmethod
    def _build_runtime_where(
        cls,
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
        cls._append_sub_equipment_condition(
            conditions,
            params,
            ctx.sub_equipment_id,
        )
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
        clamp_threshold = self._negative_delta_clamp_threshold

        try:
            with self.db.cursor() as cursor:
                cursor.execute("SET @ndc_threshold = %s", [clamp_threshold])
                selected_metric: Optional[str] = None
                selected_row = None
                selected_sql = ""
                for metric_name in self._metric_candidates:
                    where, params = self._build_runtime_where(
                        ctx, metric_name, scope_equipment_type, scope_equipment_types)
                    sql = f"""
                        SELECT
                            SUM(
                                CASE
                                    WHEN agg_delta < 0 THEN 0
                                    ELSE agg_delta
                                END
                            ) AS total_runtime,
                            SUM(
                                CASE
                                    WHEN agg_delta < 0 AND agg_delta >= -@ndc_threshold THEN agg_delta
                                    ELSE 0
                                END
                            ) AS clamped_negative_total,
                            SUM(
                                CASE
                                    WHEN agg_delta < 0 AND agg_delta >= -@ndc_threshold THEN 1
                                    ELSE 0
                                END
                            ) AS clamped_negative_count,
                            SUM(
                                CASE
                                    WHEN agg_delta < -@ndc_threshold THEN agg_delta
                                    ELSE 0
                                END
                            ) AS severe_negative_total,
                            SUM(
                                CASE
                                    WHEN agg_delta < -@ndc_threshold THEN 1
                                    ELSE 0
                                END
                            ) AS severe_negative_count,
                            COUNT(DISTINCT equipment_id) AS device_count,
                            COUNT(*) AS record_count
                        FROM agg_hour
                        WHERE {where}
                    """
                    query_params: List[Any] = [*params]
                    cursor.execute(sql, query_params)
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
                        metric_name=self.metric_name,
                        value=None,
                        unit=self.unit,
                        status="no_data",
                        formula=self.formula,
                        quality_score=0.0,
                        quality_issues=missing_issues,
                    )

                total_runtime = float(selected_row["total_runtime"] or 0)
                record_count = int(selected_row["record_count"] or 0)
                device_count = max(1, int(selected_row["device_count"] or 0))
                max_runtime = float(record_count)  # 最小可算：实际有数据的设备·小时

                ratio = 0.0 if max_runtime == 0 else round(total_runtime / max_runtime * 100, 2)

                quality_score, quality_issues = self._check_quality_from_table(
                    cursor,
                    ctx,
                    [selected_metric],
                    equipment_type=scope_equipment_type,
                    equipment_types=scope_equipment_types,
                )

                calc_issues: List[dict[str, Any]] = []
                if selected_metric != "runtime":
                    calc_issues.append({
                        "type": "fallback_data_source",
                        "description": "runtime 缺失，已回退使用 run_status 增量口径",
                        "details": {"metric_used": selected_metric},
                    })

                clamped_negative_count = int(selected_row.get("clamped_negative_count") or 0)
                clamped_negative_total = float(selected_row.get("clamped_negative_total") or 0.0)
                if clamped_negative_count > 0:
                    calc_issues.append({
                        "type": "negative_delta_clamped",
                        "description": (
                            f"Detected {clamped_negative_count} small negative deltas "
                            f"(-{clamp_threshold} <= agg_delta < 0); excluded from SUM."
                        ),
                        "count": clamped_negative_count,
                        "details": {
                            "clamp_threshold": clamp_threshold,
                            "clamped_negative_total": round(clamped_negative_total, 2),
                            "policy": "-threshold <= agg_delta < 0 -> excluded_from_sum",
                        },
                    })

                severe_negative_count = int(selected_row.get("severe_negative_count") or 0)
                severe_negative_total = float(selected_row.get("severe_negative_total") or 0.0)
                if severe_negative_count > 0:
                    calc_issues.append({
                        "type": "negative_delta_alert",
                        "description": (
                            f"Detected {severe_negative_count} severe negative deltas "
                            f"(agg_delta < -{clamp_threshold}); excluded from SUM and flagged."
                        ),
                        "count": severe_negative_count,
                        "details": {
                            "clamp_threshold": clamp_threshold,
                            "severe_negative_total": round(severe_negative_total, 2),
                            "policy": "agg_delta < -threshold -> excluded_from_sum_and_alert",
                        },
                    })

                filtered_negative_count = clamped_negative_count + severe_negative_count
                filtered_negative_total = clamped_negative_total + severe_negative_total
                if filtered_negative_count > 0:
                    calc_issues.append({
                        "type": "result_beautified",
                        "description": (
                            "This metric uses cleaned SUM scope: all negative deltas are excluded."
                        ),
                        "count": filtered_negative_count,
                        "details": {
                            "clamp_threshold": clamp_threshold,
                            "filtered_negative_count": filtered_negative_count,
                            "filtered_negative_total": round(filtered_negative_total, 2),
                            "policy": "agg_delta < 0 -> excluded_from_sum",
                        },
                    })

                if ratio < 0 or ratio > 100:
                    calc_issues.append({
                        "type": "ratio_out_of_range",
                        "description": "运行时长占比超出 0-100%，已保留真实值并告警",
                        "details": {"raw_ratio": round(ratio, 2)},
                    })

                delta = ctx.time_end - ctx.time_start
                period_hours = max(0.0, delta.total_seconds() / 3600)
                theoretical_max = period_hours * device_count
                calc_issues.append({
                    "type": "minimum_calculable_principle",
                    "description": (
                        f"运行时长占比基于实际有数据的 {record_count} 设备·小时计算"
                        f"（理论最大 {round(theoretical_max, 1)} = {round(period_hours, 1)}h × {device_count}台）"
                    ),
                    "details": {
                        "intersection_hours": record_count,
                        "expected_hours": int(round(theoretical_max)),
                        "actual_device_hours": record_count,
                        "theoretical_device_hours": round(theoretical_max, 1),
                        "device_count": device_count,
                        "period_hours": round(period_hours, 1),
                        "coverage_rate": round(record_count / theoretical_max * 100, 1) if theoretical_max > 0 else 0,
                    },
                })

                all_issues = quality_issues + calc_issues
                formula_with_values = (
                    f"= {round(total_runtime, 1)}h / {record_count}设备·小时"
                    f" ({device_count}台设备, 实际覆盖) = {ratio}%"
                )

                return CalculationResult(
                    metric_name=self.metric_name,
                    value=ratio,
                    unit=self.unit,
                    status=self._status_from_issues(all_issues),
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
                metric_name=self.metric_name,
                value=None,
                unit=self.unit,
                status="failed",
                formula=self.formula,
                quality_issues=[{
                    "type": "error", "description": str(e),
                }],
            )


class ChillerRuntimeRatioMetric(_RuntimeRatioMetric):
    """Chiller runtime ratio metric."""
    @property
    def metric_name(self) -> str:
        return "冷机运行时长占比"

    @property
    def formula(self) -> str:
        return "冷机运行时长占比 = 冷机运行时长 / (评估周期 x 设备数 x 100%"

    @property
    def _equipment_type(self) -> Optional[str]:
        return "chiller"


class TowerFanRuntimeRatioMetric(_RuntimeRatioMetric):
    """Tower fan runtime ratio metric."""
    @property
    def metric_name(self) -> str:
        return "风机运行时长占比"

    @property
    def formula(self) -> str:
        return "风机运行时长占比 = 风机运行时长 / (评估周期 x 设备数 x 100%"

    @property
    def _equipment_types(self) -> Optional[List[str]]:
        return ["tower_fan", "cooling_tower", "cooling_tower_closed"]


