"""
预测性维护指标计算
"""
from typing import Optional

from .base import BaseMetric, MetricContext, CalculationResult


class ChillerOverloadRiskMetric(BaseMetric):
    """冷机过载风险指数"""

    LOAD_METRIC_CANDIDATES = ["load_rate", "load_ratio"]

    @property
    def metric_name(self) -> str:
        return "过载风险指数"

    @property
    def unit(self) -> str:
        return ""

    @property
    def formula(self) -> str:
        return "过载风险指数 = AVG((负载率 - 80) / 80) WHERE 负载率 > 80"

    def _resolve_load_metric(self, cursor, ctx: MetricContext) -> Optional[str]:
        for metric_name in self.LOAD_METRIC_CANDIDATES:
            where, params = self._build_where(
                ctx, metric_name, equipment_type="chiller")
            cursor.execute(
                f"SELECT COUNT(*) AS total FROM agg_hour WHERE {where}",
                params,
            )
            row = cursor.fetchone()
            if row and int(row["total"] or 0) > 0:
                return metric_name
        return None

    def calculate(self, ctx: MetricContext) -> CalculationResult:
        try:
            with self.db.cursor() as cursor:
                selected_metric = self._resolve_load_metric(cursor, ctx)

                if not selected_metric:
                    missing_issues = self._build_missing_dependency_issues(
                        cursor, ctx, self.LOAD_METRIC_CANDIDATES, equipment_type="chiller")
                    return CalculationResult(
                        metric_name=self.metric_name,
                        value=None, unit=self.unit,
                        status="no_data", formula=self.formula,
                        quality_score=0.0,
                        quality_issues=missing_issues,
                    )

                where_all, params_all = self._build_where(
                    ctx, selected_metric, equipment_type="chiller")
                sql_total = f"""
                    SELECT COUNT(*) AS total
                    FROM agg_hour WHERE {where_all}
                """
                cursor.execute(sql_total, params_all)
                total_row = cursor.fetchone()
                total = int(total_row["total"] or 0)

                where, params = self._build_where(
                    ctx, selected_metric, equipment_type="chiller",
                    extra_conditions=["agg_avg > 80"])
                sql = f"""
                    SELECT
                        AVG((agg_avg - 80) / 80) AS risk_val,
                        COUNT(*) AS overload_count
                    FROM agg_hour
                    WHERE {where}
                """
                cursor.execute(sql, params)
                row = cursor.fetchone()
                overload = int(row["overload_count"] or 0)

                quality_score, quality_issues = self._check_quality_from_table(
                    cursor, ctx, [selected_metric], equipment_type="chiller")

                calc_issues = []
                if selected_metric != "load_rate":
                    calc_issues.append({
                        "type": "fallback_metric_name",
                        "description": "load_rate 缺失，已回退使用 load_ratio",
                        "details": {"metric_used": selected_metric},
                    })
                all_issues = quality_issues + calc_issues

                if overload == 0:
                    return CalculationResult(
                        metric_name=self.metric_name,
                        value=0.0, unit=self.unit,
                        status=self._status_from_issues(all_issues), formula=self.formula,
                        formula_with_values=(
                            f"= 0 (无过载记录，共 {total} 条)"
                        ),
                        sql_executed=sql.strip(),
                        input_records=total,
                        valid_records=total,
                        data_source_condition=(
                            f"metric_name='{selected_metric}', equipment_type='chiller', agg_avg>80"
                        ),
                        quality_score=quality_score,
                        quality_issues=all_issues,
                    )

                risk = round(float(row["risk_val"] or 0), 4)

                return CalculationResult(
                    metric_name=self.metric_name,
                    value=risk, unit=self.unit,
                    status=self._status_from_issues(all_issues), formula=self.formula,
                    formula_with_values=(
                        f"= {risk}"
                        f" ({overload}/{total} 条过载)"
                    ),
                    sql_executed=sql.strip(),
                    input_records=total,
                    valid_records=overload,
                    data_source_condition=(
                        f"metric_name='{selected_metric}', equipment_type='chiller', agg_avg>80"
                    ),
                    quality_score=quality_score,
                    quality_issues=all_issues,
                )
        except Exception as e:
            return CalculationResult(
                metric_name=self.metric_name,
                value=None, unit=self.unit,
                status="failed", formula=self.formula,
                quality_issues=[{
                    "type": "error",
                    "description": str(e),
                }],
            )
