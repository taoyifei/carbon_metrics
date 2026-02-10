"""
流量效率指标计算
"""
from typing import List, Any
from .base import BaseMetric, MetricContext, CalculationResult


class ChilledFlowMetric(BaseMetric):
    """冷冻水流量"""

    @property
    def metric_name(self) -> str:
        return "冷冻水流量"

    @property
    def unit(self) -> str:
        return "m³/h"

    @property
    def formula(self) -> str:
        return "冷冻水流量 = AVG(chilled_flow)"

    def calculate(self, ctx: MetricContext) -> CalculationResult:
        where, params = self._build_where(ctx, "chilled_flow")

        sql = f"""
            SELECT
                AVG(agg_avg) AS avg_val,
                MIN(agg_min) AS min_val,
                MAX(agg_max) AS max_val,
                COUNT(*) AS record_count
            FROM agg_hour
            WHERE {where}
        """

        try:
            with self.db.cursor() as cursor:
                cursor.execute(sql, params)
                row = cursor.fetchone()

                if not row or row["record_count"] == 0:
                    missing_issues = self._build_missing_dependency_issues(
                        cursor, ctx, ["chilled_flow"])
                    return CalculationResult(
                        metric_name=self.metric_name, value=None,
                        unit=self.unit, status="no_data",
                        formula=self.formula,
                        quality_score=0.0,
                        quality_issues=missing_issues,
                    )

                val = round(float(row["avg_val"]), 2)
                quality_score, quality_issues = self._check_quality_from_table(
                    cursor, ctx, ["chilled_flow"])

                return CalculationResult(
                    metric_name=self.metric_name, value=val,
                    unit=self.unit, status=self._status_from_issues(quality_issues),
                    formula=self.formula,
                    formula_with_values=f"= AVG = {val} m³/h",
                    sql_executed=sql.strip(),
                    input_records=int(row["record_count"]),
                    valid_records=int(row["record_count"]),
                    data_source_condition="metric_name='chilled_flow'",
                    quality_score=quality_score,
                    quality_issues=quality_issues,
                )
        except Exception as e:
            return CalculationResult(
                metric_name=self.metric_name, value=None, unit=self.unit,
                status="failed", formula=self.formula,
                quality_issues=[{"type": "error", "description": str(e)}],
            )


class CoolingFlowMetric(BaseMetric):
    """冷却水流量"""

    @property
    def metric_name(self) -> str:
        return "冷却水流量"

    @property
    def unit(self) -> str:
        return "m³/h"

    @property
    def formula(self) -> str:
        return "冷却水流量 = AVG(cooling_flow)"

    def calculate(self, ctx: MetricContext) -> CalculationResult:
        where, params = self._build_where(ctx, "cooling_flow")

        sql = f"""
            SELECT
                AVG(agg_avg) AS avg_val,
                MIN(agg_min) AS min_val,
                MAX(agg_max) AS max_val,
                COUNT(*) AS record_count
            FROM agg_hour
            WHERE {where}
        """

        try:
            with self.db.cursor() as cursor:
                cursor.execute(sql, params)
                row = cursor.fetchone()

                if not row or row["record_count"] == 0:
                    missing_issues = self._build_missing_dependency_issues(
                        cursor, ctx, ["cooling_flow"])
                    return CalculationResult(
                        metric_name=self.metric_name, value=None,
                        unit=self.unit, status="no_data",
                        formula=self.formula,
                        quality_score=0.0,
                        quality_issues=missing_issues,
                    )

                val = round(float(row["avg_val"]), 2)
                quality_score, quality_issues = self._check_quality_from_table(
                    cursor, ctx, ["cooling_flow"])

                return CalculationResult(
                    metric_name=self.metric_name, value=val,
                    unit=self.unit, status=self._status_from_issues(quality_issues),
                    formula=self.formula,
                    formula_with_values=f"= AVG = {val} m³/h",
                    sql_executed=sql.strip(),
                    input_records=int(row["record_count"]),
                    valid_records=int(row["record_count"]),
                    data_source_condition="metric_name='cooling_flow'",
                    quality_score=quality_score,
                    quality_issues=quality_issues,
                )
        except Exception as e:
            return CalculationResult(
                metric_name=self.metric_name, value=None, unit=self.unit,
                status="failed", formula=self.formula,
                quality_issues=[{"type": "error", "description": str(e)}],
            )


class CoolingCapacityMetric(BaseMetric):
    """制冷量"""

    @property
    def metric_name(self) -> str:
        return "制冷量"

    @property
    def unit(self) -> str:
        return "kW"

    @property
    def formula(self) -> str:
        return "制冷量 = chilled_flow × (chilled_return_temp - chilled_supply_temp) × 1.163"

    def calculate(self, ctx: MetricContext) -> CalculationResult:
        where_flow, params_flow = self._build_where(ctx, "chilled_flow")
        where_ret, params_ret = self._build_where(ctx, "chilled_return_temp")
        where_sup, params_sup = self._build_where(ctx, "chilled_supply_temp")

        sql_flow = f"SELECT AVG(agg_avg) AS v, COUNT(*) AS n FROM agg_hour WHERE {where_flow}"
        sql_ret = f"SELECT AVG(agg_avg) AS v, COUNT(*) AS n FROM agg_hour WHERE {where_ret}"
        sql_sup = f"SELECT AVG(agg_avg) AS v, COUNT(*) AS n FROM agg_hour WHERE {where_sup}"

        try:
            with self.db.cursor() as cursor:
                cursor.execute(sql_flow, params_flow)
                r_flow = cursor.fetchone()
                cursor.execute(sql_ret, params_ret)
                r_ret = cursor.fetchone()
                cursor.execute(sql_sup, params_sup)
                r_sup = cursor.fetchone()

                if (not r_flow or r_flow["n"] == 0
                        or not r_ret or r_ret["n"] == 0
                        or not r_sup or r_sup["n"] == 0):
                    missing_issues = self._build_missing_dependency_issues(
                        cursor, ctx,
                        ["chilled_flow", "chilled_return_temp", "chilled_supply_temp"])
                    return CalculationResult(
                        metric_name=self.metric_name, value=None,
                        unit=self.unit, status="no_data",
                        formula=self.formula,
                        quality_score=0.0,
                        quality_issues=missing_issues,
                    )

                flow_val = round(float(r_flow["v"]), 2)
                ret_val = round(float(r_ret["v"]), 2)
                sup_val = round(float(r_sup["v"]), 2)
                delta_t = round(ret_val - sup_val, 2)
                capacity = round(flow_val * delta_t * 1.163, 2)
                total_records = int(r_flow["n"]) + int(r_ret["n"]) + int(r_sup["n"])
                quality_score, quality_issues = self._check_quality_from_table(
                    cursor, ctx,
                    ["chilled_flow", "chilled_return_temp", "chilled_supply_temp"])

                return CalculationResult(
                    metric_name=self.metric_name, value=capacity,
                    unit=self.unit, status=self._status_from_issues(quality_issues),
                    formula=self.formula,
                    formula_with_values=(
                        f"= {flow_val} m³/h × ({ret_val} - {sup_val})℃ × 1.163"
                        f" = {flow_val} × {delta_t} × 1.163 = {capacity} kW"
                    ),
                    sql_executed=f"{sql_flow.strip()}; {sql_ret.strip()}; {sql_sup.strip()}",
                    input_records=total_records,
                    valid_records=total_records,
                    data_source_condition="metric_name IN ('chilled_flow','chilled_return_temp','chilled_supply_temp')",
                    quality_score=quality_score,
                    quality_issues=quality_issues,
                )
        except Exception as e:
            return CalculationResult(
                metric_name=self.metric_name, value=None, unit=self.unit,
                status="failed", formula=self.formula,
                quality_issues=[{"type": "error", "description": str(e)}],
            )
