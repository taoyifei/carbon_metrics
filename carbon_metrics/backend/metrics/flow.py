"""流量与制冷量指标计算。"""
from typing import List

from .base import BaseMetric, MetricContext, CalculationResult, COOLING_CAPACITY_FACTOR


class ChilledFlowMetric(BaseMetric):
    """冷冻水流量。"""

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

                if not row or int(row["record_count"] or 0) == 0:
                    missing_issues = self._build_missing_dependency_issues(
                        cursor, ctx, ["chilled_flow"]
                    )
                    return CalculationResult(
                        metric_name=self.metric_name,
                        value=None,
                        unit=self.unit,
                        status="no_data",
                        formula=self.formula,
                        quality_score=0.0,
                        quality_issues=missing_issues,
                    )

                val = round(float(row["avg_val"] or 0), 2)
                quality_score, quality_issues = self._check_quality_from_table(
                    cursor, ctx, ["chilled_flow"]
                )

                return CalculationResult(
                    metric_name=self.metric_name,
                    value=val,
                    unit=self.unit,
                    status=self._status_from_issues(quality_issues),
                    formula=self.formula,
                    formula_with_values=f"= AVG = {val} m³/h",
                    sql_executed=sql.strip(),
                    input_records=int(row["record_count"] or 0),
                    valid_records=int(row["record_count"] or 0),
                    data_source_condition=where,
                    quality_score=quality_score,
                    quality_issues=quality_issues,
                )
        except Exception as e:
            return CalculationResult(
                metric_name=self.metric_name,
                value=None,
                unit=self.unit,
                status="failed",
                formula=self.formula,
                quality_issues=[{"type": "error", "description": str(e)}],
            )


class CoolingFlowMetric(BaseMetric):
    """冷却水流量。"""

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

                if not row or int(row["record_count"] or 0) == 0:
                    missing_issues = self._build_missing_dependency_issues(
                        cursor, ctx, ["cooling_flow"]
                    )
                    return CalculationResult(
                        metric_name=self.metric_name,
                        value=None,
                        unit=self.unit,
                        status="no_data",
                        formula=self.formula,
                        quality_score=0.0,
                        quality_issues=missing_issues,
                    )

                val = round(float(row["avg_val"] or 0), 2)
                quality_score, quality_issues = self._check_quality_from_table(
                    cursor, ctx, ["cooling_flow"]
                )

                return CalculationResult(
                    metric_name=self.metric_name,
                    value=val,
                    unit=self.unit,
                    status=self._status_from_issues(quality_issues),
                    formula=self.formula,
                    formula_with_values=f"= AVG = {val} m³/h",
                    sql_executed=sql.strip(),
                    input_records=int(row["record_count"] or 0),
                    valid_records=int(row["record_count"] or 0),
                    data_source_condition=where,
                    quality_score=quality_score,
                    quality_issues=quality_issues,
                )
        except Exception as e:
            return CalculationResult(
                metric_name=self.metric_name,
                value=None,
                unit=self.unit,
                status="failed",
                formula=self.formula,
                quality_issues=[{"type": "error", "description": str(e)}],
            )


class CoolingCapacityMetric(BaseMetric):
    """制冷量。"""

    @property
    def metric_name(self) -> str:
        return "制冷量"

    @property
    def unit(self) -> str:
        return "kW"

    @property
    def formula(self) -> str:
        factor = round(COOLING_CAPACITY_FACTOR, 4)
        return (
            "制冷量 = AVG(chilled_flow * (chilled_return_temp - chilled_supply_temp) * "
            f"{factor})，按小时对齐"
        )

    def calculate(self, ctx: MetricContext) -> CalculationResult:
        where_flow, params_flow = self._build_where(ctx, "chilled_flow")
        where_ret, params_ret = self._build_where(ctx, "chilled_return_temp")
        where_sup, params_sup = self._build_where(ctx, "chilled_supply_temp")

        factor = COOLING_CAPACITY_FACTOR

        sql = f"""
            WITH flow_hour AS (
                SELECT bucket_time, AVG(agg_avg) AS flow_avg, COUNT(*) AS cnt
                FROM agg_hour
                WHERE {where_flow}
                GROUP BY bucket_time
            ),
            ret_hour AS (
                SELECT bucket_time, AVG(agg_avg) AS ret_avg, COUNT(*) AS cnt
                FROM agg_hour
                WHERE {where_ret}
                GROUP BY bucket_time
            ),
            sup_hour AS (
                SELECT bucket_time, AVG(agg_avg) AS sup_avg, COUNT(*) AS cnt
                FROM agg_hour
                WHERE {where_sup}
                GROUP BY bucket_time
            )
            SELECT
                COUNT(*) AS overlapped_hours,
                AVG(fh.flow_avg * (rh.ret_avg - sh.sup_avg) * %s) AS avg_capacity,
                SUM(fh.flow_avg * (rh.ret_avg - sh.sup_avg) * %s) AS total_capacity,
                AVG(fh.flow_avg) AS avg_flow,
                AVG(rh.ret_avg) AS avg_ret,
                AVG(sh.sup_avg) AS avg_sup,
                SUM(fh.cnt) + SUM(rh.cnt) + SUM(sh.cnt) AS total_records
            FROM flow_hour fh
            JOIN ret_hour rh ON rh.bucket_time = fh.bucket_time
            JOIN sup_hour sh ON sh.bucket_time = fh.bucket_time
        """

        try:
            with self.db.cursor() as cursor:
                cursor.execute(sql, params_flow + params_ret + params_sup + [factor, factor])
                row = cursor.fetchone()

                if not row or int(row["overlapped_hours"] or 0) == 0:
                    missing_issues = self._build_missing_dependency_issues(
                        cursor,
                        ctx,
                        ["chilled_flow", "chilled_return_temp", "chilled_supply_temp"],
                    )
                    return CalculationResult(
                        metric_name=self.metric_name,
                        value=None,
                        unit=self.unit,
                        status="no_data",
                        formula=self.formula,
                        quality_score=0.0,
                        quality_issues=missing_issues,
                    )

                capacity = round(float(row["avg_capacity"] or 0), 2)
                flow_val = round(float(row["avg_flow"] or 0), 2)
                ret_val = round(float(row["avg_ret"] or 0), 2)
                sup_val = round(float(row["avg_sup"] or 0), 2)
                delta_t = round(ret_val - sup_val, 2)
                overlapped = int(row["overlapped_hours"] or 0)
                total_records = int(row["total_records"] or 0)
                factor_display = round(factor, 4)

                quality_score, quality_issues = self._check_quality_from_table(
                    cursor,
                    ctx,
                    ["chilled_flow", "chilled_return_temp", "chilled_supply_temp"],
                )

                return CalculationResult(
                    metric_name=self.metric_name,
                    value=capacity,
                    unit=self.unit,
                    status=self._status_from_issues(quality_issues),
                    formula=self.formula,
                    formula_with_values=(
                        f"= AVG[flow*(return-supply)*{factor_display}] "
                        f"(aligned {overlapped}h) = {capacity} kW; "
                        f"approx_ref={flow_val}*{delta_t}*{factor_display}"
                    ),
                    sql_executed=sql.strip(),
                    input_records=total_records,
                    valid_records=total_records,
                    data_source_condition=(
                        f"flow: {where_flow}; "
                        f"return: {where_ret}; "
                        f"supply: {where_sup}; "
                        "join_on: bucket_time"
                    ),
                    quality_score=quality_score,
                    quality_issues=quality_issues,
                )
        except Exception as e:
            return CalculationResult(
                metric_name=self.metric_name,
                value=None,
                unit=self.unit,
                status="failed",
                formula=self.formula,
                quality_issues=[{"type": "error", "description": str(e)}],
            )
