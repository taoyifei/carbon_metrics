"""温度与温差指标计算"""
from math import ceil
from typing import Any, List

from .base import BaseMetric, MetricContext, CalculationResult


class _SingleTempMetric(BaseMetric):
    """单一温度指标基类。"""

    @property
    def unit(self) -> str:
        return "℃"

    @property
    def _metric_name_db(self) -> str:
        raise NotImplementedError

    def calculate(self, ctx: MetricContext) -> CalculationResult:
        where, params = self._build_where(ctx, self._metric_name_db)

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
                        cursor, ctx, [self._metric_name_db]
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
                    cursor, ctx, [self._metric_name_db]
                )

                return CalculationResult(
                    metric_name=self.metric_name,
                    value=val,
                    unit=self.unit,
                    status=self._status_from_issues(quality_issues),
                    formula=self.formula,
                    formula_with_values=(
                        f"= AVG = {val}℃ (min={row['min_val']}, max={row['max_val']})"
                    ),
                    sql_executed=sql.strip(),
                    input_records=int(row["record_count"] or 0),
                    valid_records=int(row["record_count"] or 0),
                    data_source_condition=f"metric_name='{self._metric_name_db}'",
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


class ChilledSupplyTempMetric(_SingleTempMetric):
    """冷冻水供水温度。"""

    @property
    def metric_name(self) -> str:
        return "冷冻水供水温度"

    @property
    def formula(self) -> str:
        return "冷冻水供水温度 = AVG(chilled_supply_temp)"

    @property
    def _metric_name_db(self) -> str:
        return "chilled_supply_temp"


class ChilledReturnTempMetric(_SingleTempMetric):
    """冷冻水回水温度。"""

    @property
    def metric_name(self) -> str:
        return "冷冻水回水温度"

    @property
    def formula(self) -> str:
        return "冷冻水回水温度 = AVG(chilled_return_temp)"

    @property
    def _metric_name_db(self) -> str:
        return "chilled_return_temp"


class CoolingSupplyTempMetric(_SingleTempMetric):
    """冷却水供水温度。"""

    @property
    def metric_name(self) -> str:
        return "冷却水供水温度"

    @property
    def formula(self) -> str:
        return "冷却水供水温度 = AVG(cooling_supply_temp)"

    @property
    def _metric_name_db(self) -> str:
        return "cooling_supply_temp"


class CoolingReturnTempMetric(_SingleTempMetric):
    """冷却水回水温度。"""

    @property
    def metric_name(self) -> str:
        return "冷却水回水温度"

    @property
    def formula(self) -> str:
        return "冷却水回水温度 = AVG(cooling_return_temp)"

    @property
    def _metric_name_db(self) -> str:
        return "cooling_return_temp"


class ChilledWaterDeltaTMetric(BaseMetric):
    """冷冻水温差。"""

    @property
    def metric_name(self) -> str:
        return "冷冻水温差"

    @property
    def unit(self) -> str:
        return "℃"

    @property
    def formula(self) -> str:
        return "冷冻水温差 = AVG(chilled_return_temp - chilled_supply_temp)，按小时对齐"

    def calculate(self, ctx: MetricContext) -> CalculationResult:
        where_ret, params_ret = self._build_where(ctx, "chilled_return_temp")
        where_sup, params_sup = self._build_where(ctx, "chilled_supply_temp")

        sql = f"""
            WITH ret_hour AS (
                SELECT bucket_time, AVG(agg_avg) AS ret_avg, COUNT(*) AS ret_cnt
                FROM agg_hour
                WHERE {where_ret}
                GROUP BY bucket_time
            ),
            sup_hour AS (
                SELECT bucket_time, AVG(agg_avg) AS sup_avg, COUNT(*) AS sup_cnt
                FROM agg_hour
                WHERE {where_sup}
                GROUP BY bucket_time
            )
            SELECT
                COUNT(*) AS overlapped_hours,
                AVG(rh.ret_avg) AS avg_ret,
                AVG(sh.sup_avg) AS avg_sup,
                AVG(rh.ret_avg - sh.sup_avg) AS avg_delta_t,
                SUM(rh.ret_cnt) + SUM(sh.sup_cnt) AS total_records
            FROM ret_hour rh
            JOIN sup_hour sh ON sh.bucket_time = rh.bucket_time
        """

        try:
            with self.db.cursor() as cursor:
                cursor.execute(sql, params_ret + params_sup)
                row = cursor.fetchone()

                if not row or int(row["overlapped_hours"] or 0) == 0:
                    missing_issues = self._build_missing_dependency_issues(
                        cursor, ctx, ["chilled_return_temp", "chilled_supply_temp"]
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

                ret_val = round(float(row["avg_ret"] or 0), 2)
                sup_val = round(float(row["avg_sup"] or 0), 2)
                delta = round(float(row["avg_delta_t"] or 0), 2)
                overlapped_hours = int(row["overlapped_hours"] or 0)
                total_records = int(row["total_records"] or 0)
                quality_score, quality_issues = self._check_quality_from_table(
                    cursor, ctx, ["chilled_return_temp", "chilled_supply_temp"]
                )

                expected_hours = max(1, int(ceil((ctx.time_end - ctx.time_start).total_seconds() / 3600)))
                quality_issues = quality_issues + [{
                    "type": "minimum_calculable_principle",
                    "description": f"基于 {overlapped_hours}/{expected_hours} 小时交集计算（各组件按 bucket_time 对齐）",
                    "details": {
                        "intersection_hours": overlapped_hours,
                        "expected_hours": expected_hours,
                        "overlapped_hours": overlapped_hours,
                        "components": ["chilled_return_temp", "chilled_supply_temp"],
                        "join_key": "bucket_time",
                    },
                }]

                return CalculationResult(
                    metric_name=self.metric_name,
                    value=delta,
                    unit=self.unit,
                    status=self._status_from_issues(quality_issues),
                    formula=self.formula,
                    formula_with_values=(
                        f"= AVG(return-supply, aligned {overlapped_hours}h) = {delta}℃ "
                        f"(avg_return={ret_val}, avg_supply={sup_val})"
                    ),
                    sql_executed=sql.strip(),
                    input_records=total_records,
                    valid_records=total_records,
                    data_source_condition=(
                        "metric_name IN ('chilled_return_temp','chilled_supply_temp'), "
                        "aligned by bucket_time"
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
