"""
温度与温差指标计算
"""
from typing import List, Any
from .base import BaseMetric, MetricContext, CalculationResult


class _SingleTempMetric(BaseMetric):
    """单一温度指标基类"""

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

                if not row or row["record_count"] == 0:
                    missing_issues = self._build_missing_dependency_issues(
                        cursor, ctx, [self._metric_name_db])
                    return CalculationResult(
                        metric_name=self.metric_name, value=None,
                        unit=self.unit, status="no_data", formula=self.formula,
                        quality_score=0.0,
                        quality_issues=missing_issues,
                    )

                val = round(float(row["avg_val"] or 0), 2)
                quality_score, quality_issues = self._check_quality_from_table(
                    cursor, ctx, [self._metric_name_db])

                return CalculationResult(
                    metric_name=self.metric_name,
                    value=val,
                    unit=self.unit,
                    status=self._status_from_issues(quality_issues),
                    formula=self.formula,
                    formula_with_values=f"= AVG = {val}℃ (min={row['min_val']}, max={row['max_val']})",
                    sql_executed=sql.strip(),
                    input_records=int(row["record_count"]),
                    valid_records=int(row["record_count"]),
                    data_source_condition=f"metric_name='{self._metric_name_db}'",
                    quality_score=quality_score,
                    quality_issues=quality_issues,
                )
        except Exception as e:
            return CalculationResult(
                metric_name=self.metric_name, value=None, unit=self.unit,
                status="failed", formula=self.formula,
                quality_issues=[{"type": "error", "description": str(e)}],
            )


class ChilledSupplyTempMetric(_SingleTempMetric):
    """冷冻水供水温度"""

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
    """冷冻水回水温度"""

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
    """冷却水供水温度"""

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
    """冷却水回水温度"""

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
    """冷冻水温差"""

    @property
    def metric_name(self) -> str:
        return "冷冻水温差"

    @property
    def unit(self) -> str:
        return "℃"

    @property
    def formula(self) -> str:
        return "冷冻水温差 = AVG(chilled_return_temp) - AVG(chilled_supply_temp)"

    def calculate(self, ctx: MetricContext) -> CalculationResult:
        where_ret, params_ret = self._build_where(ctx, "chilled_return_temp")
        where_sup, params_sup = self._build_where(ctx, "chilled_supply_temp")

        sql_ret = f"SELECT AVG(agg_avg) AS v, COUNT(*) AS n FROM agg_hour WHERE {where_ret}"
        sql_sup = f"SELECT AVG(agg_avg) AS v, COUNT(*) AS n FROM agg_hour WHERE {where_sup}"

        try:
            with self.db.cursor() as cursor:
                cursor.execute(sql_ret, params_ret)
                r_ret = cursor.fetchone()
                cursor.execute(sql_sup, params_sup)
                r_sup = cursor.fetchone()

                if (not r_ret or r_ret["n"] == 0
                        or not r_sup or r_sup["n"] == 0):
                    missing_issues = self._build_missing_dependency_issues(
                        cursor, ctx, ["chilled_return_temp", "chilled_supply_temp"])
                    return CalculationResult(
                        metric_name=self.metric_name, value=None,
                        unit=self.unit, status="no_data",
                        formula=self.formula,
                        quality_score=0.0,
                        quality_issues=missing_issues,
                    )

                ret_val = round(float(r_ret["v"] or 0), 2)
                sup_val = round(float(r_sup["v"] or 0), 2)
                delta = round(ret_val - sup_val, 2)
                total_records = int(r_ret["n"]) + int(r_sup["n"])
                quality_score, quality_issues = self._check_quality_from_table(
                    cursor, ctx, ["chilled_return_temp", "chilled_supply_temp"])

                return CalculationResult(
                    metric_name=self.metric_name, value=delta,
                    unit=self.unit, status=self._status_from_issues(quality_issues),
                    formula=self.formula,
                    formula_with_values=f"= {ret_val} - {sup_val} = {delta}℃",
                    sql_executed=f"{sql_ret.strip()}; {sql_sup.strip()}",
                    input_records=total_records,
                    valid_records=total_records,
                    data_source_condition="metric_name IN ('chilled_return_temp','chilled_supply_temp')",
                    quality_score=quality_score,
                    quality_issues=quality_issues,
                )
        except Exception as e:
            return CalculationResult(
                metric_name=self.metric_name, value=None, unit=self.unit,
                status="failed", formula=self.formula,
                quality_issues=[{"type": "error", "description": str(e)}],
            )
