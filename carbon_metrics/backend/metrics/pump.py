"""
水泵效率指标计算
"""
from .base import BaseMetric, MetricContext, CalculationResult


class _PumpFrequencyMetric(BaseMetric):
    """水泵频率指标基类"""

    @property
    def unit(self) -> str:
        return "Hz"

    @property
    def _equipment_type(self) -> str:
        raise NotImplementedError

    def calculate(self, ctx: MetricContext) -> CalculationResult:
        where, params = self._build_where(
            ctx, "frequency", equipment_type=self._equipment_type)

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
                        cursor, ctx, ["frequency"], equipment_type=self._equipment_type)
                    return CalculationResult(
                        metric_name=self.metric_name, value=None,
                        unit=self.unit, status="no_data",
                        formula=self.formula,
                        quality_score=0.0,
                        quality_issues=missing_issues,
                    )

                val = round(float(row["avg_val"]), 2)
                quality_score, quality_issues = self._check_quality_from_table(
                    cursor, ctx, ["frequency"], equipment_type=self._equipment_type)

                return CalculationResult(
                    metric_name=self.metric_name, value=val,
                    unit=self.unit, status=self._status_from_issues(quality_issues),
                    formula=self.formula,
                    formula_with_values=(
                        f"= AVG = {val} Hz"
                        f" (min={row['min_val']}, max={row['max_val']})"
                    ),
                    sql_executed=sql.strip(),
                    input_records=int(row["record_count"]),
                    valid_records=int(row["record_count"]),
                    data_source_condition=f"metric_name='frequency', equipment_type='{self._equipment_type}'",
                    quality_score=quality_score,
                    quality_issues=quality_issues,
                )
        except Exception as e:
            return CalculationResult(
                metric_name=self.metric_name, value=None, unit=self.unit,
                status="failed", formula=self.formula,
                quality_issues=[{"type": "error", "description": str(e)}],
            )


class ChilledPumpFrequencyMetric(_PumpFrequencyMetric):
    """冷冻泵工作频率"""

    @property
    def metric_name(self) -> str:
        return "冷冻泵工作频率"

    @property
    def formula(self) -> str:
        return "冷冻泵工作频率 = AVG(frequency) WHERE equipment_type='chilled_pump'"

    @property
    def _equipment_type(self) -> str:
        return "chilled_pump"


class CoolingPumpFrequencyMetric(_PumpFrequencyMetric):
    """冷却泵工作频率"""

    @property
    def metric_name(self) -> str:
        return "冷却泵工作频率"

    @property
    def formula(self) -> str:
        return "冷却泵工作频率 = AVG(frequency) WHERE equipment_type='cooling_pump'"

    @property
    def _equipment_type(self) -> str:
        return "cooling_pump"
