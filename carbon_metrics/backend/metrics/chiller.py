"""冷机运行效率指标计算"""
from typing import List, Optional

from .base import BaseMetric, MetricContext, CalculationResult

LOAD_METRIC_CANDIDATES = ["load_rate", "load_ratio"]


def _select_load_metric(metric: BaseMetric, cursor, ctx: MetricContext) -> Optional[str]:
    for metric_name in LOAD_METRIC_CANDIDATES:
        where, params = metric._build_where(
            ctx, metric_name, equipment_type="chiller")
        cursor.execute(
            f"SELECT COUNT(*) AS record_count FROM agg_hour WHERE {where}",
            params,
        )
        row = cursor.fetchone()
        if row and int(row["record_count"] or 0) > 0:
            return metric_name
    return None


def _fallback_issues(metric_name_used: str) -> List[dict]:
    if metric_name_used == "load_rate":
        return []
    return [{
        "type": "fallback_metric_name",
        "description": "load_rate 缺失，已回退使用 load_ratio",
        "details": {"metric_used": metric_name_used},
    }]


class ChillerAvgLoadMetric(BaseMetric):
    """冷机平均负载率"""

    @property
    def metric_name(self) -> str:
        return "冷机平均负载率"

    @property
    def unit(self) -> str:
        return "%"

    @property
    def formula(self) -> str:
        return "冷机平均负载率 = AVG(load_rate)"

    def calculate(self, ctx: MetricContext) -> CalculationResult:
        try:
            with self.db.cursor() as cursor:
                selected_metric = _select_load_metric(self, cursor, ctx)
                if not selected_metric:
                    missing_issues = self._build_missing_dependency_issues(
                        cursor, ctx, LOAD_METRIC_CANDIDATES, equipment_type="chiller")
                    return CalculationResult(
                        metric_name=self.metric_name, value=None,
                        unit=self.unit, status="no_data",
                        formula=self.formula,
                        quality_score=0.0,
                        quality_issues=missing_issues,
                    )

                where, params = self._build_where(
                    ctx, selected_metric, equipment_type="chiller")
                sql = f"""
                    SELECT
                        AVG(agg_avg) AS avg_val,
                        COUNT(*) AS record_count
                    FROM agg_hour
                    WHERE {where}
                """
                cursor.execute(sql, params)
                row = cursor.fetchone()

                if not row or int(row["record_count"] or 0) == 0:
                    return CalculationResult(
                        metric_name=self.metric_name, value=None,
                        unit=self.unit, status="no_data",
                        formula=self.formula,
                        quality_score=0.0,
                        quality_issues=[{
                            "type": "missing_dependency",
                            "description": f"缺少依赖数据: {selected_metric}",
                        }],
                    )

                val = round(float(row["avg_val"] or 0), 2)
                quality_score, quality_issues = self._check_quality_from_table(
                    cursor, ctx, [selected_metric], equipment_type="chiller")
                calc_issues = _fallback_issues(selected_metric)
                all_issues = quality_issues + calc_issues

                return CalculationResult(
                    metric_name=self.metric_name, value=val,
                    unit=self.unit, status=self._status_from_issues(all_issues),
                    formula=self.formula,
                    formula_with_values=f"= AVG = {val}%",
                    sql_executed=sql.strip(),
                    input_records=int(row["record_count"] or 0),
                    valid_records=int(row["record_count"] or 0),
                    data_source_condition=(
                        f"metric_name='{selected_metric}', equipment_type='chiller'"
                    ),
                    quality_score=quality_score,
                    quality_issues=all_issues,
                )
        except Exception as e:
            return CalculationResult(
                metric_name=self.metric_name, value=None, unit=self.unit,
                status="failed", formula=self.formula,
                quality_issues=[{"type": "error", "description": str(e)}],
            )


class ChillerMaxLoadMetric(BaseMetric):
    """冷机最大负载率"""

    @property
    def metric_name(self) -> str:
        return "冷机最大负载率"

    @property
    def unit(self) -> str:
        return "%"

    @property
    def formula(self) -> str:
        return "冷机最大负载率 = MAX(load_rate)"

    def calculate(self, ctx: MetricContext) -> CalculationResult:
        try:
            with self.db.cursor() as cursor:
                selected_metric = _select_load_metric(self, cursor, ctx)
                if not selected_metric:
                    missing_issues = self._build_missing_dependency_issues(
                        cursor, ctx, LOAD_METRIC_CANDIDATES, equipment_type="chiller")
                    return CalculationResult(
                        metric_name=self.metric_name, value=None,
                        unit=self.unit, status="no_data",
                        formula=self.formula,
                        quality_score=0.0,
                        quality_issues=missing_issues,
                    )

                where, params = self._build_where(
                    ctx, selected_metric, equipment_type="chiller")
                sql = f"""
                    SELECT
                        MAX(agg_max) AS max_val,
                        COUNT(*) AS record_count
                    FROM agg_hour
                    WHERE {where}
                """
                cursor.execute(sql, params)
                row = cursor.fetchone()

                if not row or int(row["record_count"] or 0) == 0:
                    return CalculationResult(
                        metric_name=self.metric_name, value=None,
                        unit=self.unit, status="no_data",
                        formula=self.formula,
                        quality_score=0.0,
                        quality_issues=[{
                            "type": "missing_dependency",
                            "description": f"缺少依赖数据: {selected_metric}",
                        }],
                    )

                val = round(float(row["max_val"] or 0), 2)
                quality_score, quality_issues = self._check_quality_from_table(
                    cursor, ctx, [selected_metric], equipment_type="chiller")
                calc_issues = _fallback_issues(selected_metric)
                all_issues = quality_issues + calc_issues

                return CalculationResult(
                    metric_name=self.metric_name, value=val,
                    unit=self.unit, status=self._status_from_issues(all_issues),
                    formula=self.formula,
                    formula_with_values=f"= MAX = {val}%",
                    sql_executed=sql.strip(),
                    input_records=int(row["record_count"] or 0),
                    valid_records=int(row["record_count"] or 0),
                    data_source_field="agg_max",
                    data_source_condition=(
                        f"metric_name='{selected_metric}', equipment_type='chiller'"
                    ),
                    quality_score=quality_score,
                    quality_issues=all_issues,
                )
        except Exception as e:
            return CalculationResult(
                metric_name=self.metric_name, value=None, unit=self.unit,
                status="failed", formula=self.formula,
                quality_issues=[{"type": "error", "description": str(e)}],
            )


class ChillerLoadCvMetric(BaseMetric):
    """冷机负载波动系数"""

    @property
    def metric_name(self) -> str:
        return "冷机负载波动系数"

    @property
    def unit(self) -> str:
        return ""

    @property
    def formula(self) -> str:
        return "冷机负载波动系数 = STDDEV(load_rate) / AVG(load_rate)"

    def calculate(self, ctx: MetricContext) -> CalculationResult:
        try:
            with self.db.cursor() as cursor:
                selected_metric = _select_load_metric(self, cursor, ctx)
                if not selected_metric:
                    missing_issues = self._build_missing_dependency_issues(
                        cursor, ctx, LOAD_METRIC_CANDIDATES, equipment_type="chiller")
                    return CalculationResult(
                        metric_name=self.metric_name, value=None,
                        unit=self.unit, status="no_data",
                        formula=self.formula,
                        quality_score=0.0,
                        quality_issues=missing_issues,
                    )

                where, params = self._build_where(
                    ctx, selected_metric, equipment_type="chiller")
                sql = f"""
                    SELECT
                        AVG(agg_avg) AS avg_val,
                        STDDEV(agg_avg) AS std_val,
                        COUNT(*) AS record_count
                    FROM agg_hour
                    WHERE {where}
                """
                cursor.execute(sql, params)
                row = cursor.fetchone()

                if not row or int(row["record_count"] or 0) == 0:
                    return CalculationResult(
                        metric_name=self.metric_name, value=None,
                        unit=self.unit, status="no_data",
                        formula=self.formula,
                        quality_score=0.0,
                        quality_issues=[{
                            "type": "missing_dependency",
                            "description": f"缺少依赖数据: {selected_metric}",
                        }],
                    )

                avg_val = float(row["avg_val"] or 0)
                std_val = float(row["std_val"] or 0)
                cv = 0.0 if avg_val == 0 else round(std_val / avg_val, 4)

                quality_score, quality_issues = self._check_quality_from_table(
                    cursor, ctx, [selected_metric], equipment_type="chiller")
                calc_issues = _fallback_issues(selected_metric)
                all_issues = quality_issues + calc_issues

                return CalculationResult(
                    metric_name=self.metric_name, value=cv,
                    unit=self.unit, status=self._status_from_issues(all_issues),
                    formula=self.formula,
                    formula_with_values=f"= {round(std_val, 2)} / {round(avg_val, 2)} = {cv}",
                    sql_executed=sql.strip(),
                    input_records=int(row["record_count"] or 0),
                    valid_records=int(row["record_count"] or 0),
                    data_source_condition=(
                        f"metric_name='{selected_metric}', equipment_type='chiller'"
                    ),
                    quality_score=quality_score,
                    quality_issues=all_issues,
                )
        except Exception as e:
            return CalculationResult(
                metric_name=self.metric_name, value=None, unit=self.unit,
                status="failed", formula=self.formula,
                quality_issues=[{"type": "error", "description": str(e)}],
            )
