"""冷却塔效率指标计算"""
from typing import Any, List, Optional

from .base import BaseMetric, MetricContext, CalculationResult


class CoolingWaterDeltaTMetric(BaseMetric):
    """冷却水温差"""

    @property
    def metric_name(self) -> str:
        return "冷却水温差"

    @property
    def unit(self) -> str:
        return "℃"

    @property
    def formula(self) -> str:
        return "冷却水温差 = AVG(cooling_return_temp) - AVG(cooling_supply_temp)"

    def calculate(self, ctx: MetricContext) -> CalculationResult:
        where_ret, params_ret = self._build_where(ctx, "cooling_return_temp")
        where_sup, params_sup = self._build_where(ctx, "cooling_supply_temp")

        sql_ret = f"SELECT AVG(agg_avg) AS v, COUNT(*) AS n FROM agg_hour WHERE {where_ret}"
        sql_sup = f"SELECT AVG(agg_avg) AS v, COUNT(*) AS n FROM agg_hour WHERE {where_sup}"

        try:
            with self.db.cursor() as cursor:
                cursor.execute(sql_ret, params_ret)
                r_ret = cursor.fetchone()
                cursor.execute(sql_sup, params_sup)
                r_sup = cursor.fetchone()

                if (not r_ret or int(r_ret["n"] or 0) == 0
                        or not r_sup or int(r_sup["n"] or 0) == 0):
                    missing_issues = self._build_missing_dependency_issues(
                        cursor, ctx, ["cooling_return_temp", "cooling_supply_temp"])
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
                total_records = int(r_ret["n"] or 0) + int(r_sup["n"] or 0)
                quality_score, quality_issues = self._check_quality_from_table(
                    cursor, ctx, ["cooling_return_temp", "cooling_supply_temp"])

                return CalculationResult(
                    metric_name=self.metric_name, value=delta,
                    unit=self.unit, status=self._status_from_issues(quality_issues),
                    formula=self.formula,
                    formula_with_values=f"= {ret_val} - {sup_val} = {delta}℃",
                    sql_executed=f"{sql_ret.strip()}; {sql_sup.strip()}",
                    input_records=total_records,
                    valid_records=total_records,
                    data_source_condition="metric_name IN ('cooling_return_temp','cooling_supply_temp')",
                    quality_score=quality_score,
                    quality_issues=quality_issues,
                )
        except Exception as e:
            return CalculationResult(
                metric_name=self.metric_name, value=None, unit=self.unit,
                status="failed", formula=self.formula,
                quality_issues=[{"type": "error", "description": str(e)}],
            )


class TowerFanPowerMetric(BaseMetric):
    """冷却塔风机功率"""

    TOWER_TYPES = ["cooling_tower", "cooling_tower_closed", "tower_fan"]

    @property
    def metric_name(self) -> str:
        return "冷却塔风机功率"

    @property
    def unit(self) -> str:
        return "kW"

    @property
    def formula(self) -> str:
        return "冷却塔风机功率 = AVG(power) WHERE equipment_type IN (cooling_tower, cooling_tower_closed, tower_fan)"

    def _build_sparse_issue(
        self,
        cursor,
        where_clause: str,
        params: List[Any],
        ctx: MetricContext,
    ) -> Optional[dict]:
        # 用户已经精确到 system/equipment 时，不再提示全局覆盖稀疏
        if ctx.system_id or ctx.equipment_id:
            return None

        coverage_sql = f"""
            SELECT
                COUNT(DISTINCT system_id) AS system_count,
                GROUP_CONCAT(DISTINCT system_id ORDER BY system_id SEPARATOR ',') AS systems
            FROM agg_hour
            WHERE {where_clause}
        """
        cursor.execute(coverage_sql, params)
        row = cursor.fetchone()
        system_count = int(row["system_count"] or 0)
        systems = row.get("systems") or ""

        if system_count <= 1:
            return {
                "type": "sparse",
                "description": "冷却塔功率数据覆盖系统较少，结果代表性可能不足",
                "details": {
                    "covered_system_count": system_count,
                    "covered_systems": systems.split(",") if systems else [],
                },
            }
        return None

    def calculate(self, ctx: MetricContext) -> CalculationResult:
        conditions = [
            "metric_name = %s",
            "bucket_time >= %s",
            "bucket_time < %s",
        ]
        params: List[Any] = ["power", ctx.time_start, ctx.time_end]

        if ctx.equipment_type:
            conditions.append("equipment_type = %s")
            params.append(ctx.equipment_type)
            quality_equipment_type: Optional[str] = ctx.equipment_type
            quality_equipment_types: Optional[List[str]] = None
        else:
            placeholders = ", ".join(["%s"] * len(self.TOWER_TYPES))
            conditions.append(f"equipment_type IN ({placeholders})")
            params.extend(self.TOWER_TYPES)
            quality_equipment_type = None
            quality_equipment_types = self.TOWER_TYPES

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

        where = " AND ".join(conditions)

        sql = f"""
            SELECT
                AVG(agg_avg) AS avg_val,
                COUNT(*) AS record_count
            FROM agg_hour
            WHERE {where}
        """

        try:
            with self.db.cursor() as cursor:
                cursor.execute(sql, params)
                row = cursor.fetchone()

                if not row or int(row["record_count"] or 0) == 0:
                    if quality_equipment_type:
                        missing_issues = self._build_missing_dependency_issues(
                            cursor, ctx, ["power"], equipment_type=quality_equipment_type)
                    else:
                        missing_issues = self._build_missing_dependency_issues(
                            cursor, ctx, ["power"], equipment_types=quality_equipment_types)
                    return CalculationResult(
                        metric_name=self.metric_name, value=None,
                        unit=self.unit, status="no_data",
                        formula=self.formula,
                        quality_score=0.0,
                        quality_issues=missing_issues,
                    )

                val = round(float(row["avg_val"] or 0), 2)
                quality_score, quality_issues = self._check_quality_from_table(
                    cursor, ctx, ["power"],
                    equipment_type=quality_equipment_type,
                    equipment_types=quality_equipment_types)

                sparse_issue = self._build_sparse_issue(cursor, where, params, ctx)
                all_issues = quality_issues + ([sparse_issue] if sparse_issue else [])

                return CalculationResult(
                    metric_name=self.metric_name, value=val,
                    unit=self.unit, status=self._status_from_issues(all_issues),
                    formula=self.formula,
                    formula_with_values=f"= AVG = {val} kW",
                    sql_executed=sql.strip(),
                    input_records=int(row["record_count"] or 0),
                    valid_records=int(row["record_count"] or 0),
                    data_source_condition=(
                        "metric_name='power', equipment_type IN "
                        "('cooling_tower','cooling_tower_closed','tower_fan')"
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
