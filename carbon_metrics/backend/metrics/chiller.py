"""冷机运行效率指标计算"""
import os
from math import ceil
from typing import Any, List, Optional

from .base import BaseMetric, MetricContext, CalculationResult, COOLING_CAPACITY_FACTOR

LOAD_METRIC_CANDIDATES = ["load_rate", "load_ratio"]
COP_CAPACITY_FACTOR = COOLING_CAPACITY_FACTOR
DEFAULT_COP_MIN_POWER_KW = 20.0


def _select_load_metric(metric: BaseMetric, cursor, ctx: MetricContext) -> Optional[str]:
    # Dependency probing should ignore sub_equipment scope because load metrics may
    # be stored without main/backup split.
    ctx_without_sub = MetricContext(
        time_start=ctx.time_start,
        time_end=ctx.time_end,
        building_id=ctx.building_id,
        system_id=ctx.system_id,
        equipment_type=ctx.equipment_type,
        equipment_id=ctx.equipment_id,
        sub_equipment_id=None,
    )
    for metric_name in LOAD_METRIC_CANDIDATES:
        where, params = metric._build_where(
            ctx_without_sub, metric_name, equipment_type="chiller")
        sql = f"SELECT COUNT(*) AS record_count FROM agg_hour WHERE {where}"
        row = metric._cached_fetchone(cursor, sql, params)
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


def _merge_issues(*groups: List[dict]) -> List[dict]:
    merged: List[dict] = []
    for group in groups:
        if not group:
            continue
        merged.extend(group)
    return merged


def _parse_positive_float_env(name: str, default: float) -> float:
    raw = os.getenv(name, str(default)).strip()
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return default
    if val <= 0:
        return default
    return val


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

                ctx_without_sub = MetricContext(
                    time_start=ctx.time_start,
                    time_end=ctx.time_end,
                    building_id=ctx.building_id,
                    system_id=ctx.system_id,
                    equipment_type=ctx.equipment_type,
                    equipment_id=ctx.equipment_id,
                    sub_equipment_id=None,
                )
                where, params = self._build_where(
                    ctx_without_sub, selected_metric, equipment_type="chiller")
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

                ctx_without_sub = MetricContext(
                    time_start=ctx.time_start,
                    time_end=ctx.time_end,
                    building_id=ctx.building_id,
                    system_id=ctx.system_id,
                    equipment_type=ctx.equipment_type,
                    equipment_id=ctx.equipment_id,
                    sub_equipment_id=None,
                )
                where, params = self._build_where(
                    ctx_without_sub, selected_metric, equipment_type="chiller")
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
                if val > 100:
                    calc_issues.append({
                        "type": "equipment_overload",
                        "description": "冷机负载率超过100%，设备处于过载运行状态",
                        "details": {
                            "max_load_rate": val,
                            "overload_percentage": round(val - 100, 2),
                        },
                    })
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

                ctx_without_sub = MetricContext(
                    time_start=ctx.time_start,
                    time_end=ctx.time_end,
                    building_id=ctx.building_id,
                    system_id=ctx.system_id,
                    equipment_type=ctx.equipment_type,
                    equipment_id=ctx.equipment_id,
                    sub_equipment_id=None,
                )
                where, params = self._build_where(
                    ctx_without_sub, selected_metric, equipment_type="chiller")
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


class ChillerCopMetric(BaseMetric):
    """冷机COP"""

    @property
    def metric_name(self) -> str:
        return "冷机COP"

    @property
    def unit(self) -> str:
        return ""

    @property
    def formula(self) -> str:
        return (
            "冷机COP = 制冷量 / 冷机输入功率 = "
            "冷冻水流量 × (冷冻水回水温度 - 冷冻水供水温度) × 4.186 / 3.6 ÷ 冷机功率"
        )

    def calculate(self, ctx: MetricContext) -> CalculationResult:
        min_power_kw = _parse_positive_float_env(
            "CHILLER_COP_MIN_POWER_KW", DEFAULT_COP_MIN_POWER_KW
        )
        water_ctx = MetricContext(
            time_start=ctx.time_start,
            time_end=ctx.time_end,
            building_id=ctx.building_id,
            system_id=ctx.system_id,
            equipment_type=None,
            equipment_id=ctx.equipment_id,
            sub_equipment_id=None,
        )
        where_flow, params_flow = self._build_where(water_ctx, "chilled_flow")
        where_ret, params_ret = self._build_where(water_ctx, "chilled_return_temp")
        where_sup, params_sup = self._build_where(water_ctx, "chilled_supply_temp")

        # Default to NULL scope to avoid mixing main/backup channels when caller does not specify scope.
        power_scope_condition = "(sub_equipment_id IS NULL OR sub_equipment_id = '')"
        power_scope_text = "sub_equipment_id IN (NULL, '')"
        if ctx.sub_equipment_id:
            scope_value = ctx.sub_equipment_id.strip().lower()
            if scope_value == "main":
                power_scope_condition = "sub_equipment_id = 'main'"
                power_scope_text = "sub_equipment_id = 'main'"
            elif scope_value == "backup":
                power_scope_condition = "sub_equipment_id = 'backup'"
                power_scope_text = "sub_equipment_id = 'backup'"
            elif self._is_null_sub_equipment_scope(ctx.sub_equipment_id):
                power_scope_condition = "(sub_equipment_id IS NULL OR sub_equipment_id = '')"
                power_scope_text = "sub_equipment_id IN (NULL, '')"
            else:
                safe_scope = ctx.sub_equipment_id.replace("'", "''")
                power_scope_condition = f"sub_equipment_id = '{safe_scope}'"
                power_scope_text = f"sub_equipment_id = '{ctx.sub_equipment_id}'"

        power_ctx = MetricContext(
            time_start=ctx.time_start,
            time_end=ctx.time_end,
            building_id=ctx.building_id,
            system_id=ctx.system_id,
            equipment_type=None,
            equipment_id=ctx.equipment_id,
            sub_equipment_id=None,
        )
        where_power, params_power = self._build_where(
            power_ctx,
            "power",
            equipment_type="chiller",
            extra_conditions=[power_scope_condition],
        )

        sql = f"""
            WITH flow_hour AS (
                SELECT bucket_time, AVG(agg_avg) AS flow_avg, COUNT(*) AS flow_cnt
                FROM agg_hour
                WHERE {where_flow}
                GROUP BY bucket_time
            ),
            ret_hour AS (
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
            ),
            power_hour AS (
                SELECT
                    bucket_time,
                    SUM(agg_avg) AS power_sum_kw,
                    COUNT(*) AS power_points
                FROM agg_hour
                WHERE {where_power}
                GROUP BY bucket_time
            )
            SELECT
                COUNT(*) AS overlapped_hours,
                SUM(CASE WHEN ph.power_sum_kw > %s THEN 1 ELSE 0 END) AS active_hours,
                SUM(
                    CASE WHEN ph.power_sum_kw > %s
                    THEN fh.flow_avg * (rh.ret_avg - sh.sup_avg) * %s
                    ELSE 0 END
                ) AS cooling_capacity_sum_kw,
                SUM(CASE WHEN ph.power_sum_kw > %s THEN ph.power_sum_kw ELSE 0 END) AS chiller_power_sum_kw,
                AVG(fh.flow_avg) AS avg_flow,
                AVG(rh.ret_avg) AS avg_ret,
                AVG(sh.sup_avg) AS avg_sup,
                AVG(ph.power_sum_kw) AS avg_power_kw,
                MIN(ph.power_sum_kw) AS min_power_kw,
                MAX(ph.power_sum_kw) AS max_power_kw,
                SUM(CASE WHEN ph.power_sum_kw <= %s THEN 1 ELSE 0 END) AS low_power_hours,
                SUM(CASE WHEN (rh.ret_avg - sh.sup_avg) <= 0 THEN 1 ELSE 0 END) AS non_positive_delta_t_hours,
                SUM(ph.power_points) AS power_points_total
            FROM flow_hour fh
            JOIN ret_hour rh ON rh.bucket_time = fh.bucket_time
            JOIN sup_hour sh ON sh.bucket_time = fh.bucket_time
            JOIN power_hour ph ON ph.bucket_time = fh.bucket_time
        """

        try:
            with self.db.cursor() as cursor:
                row = self._cached_fetchone(
                    cursor,
                    sql,
                    params_flow
                    + params_ret
                    + params_sup
                    + params_power
                    + [
                        min_power_kw,
                        min_power_kw,
                        COP_CAPACITY_FACTOR,
                        min_power_kw,
                        min_power_kw,
                    ],
                )

                has_flow = self._cached_fetchone(
                    cursor, f"SELECT COUNT(*) AS n FROM agg_hour WHERE {where_flow}", params_flow
                )
                has_ret = self._cached_fetchone(
                    cursor, f"SELECT COUNT(*) AS n FROM agg_hour WHERE {where_ret}", params_ret
                )
                has_sup = self._cached_fetchone(
                    cursor, f"SELECT COUNT(*) AS n FROM agg_hour WHERE {where_sup}", params_sup
                )
                has_power = self._cached_fetchone(
                    cursor, f"SELECT COUNT(*) AS n FROM agg_hour WHERE {where_power}", params_power
                )
                has_flow_data = bool(has_flow and int(has_flow["n"] or 0) > 0)
                has_ret_data = bool(has_ret and int(has_ret["n"] or 0) > 0)
                has_sup_data = bool(has_sup and int(has_sup["n"] or 0) > 0)
                has_power_data = bool(has_power and int(has_power["n"] or 0) > 0)

                if not (has_flow_data and has_ret_data and has_sup_data and has_power_data):
                    missing_issues = _merge_issues(
                        self._build_missing_dependency_issues(
                            cursor,
                            ctx,
                            ["chilled_flow", "chilled_return_temp", "chilled_supply_temp"],
                        ) if not (has_flow_data and has_ret_data and has_sup_data) else [],
                        self._build_missing_dependency_issues(
                            cursor,
                            ctx,
                            ["power"],
                            equipment_type="chiller",
                        ) if not has_power_data else [],
                    )
                    if not has_power_data:
                        missing_issues.append({
                            "type": "missing_main_power",
                            "description": f"冷机功率口径缺失（当前口径: {power_scope_text}）",
                            "details": {
                                "power_scope": f"equipment_type='chiller' AND {power_scope_text}"
                            },
                        })
                    return CalculationResult(
                        metric_name=self.metric_name,
                        value=None,
                        unit=self.unit,
                        status="no_data",
                        formula=self.formula,
                        quality_score=0.0,
                        quality_issues=missing_issues,
                    )

                if not row or int(row["overlapped_hours"] or 0) == 0:
                    return CalculationResult(
                        metric_name=self.metric_name,
                        value=None,
                        unit=self.unit,
                        status="no_data",
                        formula=self.formula,
                        quality_score=0.0,
                        quality_issues=[{
                            "type": "time_alignment_gap",
                            "description": "流量/温度/冷机主功率在当前范围无重叠小时，无法计算COP",
                        }],
                    )

                flow_val = float(row["avg_flow"] or 0.0)
                ret_val = float(row["avg_ret"] or 0.0)
                sup_val = float(row["avg_sup"] or 0.0)
                power_val = float(row["avg_power_kw"] or 0.0)
                cooling_capacity_sum = float(row["cooling_capacity_sum_kw"] or 0.0)
                chiller_power_sum = float(row["chiller_power_sum_kw"] or 0.0)
                active_hours = int(row["active_hours"] or 0)
                overlapped_hours = int(row["overlapped_hours"] or 0)
                low_power_hours = int(row["low_power_hours"] or 0)
                non_positive_delta_t_hours = int(row["non_positive_delta_t_hours"] or 0)

                quality_score_system, quality_issues_system = self._check_quality_from_table(
                    cursor,
                    water_ctx,
                    ["chilled_flow", "chilled_return_temp", "chilled_supply_temp"],
                )
                quality_score_power, quality_issues_power = self._check_quality_from_table(
                    cursor,
                    ctx,
                    ["power"],
                    equipment_type="chiller",
                )

                total_records = overlapped_hours
                valid_records = active_hours

                calc_issues: List[dict[str, Any]] = []
                if low_power_hours > 0:
                    calc_issues.append({
                        "type": "low_power_filtered",
                        "description": (
                            f"已过滤 {low_power_hours} 个低功率小时（<= {min_power_kw} kW）以避免COP失真"
                        ),
                        "count": low_power_hours,
                        "details": {
                            "min_power_threshold_kw": min_power_kw,
                            "overlapped_hours": overlapped_hours,
                            "active_hours": active_hours,
                        },
                    })
                if non_positive_delta_t_hours > 0:
                    calc_issues.append({
                        "type": "non_positive_delta_t",
                        "description": f"存在 {non_positive_delta_t_hours} 个小时温差<=0，已按真实值参与口径",
                        "count": non_positive_delta_t_hours,
                    })

                expected_hours = max(1, int(ceil((ctx.time_end - ctx.time_start).total_seconds() / 3600)))
                calc_issues.append({
                    "type": "minimum_calculable_principle",
                    "description": f"基于 {overlapped_hours}/{expected_hours} 小时交集计算（各组件按 bucket_time 对齐）",
                    "details": {
                        "intersection_hours": overlapped_hours,
                        "expected_hours": expected_hours,
                        "overlapped_hours": overlapped_hours,
                        "components": ["chilled_flow", "chilled_return_temp", "chilled_supply_temp", "power"],
                        "join_key": "bucket_time",
                    },
                })

                if abs(chiller_power_sum) < 1e-9:
                    calc_issues.append({
                        "type": "zero_denominator",
                        "description": "有效时段冷机输入功率总和为0，无法计算COP",
                        "details": {
                            "avg_chiller_power": round(power_val, 4),
                            "power_sum_kw": round(chiller_power_sum, 4),
                            "min_power_threshold_kw": min_power_kw,
                        },
                    })
                    all_issues = _merge_issues(
                        quality_issues_system, quality_issues_power, calc_issues)
                    return CalculationResult(
                        metric_name=self.metric_name,
                        value=None,
                        unit=self.unit,
                        status="partial",
                        formula=self.formula,
                        formula_with_values=(
                            f"= {round(cooling_capacity_sum, 2)} / 0 (有效时段冷机输入功率总和为0)"
                        ),
                        sql_executed=sql.strip(),
                        input_records=total_records,
                        valid_records=valid_records,
                        data_source_condition=(
                            "metric_name IN ('chilled_flow','chilled_return_temp',"
                            "'chilled_supply_temp') + metric_name='power', equipment_type='chiller', "
                            f"{power_scope_text}"
                        ),
                        quality_score=round((quality_score_system + quality_score_power) / 2, 2),
                        quality_issues=all_issues,
                    )

                cop = round(cooling_capacity_sum / chiller_power_sum, 2)
                if power_val < 0 or chiller_power_sum < 0:
                    calc_issues.append({
                        "type": "negative_denominator",
                        "description": "冷机输入功率为负值，已按真实数据计算COP并告警",
                        "details": {
                            "avg_chiller_power": round(power_val, 2),
                            "power_sum_kw": round(chiller_power_sum, 2),
                        },
                    })
                if cooling_capacity_sum < 0:
                    calc_issues.append({
                        "type": "negative_numerator",
                        "description": "制冷量为负值，已按真实数据计算COP并告警",
                        "details": {"cooling_capacity_sum_kw": round(cooling_capacity_sum, 2)},
                    })
                if cop < 1.0 or cop > 12.0:
                    calc_issues.append({
                        "type": "cop_out_of_range",
                        "description": "COP超出常见运行区间（1-12），请核查功率口径与原始点位",
                        "details": {
                            "cop": cop,
                            "power_sum_kw": round(chiller_power_sum, 2),
                            "cooling_capacity_sum_kw": round(cooling_capacity_sum, 2),
                        },
                    })

                all_issues = _merge_issues(
                    quality_issues_system, quality_issues_power, calc_issues)

                return CalculationResult(
                    metric_name=self.metric_name,
                    value=cop,
                    unit=self.unit,
                    status=self._status_from_issues(all_issues),
                    formula=self.formula,
                    formula_with_values=(
                        f"= Σ[流量×(回水-供水)×{round(COP_CAPACITY_FACTOR, 3)}] / Σ[主功率] "
                        f"= {round(cooling_capacity_sum, 2)} / {round(chiller_power_sum, 2)} = {cop}"
                    ),
                    sql_executed=sql.strip(),
                    input_records=total_records,
                    valid_records=valid_records,
                    data_source_condition=(
                        "metric_name IN ('chilled_flow','chilled_return_temp',"
                        "'chilled_supply_temp') + metric_name='power', equipment_type='chiller', "
                        f"{power_scope_text}"
                    ),
                    quality_score=round((quality_score_system + quality_score_power) / 2, 2),
                    quality_issues=all_issues,
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


class SystemCopMetric(BaseMetric):
    """制冷系统COP: 制冷量 / 系统总功率，按小时交集。"""
    COMPONENT_TYPES = ["chiller", "chilled_pump", "cooling_pump",
                       "cooling_tower", "cooling_tower_closed", "tower_fan"]
    MIN_SYSTEM_POWER_KW = 20.0
    @property
    def metric_name(self) -> str:
        return "制冷系统COP"
    @property
    def unit(self) -> str:
        return ""
    @property
    def formula(self) -> str:
        return (
            "制冷系统COP = Σ制冷量(kW) / Σ系统总功率(kW)，"
            "制冷量 = 流量×温差×4.186/3.6，仅计入功率>20kW且温差>0的小时"
        )
    def calculate(self, ctx: MetricContext) -> CalculationResult:
        water_ctx = MetricContext(
            time_start=ctx.time_start, time_end=ctx.time_end,
            building_id=ctx.building_id, system_id=ctx.system_id,
        )
        where_flow, params_flow = self._build_where(water_ctx, "chilled_flow")
        where_ret, params_ret = self._build_where(water_ctx, "chilled_return_temp")
        where_sup, params_sup = self._build_where(water_ctx, "chilled_supply_temp")
        power_conds: List[str] = [
            "metric_name = %s", "bucket_time >= %s", "bucket_time < %s",
        ]
        power_params: List[Any] = ["power", ctx.time_start, ctx.time_end]
        placeholders = ", ".join(["%s"] * len(self.COMPONENT_TYPES))
        power_conds.append(f"equipment_type IN ({placeholders})")
        power_params.extend(self.COMPONENT_TYPES)
        if ctx.building_id:
            power_conds.append("building_id = %s")
            power_params.append(ctx.building_id)
        if ctx.system_id:
            power_conds.append("system_id = %s")
            power_params.append(ctx.system_id)
        where_power = " AND ".join(power_conds)
        min_pw = self.MIN_SYSTEM_POWER_KW
        sql = f"""
            WITH flow_hour AS (
                SELECT bucket_time, AVG(agg_avg) AS flow_avg
                FROM agg_hour WHERE {where_flow}
                GROUP BY bucket_time
            ),
            ret_hour AS (
                SELECT bucket_time, AVG(agg_avg) AS ret_avg
                FROM agg_hour WHERE {where_ret}
                GROUP BY bucket_time
            ),
            sup_hour AS (
                SELECT bucket_time, AVG(agg_avg) AS sup_avg
                FROM agg_hour WHERE {where_sup}
                GROUP BY bucket_time
            ),
            power_hour AS (
                SELECT bucket_time, SUM(agg_avg) AS power_sum_kw
                FROM agg_hour WHERE {where_power}
                GROUP BY bucket_time
            )
            SELECT
                COUNT(*) AS overlapped_hours,
                SUM(CASE WHEN ph.power_sum_kw > %s AND (rh.ret_avg - sh.sup_avg) > 0
                    THEN 1 ELSE 0 END) AS active_hours,
                SUM(CASE WHEN ph.power_sum_kw > %s AND (rh.ret_avg - sh.sup_avg) > 0
                    THEN fh.flow_avg * (rh.ret_avg - sh.sup_avg) * %s
                    ELSE 0 END) AS cooling_sum,
                SUM(CASE WHEN ph.power_sum_kw > %s AND (rh.ret_avg - sh.sup_avg) > 0
                    THEN ph.power_sum_kw ELSE 0 END) AS power_sum,
                SUM(CASE WHEN ph.power_sum_kw <= %s THEN 1 ELSE 0 END) AS low_power_hours,
                SUM(CASE WHEN (rh.ret_avg - sh.sup_avg) <= 0 THEN 1 ELSE 0 END) AS neg_dt_hours
            FROM flow_hour fh
            JOIN ret_hour rh ON rh.bucket_time = fh.bucket_time
            JOIN sup_hour sh ON sh.bucket_time = fh.bucket_time
            JOIN power_hour ph ON ph.bucket_time = fh.bucket_time
        """
        all_params = (params_flow + params_ret + params_sup + power_params
                      + [min_pw, min_pw, COP_CAPACITY_FACTOR, min_pw, min_pw])
        try:
            with self.db.cursor() as cursor:
                cursor.execute(sql, all_params)
                row = cursor.fetchone()
                if not row or int(row["overlapped_hours"] or 0) == 0:
                    missing_issues = self._build_missing_dependency_issues(
                        cursor, ctx,
                        ["chilled_flow", "chilled_return_temp",
                         "chilled_supply_temp", "power"],
                    )
                    return CalculationResult(
                        metric_name=self.metric_name, value=None,
                        unit=self.unit, status="no_data",
                        formula=self.formula, quality_score=0.0,
                        quality_issues=missing_issues,
                    )
                cooling_sum = float(row["cooling_sum"] or 0)
                power_sum = float(row["power_sum"] or 0)
                overlapped = int(row["overlapped_hours"] or 0)
                active = int(row["active_hours"] or 0)
                low_pw = int(row["low_power_hours"] or 0)
                neg_dt = int(row["neg_dt_hours"] or 0)
                expected = max(1, int(ceil(
                    (ctx.time_end - ctx.time_start).total_seconds() / 3600)))
                calc_issues: List[dict] = [{
                    "type": "minimum_calculable_principle",
                    "description": f"基于 {overlapped}/{expected} 小时交集计算，有效 {active} 小时",
                    "details": {
                        "intersection_hours": overlapped,
                        "expected_hours": expected,
                        "active_hours": active,
                        "low_power_hours": low_pw,
                        "non_positive_delta_t_hours": neg_dt,
                        "min_power_threshold_kw": min_pw,
                        "components": [
                            "chilled_flow", "chilled_return_temp",
                            "chilled_supply_temp", "power(system)"],
                    },
                }]
                if power_sum < 1e-9:
                    calc_issues.append({
                        "type": "zero_denominator",
                        "description": "有效时段系统总功率为0，无法计算系统COP",
                    })
                    return CalculationResult(
                        metric_name=self.metric_name, value=None,
                        unit=self.unit, status="partial",
                        formula=self.formula,
                        quality_issues=calc_issues,
                    )
                cop = round(cooling_sum / power_sum, 2)
                if cop < 0.5 or cop > 12.0:
                    calc_issues.append({
                        "type": "cop_out_of_range",
                        "description": "系统COP超出常见区间(0.5-12)，请核查",
                        "details": {"cop": cop,
                                    "power_sum_kw": round(power_sum, 2),
                                    "cooling_sum_kw": round(cooling_sum, 2)},
                    })
                return CalculationResult(
                    metric_name=self.metric_name, value=cop,
                    unit=self.unit,
                    status=self._status_from_issues(calc_issues),
                    formula=self.formula,
                    formula_with_values=(
                        f"= {round(cooling_sum, 2)} / {round(power_sum, 2)}"
                        f" = {cop}"
                    ),
                    sql_executed=sql.strip(),
                    input_records=overlapped,
                    valid_records=active,
                    quality_issues=calc_issues,
                )
        except Exception as e:
            return CalculationResult(
                metric_name=self.metric_name, value=None, unit=self.unit,
                status="failed", formula=self.formula,
                quality_issues=[{"type": "error", "description": str(e)}],
            )
