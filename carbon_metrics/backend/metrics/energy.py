"""能耗类指标计算"""
from math import ceil
from typing import Dict, List, Any, Optional, Tuple

from .base import BaseMetric, MetricContext, CalculationResult


# 设备类型分组映射
CHILLER_TYPES = {"chiller"}
PUMP_TYPES = {
    "chilled_pump",
    "cooling_pump",
    "closed_tower_pump",
    "user_side_pump",
    "source_side_pump",
    "heat_recovery_primary_pump",
    "heat_recovery_secondary_pump",
}
TOWER_TYPES = {"cooling_tower", "cooling_tower_closed", "tower_fan"}

COMPONENT_RULES = [
    {
        "key": "chiller",
        "label": "Chiller Energy",
        "types": CHILLER_TYPES,
    },
    {
        "key": "chilled_pump",
        "label": "Chilled Pump Energy",
        "types": {"chilled_pump"},
    },
    {
        "key": "cooling_pump",
        "label": "Cooling Pump Energy",
        "types": {"cooling_pump"},
    },
    {
        "key": "tower",
        "label": "Tower Energy",
        "types": TOWER_TYPES,
    },
]
COMPONENT_LABELS = {rule["key"]: rule["label"] for rule in COMPONENT_RULES}


def _component_key_for_type(equipment_type: str) -> Optional[str]:
    for rule in COMPONENT_RULES:
        if equipment_type in rule["types"]:
            return rule["key"]
    return None


def _query_energy_by_type(
    db,
    ctx: MetricContext,
    clamp_threshold: float,
    query_cache: Optional[Dict] = None,
) -> Tuple[Optional[List], Optional[str], Optional[str]]:
    """按 equipment_type 分组查询能耗数据，返回 (rows, sql, error)。"""
    conditions = [
        "metric_name = 'energy'",
        "bucket_time >= %s",
        "bucket_time < %s",
    ]
    params: List[Any] = [ctx.time_start, ctx.time_end]

    if ctx.building_id:
        conditions.append("building_id = %s")
        params.append(ctx.building_id)
    if ctx.system_id:
        conditions.append("system_id = %s")
        params.append(ctx.system_id)
    if ctx.equipment_type:
        conditions.append("equipment_type = %s")
        params.append(ctx.equipment_type)
    if ctx.equipment_id:
        conditions.append("equipment_id = %s")
        params.append(ctx.equipment_id)
    BaseMetric._append_sub_equipment_condition(
        conditions,
        params,
        ctx.sub_equipment_id,
    )

    where_clause = " AND ".join(conditions)
    sql = f"""
        SELECT
            equipment_type,
            SUM(
                CASE
                    WHEN agg_delta < 0 THEN 0
                    ELSE agg_delta
                END
            ) AS total_energy,
            SUM(
                CASE
                    WHEN agg_delta < 0 AND agg_delta >= -%s THEN agg_delta
                    ELSE 0
                END
            ) AS clamped_negative_total,
            SUM(
                CASE
                    WHEN agg_delta < 0 AND agg_delta >= -%s THEN 1
                    ELSE 0
                END
            ) AS clamped_negative_count,
            SUM(
                CASE
                    WHEN agg_delta < -%s THEN agg_delta
                    ELSE 0
                END
            ) AS severe_negative_total,
            SUM(
                CASE
                    WHEN agg_delta < -%s THEN 1
                    ELSE 0
                END
            ) AS severe_negative_count,
            COUNT(*) AS record_count
        FROM agg_hour
        WHERE {where_clause}
        GROUP BY equipment_type
    """

    try:
        query_params: List[Any] = [
            clamp_threshold,
            clamp_threshold,
            clamp_threshold,
            clamp_threshold,
            *params,
        ]
        cache_key = ("energy_by_type", sql.strip(), tuple(str(p) for p in query_params))
        if query_cache is not None and cache_key in query_cache:
            rows = query_cache[cache_key]
            return rows, sql.strip(), None

        with db.cursor() as cursor:
            cursor.execute(sql, query_params)
            rows = cursor.fetchall()

        if query_cache is not None:
            query_cache[cache_key] = rows
        return rows, sql.strip(), None
    except Exception as e:
        return None, sql.strip(), str(e)


def _aggregate_energy(
    rows,
) -> Tuple[
    Dict[str, float],
    int,
    List[Dict[str, Any]],
    Dict[str, float],
    Dict[str, int],
    List[str],
    Dict[str, Any],
]:
    """Aggregate energy data."""
    type_map: Dict[str, float] = {}
    total_records = 0
    breakdown = []
    component_totals = {rule["key"]: 0.0 for rule in COMPONENT_RULES}
    component_record_counts = {rule["key"]: 0 for rule in COMPONENT_RULES}
    extra_types: List[str] = []
    clamped_negative_count = 0
    clamped_negative_total = 0.0
    severe_negative_count = 0
    severe_negative_total = 0.0
    severe_by_type: List[Dict[str, Any]] = []

    for row in rows:
        eq_type = row["equipment_type"]
        energy = float(row["total_energy"] or 0)
        records = int(row["record_count"] or 0)
        clamped_count = int(row.get("clamped_negative_count") or 0)
        clamped_total = float(row.get("clamped_negative_total") or 0.0)
        severe_count = int(row.get("severe_negative_count") or 0)
        severe_total = float(row.get("severe_negative_total") or 0.0)

        type_map[eq_type] = energy
        total_records += records
        breakdown.append({
            "equipment_type": eq_type,
            "equipment_id": None,
            "value": round(energy, 2),
        })

        component_key = _component_key_for_type(eq_type)
        if component_key:
            component_totals[component_key] += energy
            component_record_counts[component_key] += records
        else:
            extra_types.append(eq_type)

        clamped_negative_count += clamped_count
        clamped_negative_total += clamped_total
        severe_negative_count += severe_count
        severe_negative_total += severe_total
        if severe_count > 0:
            severe_by_type.append({
                "equipment_type": eq_type,
                "count": severe_count,
                "total": round(severe_total, 2),
            })

    severe_by_type = sorted(
        severe_by_type,
        key=lambda item: abs(float(item["total"])),
        reverse=True,
    )
    negative_summary = {
        "clamped_negative_count": clamped_negative_count,
        "clamped_negative_total": round(clamped_negative_total, 2),
        "severe_negative_count": severe_negative_count,
        "severe_negative_total": round(severe_negative_total, 2),
        "severe_negative_by_type": severe_by_type[:10],
    }

    return (
        type_map,
        total_records,
        breakdown,
        component_totals,
        component_record_counts,
        sorted(set(extra_types)),
        negative_summary,
    )


def _build_negative_delta_issues(
    negative_summary: Dict[str, Any],
    clamp_threshold: float,
) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    clamped_count = int(negative_summary.get("clamped_negative_count") or 0)
    clamped_total = float(negative_summary.get("clamped_negative_total") or 0.0)
    severe_count = int(negative_summary.get("severe_negative_count") or 0)
    severe_total = float(negative_summary.get("severe_negative_total") or 0.0)
    severe_by_type = negative_summary.get("severe_negative_by_type") or []
    filtered_count = clamped_count + severe_count
    filtered_total = clamped_total + severe_total

    if clamped_count > 0:
        issues.append({
            "type": "negative_delta_clamped",
            "description": (
                f"Detected {clamped_count} small negative deltas "
                f"(-{clamp_threshold} <= agg_delta < 0); excluded from SUM."
            ),
            "count": clamped_count,
            "details": {
                "clamp_threshold": clamp_threshold,
                "clamped_negative_total": round(clamped_total, 2),
                "policy": "-threshold <= agg_delta < 0 -> excluded_from_sum",
            },
        })

    if severe_count > 0:
        issues.append({
            "type": "negative_delta_alert",
            "description": (
                f"Detected {severe_count} severe negative deltas "
                f"(agg_delta < -{clamp_threshold}); excluded from SUM and flagged."
            ),
            "count": severe_count,
            "details": {
                "clamp_threshold": clamp_threshold,
                "severe_negative_total": round(severe_total, 2),
                "severe_negative_by_type": severe_by_type,
                "policy": "agg_delta < -threshold -> excluded_from_sum_and_alert",
            },
        })

    if filtered_count > 0:
        issues.append({
            "type": "result_beautified",
            "description": (
                "This metric uses cleaned SUM scope: all negative deltas are excluded."
            ),
            "count": filtered_count,
            "details": {
                "clamp_threshold": clamp_threshold,
                "filtered_negative_count": filtered_count,
                "filtered_negative_total": round(filtered_total, 2),
                "severe_negative_by_type": severe_by_type,
                "policy": "agg_delta < 0 -> excluded_from_sum",
            },
        })

    return issues

class TotalEnergyMetric(BaseMetric):
    """Total energy metric."""
    @property
    def metric_name(self) -> str:
        return "系统总电量"

    @property
    def unit(self) -> str:
        return "kWh"

    @property
    def formula(self) -> str:
        return "系统总电量 = 冷机电量 + 冷冻泵电量 + 冷却泵电量 + 冷却塔电量"

    def _query_component_bucket_coverage(
        self,
        cursor,
        ctx: MetricContext,
    ) -> Tuple[int, Dict[str, int], int, List[str], int]:
        """Query component coverage by hour."""
        conditions, params = self._build_scope_conditions(ctx)
        conditions.append("metric_name = %s")
        params.append("energy")
        where_clause = " AND ".join(conditions)

        sql = f"""
            SELECT bucket_time, equipment_type
            FROM agg_hour
            WHERE {where_clause}
            GROUP BY bucket_time, equipment_type
        """
        cursor.execute(sql, params)
        rows = cursor.fetchall()

        expected_hours = max(
            1,
            int(ceil((ctx.time_end - ctx.time_start).total_seconds() / 3600)),
        )

        per_bucket_components: Dict[str, set] = {}
        for row in rows:
            bucket_key = str(row["bucket_time"])
            comp_key = _component_key_for_type(row["equipment_type"])
            if not comp_key:
                continue
            if bucket_key not in per_bucket_components:
                per_bucket_components[bucket_key] = set()
            per_bucket_components[bucket_key].add(comp_key)

        all_required = set(COMPONENT_LABELS.keys())
        component_covered_hours = {k: 0 for k in COMPONENT_LABELS}
        missing_sample_buckets: List[str] = []
        complete_bucket_count = 0

        for bucket_key, comp_set in per_bucket_components.items():
            for key in comp_set:
                component_covered_hours[key] += 1
            if all_required.issubset(comp_set):
                complete_bucket_count += 1
            elif len(missing_sample_buckets) < 20:
                missing_sample_buckets.append(bucket_key)

        represented_hours = len(per_bucket_components)
        missing_hours_in_represented = represented_hours - complete_bucket_count
        missing_hours_outside = max(0, expected_hours - represented_hours)
        total_missing_hours = missing_hours_in_represented + missing_hours_outside

        return (
            expected_hours,
            component_covered_hours,
            total_missing_hours,
            missing_sample_buckets,
            missing_hours_outside,
        )

    def calculate(self, ctx: MetricContext) -> CalculationResult:
        clamp_threshold = self._negative_delta_clamp_threshold
        rows, sql, error = _query_energy_by_type(
            self.db,
            ctx,
            clamp_threshold=clamp_threshold,
            query_cache=self._query_cache,
        )

        if error:
            return CalculationResult(
                metric_name=self.metric_name, value=None, unit=self.unit,
                status="failed", formula=self.formula,
                quality_issues=[{"type": "error", "description": error}],
            )

        with self.db.cursor() as cursor:
            if not rows:
                missing_issues = self._build_missing_dependency_issues(
                    cursor, ctx, ["energy"])
                return CalculationResult(
                    metric_name=self.metric_name,
                    value=None,
                    unit=self.unit,
                    status="no_data",
                    formula=self.formula,
                    quality_score=0.0,
                    quality_issues=missing_issues,
                )

            (
                type_map,
                total_records,
                breakdown,
                component_totals,
                component_record_counts,
                extra_types,
                negative_summary,
            ) = _aggregate_energy(rows)

            quality_score, quality_issues = self._check_quality_from_table(
                cursor, ctx, ["energy"])

            component_issues: List[Dict[str, Any]] = []
            formula_with_values = ""
            total = 0.0

            # If equipment_type is not explicitly filtered, use fixed formula components.
            if not ctx.equipment_type:
                component_values = [
                    ("chiller", round(component_totals["chiller"], 2)),
                    ("chilled_pump", round(component_totals["chilled_pump"], 2)),
                    ("cooling_pump", round(component_totals["cooling_pump"], 2)),
                    ("tower", round(component_totals["tower"], 2)),
                ]
                total = round(sum(v for _, v in component_values), 2)
                values_str = " + ".join([f"{v}" for _, v in component_values])
                formula_with_values = f"= {values_str} = {total}"

                missing_components = [
                    COMPONENT_LABELS[key]
                    for key, cnt in component_record_counts.items()
                    if cnt == 0
                ]
                if missing_components:
                    component_issues.append({
                        "type": "missing_component",
                        "description": f"总电量口径缺少组件 {', '.join(missing_components)}",
                        "count": len(missing_components),
                        "details": {
                            "missing_components": missing_components,
                            "component_totals": {
                                COMPONENT_LABELS[k]: round(v, 2)
                                for k, v in component_totals.items()
                            },
                        },
                    })

                (
                    expected_hours,
                    covered_hours,
                    missing_hours,
                    missing_bucket_samples,
                    missing_hours_outside,
                ) = self._query_component_bucket_coverage(cursor, ctx)

                if missing_hours > 0:
                    component_issues.append({
                        "type": "missing_time_bucket",
                        "description": (
                            f"Detected {missing_hours}/{expected_hours} hours with missing component coverage."
                        ),
                        "count": missing_hours,
                        "details": {
                            "expected_hours": expected_hours,
                            "covered_hours_by_component": {
                                COMPONENT_LABELS[k]: covered_hours[k]
                                for k in covered_hours
                            },
                            "missing_bucket_samples": missing_bucket_samples,
                            "missing_hours_without_any_component_record": missing_hours_outside,
                        },
                    })

                if extra_types:
                    component_issues.append({
                        "type": "scope_notice",
                        "description": "存在未纳入总电量公式的设备类型",
                        "details": {"excluded_equipment_types": extra_types},
                    })
            else:
                total = round(sum(type_map.values()), 2)
                values_str = " + ".join([f"{b['value']}" for b in breakdown])
                formula_with_values = f"= {values_str} = {total}"

            component_issues.extend(_build_negative_delta_issues(
                negative_summary=negative_summary,
                clamp_threshold=clamp_threshold,
            ))

            all_issues = quality_issues + component_issues
            status = "partial" if all_issues else "success"

            return CalculationResult(
                metric_name=self.metric_name,
                value=total,
                unit=self.unit,
                status=status,
                formula=self.formula,
                formula_with_values=formula_with_values,
                sql_executed=sql,
                input_records=total_records,
                valid_records=total_records,
                data_source_field="agg_delta",
                data_source_condition="metric_name='energy'",
                quality_score=quality_score,
                quality_issues=all_issues,
                breakdown=breakdown,
            )


class _EnergyRatioBase(BaseMetric):
    """Energy ratio metric base class."""
    @property
    def unit(self) -> str:
        return "%"

    @property
    def _target_types(self) -> set:
        raise NotImplementedError

    @property
    def _target_label(self) -> str:
        raise NotImplementedError

    def calculate(self, ctx: MetricContext) -> CalculationResult:
        clamp_threshold = self._negative_delta_clamp_threshold
        rows, sql, error = _query_energy_by_type(
            self.db,
            ctx,
            clamp_threshold=clamp_threshold,
            query_cache=self._query_cache,
        )

        if error:
            return CalculationResult(
                metric_name=self.metric_name, value=None, unit=self.unit,
                status="failed", formula=self.formula,
                quality_issues=[{"type": "error", "description": error}],
            )
        with self.db.cursor() as cursor:
            if not rows:
                missing_issues = self._build_missing_dependency_issues(
                    cursor, ctx, ["energy"])
                return CalculationResult(
                    metric_name=self.metric_name,
                    value=None,
                    unit=self.unit,
                    status="no_data",
                    formula=self.formula,
                    quality_score=0.0,
                    quality_issues=missing_issues,
                )

            (
                type_map,
                total_records,
                breakdown,
                component_totals,
                component_record_counts,
                extra_types,
                negative_summary,
            ) = _aggregate_energy(rows)

            total = round(sum(component_totals.values()), 2) if not ctx.equipment_type else round(sum(type_map.values()), 2)
            part = round(sum(v for k, v in type_map.items() if k in self._target_types), 2)

            quality_score, quality_issues = self._check_quality_from_table(
                cursor, ctx, ["energy"])

            calc_issues: List[Dict[str, Any]] = []
            if not ctx.equipment_type:
                missing_components = [
                    COMPONENT_LABELS[key]
                    for key, cnt in component_record_counts.items()
                    if cnt == 0
                ]
                if missing_components:
                    calc_issues.append({
                        "type": "missing_component",
                        "description": f"{self.metric_name}分母口径缺少组件: {', '.join(missing_components)}",
                        "count": len(missing_components),
                        "details": {"missing_components": missing_components},
                    })
                if extra_types:
                    calc_issues.append({
                        "type": "scope_notice",
                        "description": "存在未纳入分母口径的设备类型",
                        "details": {"excluded_equipment_types": extra_types},
                    })

            calc_issues.extend(_build_negative_delta_issues(
                negative_summary=negative_summary,
                clamp_threshold=clamp_threshold,
            ))

            if total == 0:
                all_issues = quality_issues + calc_issues + [{
                    "type": "zero_denominator",
                    "description": "Total energy is 0, ratio cannot be calculated.",
                }]
                return CalculationResult(
                    metric_name=self.metric_name,
                    value=None,
                    unit=self.unit,
                    status="partial",
                    formula=self.formula,
                    formula_with_values="= 0 / 0 (鎬荤數閲忎负0)",
                    sql_executed=sql,
                    input_records=total_records,
                    valid_records=total_records,
                    data_source_field="agg_delta",
                    data_source_condition="metric_name='energy'",
                    quality_score=quality_score,
                    quality_issues=all_issues,
                    breakdown=breakdown,
                )

            ratio = round(part / total * 100, 2)
            formula_with_values = f"= {part} / {total} x 100% = {ratio}%"
            all_issues = quality_issues + calc_issues
            status = "partial" if all_issues else "success"

            return CalculationResult(
                metric_name=self.metric_name,
                value=ratio,
                unit=self.unit,
                status=status,
                formula=self.formula,
                formula_with_values=formula_with_values,
                sql_executed=sql,
                input_records=total_records,
                valid_records=total_records,
                data_source_field="agg_delta",
                data_source_condition="metric_name='energy'",
                quality_score=quality_score,
                quality_issues=all_issues,
                breakdown=breakdown,
            )


class ChillerEnergyRatioMetric(_EnergyRatioBase):
    """Chiller energy ratio metric."""
    @property
    def metric_name(self) -> str:
        return "冷机能耗占比"

    @property
    def formula(self) -> str:
        return "冷机能耗占比 = 冷机电量 / 系统总电量 × 100%"

    @property
    def _target_types(self) -> set:
        return CHILLER_TYPES

    @property
    def _target_label(self) -> str:
        return "Chiller"


class PumpEnergyRatioMetric(_EnergyRatioBase):
    """Pump energy ratio metric."""
    @property
    def metric_name(self) -> str:
        return "水泵能耗占比"

    @property
    def formula(self) -> str:
        return "水泵能耗占比 = (冷冻泵+冷却泵)电量 / 系统总电量 × 100%"

    @property
    def _target_types(self) -> set:
        return PUMP_TYPES

    @property
    def _target_label(self) -> str:
        return "Pump"


class TowerEnergyRatioMetric(_EnergyRatioBase):
    """Tower energy ratio metric."""
    @property
    def metric_name(self) -> str:
        return "风机能耗占比"

    @property
    def formula(self) -> str:
        return "风机能耗占比 = 冷却塔电量 / 系统总电量 × 100%"

    @property
    def _target_types(self) -> set:
        return TOWER_TYPES

    @property
    def _target_label(self) -> str:
        return "Tower"






