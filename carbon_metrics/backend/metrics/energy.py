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


def _query_energy_by_bucket_type(
    db,
    ctx: MetricContext,
    clamp_threshold: float,
    query_cache: Optional[Dict] = None,
) -> Tuple[Optional[List], Optional[str], Optional[str]]:
    """Query energy by hour and equipment_type for strict intersection scope."""
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
            bucket_time,
            equipment_type,
            SUM(CASE WHEN agg_delta < 0 THEN 0 ELSE agg_delta END) AS total_energy,
            SUM(CASE WHEN agg_delta < 0 AND agg_delta >= -%s THEN agg_delta ELSE 0 END) AS clamped_negative_total,
            SUM(CASE WHEN agg_delta < 0 AND agg_delta >= -%s THEN 1 ELSE 0 END) AS clamped_negative_count,
            SUM(CASE WHEN agg_delta < -%s THEN agg_delta ELSE 0 END) AS severe_negative_total,
            SUM(CASE WHEN agg_delta < -%s THEN 1 ELSE 0 END) AS severe_negative_count,
            COUNT(*) AS record_count
        FROM agg_hour
        WHERE {where_clause}
        GROUP BY bucket_time, equipment_type
    """

    try:
        query_params: List[Any] = [
            clamp_threshold,
            clamp_threshold,
            clamp_threshold,
            clamp_threshold,
            *params,
        ]
        cache_key = ("energy_by_bucket", sql.strip(), tuple(str(p) for p in query_params))
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


def _format_bucket(bucket_time: Any) -> str:
    if hasattr(bucket_time, "strftime"):
        return bucket_time.strftime("%Y-%m-%d %H:%M:%S")
    return str(bucket_time)


def _build_minimum_calculable_summary(rows: List[Dict[str, Any]], ctx: MetricContext) -> Dict[str, Any]:
    required_components = set(COMPONENT_LABELS.keys())
    expected_hours = max(
        1,
        int(ceil((ctx.time_end - ctx.time_start).total_seconds() / 3600)),
    )

    per_bucket_components: Dict[Any, set] = {}
    extra_types: set = set()
    for row in rows:
        eq_type = str(row.get("equipment_type") or "")
        comp = _component_key_for_type(eq_type)
        if not comp:
            if eq_type:
                extra_types.add(eq_type)
            continue
        bucket = row.get("bucket_time")
        per_bucket_components.setdefault(bucket, set()).add(comp)

    component_covered_hours = {k: 0 for k in COMPONENT_LABELS}
    missing_bucket_samples: List[str] = []
    intersection_buckets: set = set()

    for bucket, comp_set in per_bucket_components.items():
        for comp in comp_set:
            component_covered_hours[comp] += 1
        if required_components.issubset(comp_set):
            intersection_buckets.add(bucket)
        elif len(missing_bucket_samples) < 20:
            missing_bucket_samples.append(_format_bucket(bucket))

    represented_hours = len(per_bucket_components)
    intersection_hours = len(intersection_buckets)
    missing_hours_in_represented = represented_hours - intersection_hours
    missing_hours_outside = max(0, expected_hours - represented_hours)
    total_missing_hours = missing_hours_in_represented + missing_hours_outside

    type_map: Dict[str, float] = {}
    total_records = 0
    clamped_negative_count = 0
    clamped_negative_total = 0.0
    severe_negative_count = 0
    severe_negative_total = 0.0
    severe_by_type: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        bucket = row.get("bucket_time")
        if bucket not in intersection_buckets:
            continue
        eq_type = str(row.get("equipment_type") or "")
        comp = _component_key_for_type(eq_type)
        if not comp:
            continue

        energy = float(row.get("total_energy") or 0.0)
        type_map[eq_type] = type_map.get(eq_type, 0.0) + energy
        total_records += int(row.get("record_count") or 0)

        clamped_count = int(row.get("clamped_negative_count") or 0)
        clamped_total = float(row.get("clamped_negative_total") or 0.0)
        severe_count = int(row.get("severe_negative_count") or 0)
        severe_total = float(row.get("severe_negative_total") or 0.0)

        clamped_negative_count += clamped_count
        clamped_negative_total += clamped_total
        severe_negative_count += severe_count
        severe_negative_total += severe_total

        if severe_count > 0:
            item = severe_by_type.setdefault(
                eq_type,
                {"equipment_type": eq_type, "count": 0, "total": 0.0},
            )
            item["count"] += severe_count
            item["total"] += severe_total

    component_totals = {k: 0.0 for k in COMPONENT_LABELS}
    for eq_type, energy in type_map.items():
        comp = _component_key_for_type(eq_type)
        if comp:
            component_totals[comp] += energy

    breakdown = [
        {
            "equipment_type": eq_type,
            "equipment_id": None,
            "value": round(value, 2),
        }
        for eq_type, value in sorted(type_map.items())
    ]
    severe_by_type_rows = sorted(
        [
            {
                "equipment_type": key,
                "count": int(value["count"]),
                "total": round(float(value["total"]), 2),
            }
            for key, value in severe_by_type.items()
        ],
        key=lambda item: abs(float(item["total"])),
        reverse=True,
    )[:10]

    return {
        "expected_hours": expected_hours,
        "represented_hours": represented_hours,
        "intersection_hours": intersection_hours,
        "missing_hours": total_missing_hours,
        "missing_hours_without_any_component_record": missing_hours_outside,
        "missing_bucket_samples": missing_bucket_samples,
        "component_covered_hours": component_covered_hours,
        "component_totals": {k: round(v, 2) for k, v in component_totals.items()},
        "type_map": type_map,
        "breakdown": breakdown,
        "total_records": total_records,
        "extra_types": sorted(extra_types),
        "negative_summary": {
            "clamped_negative_count": clamped_negative_count,
            "clamped_negative_total": round(clamped_negative_total, 2),
            "severe_negative_count": severe_negative_count,
            "severe_negative_total": round(severe_negative_total, 2),
            "severe_negative_by_type": severe_by_type_rows,
        },
    }


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

            quality_score, quality_issues = self._check_quality_from_table(
                cursor, ctx, ["energy"])

            # Scoped query keeps by-type behavior.
            if ctx.equipment_type:
                (
                    type_map,
                    total_records,
                    breakdown,
                    _component_totals,
                    _component_record_counts,
                    _extra_types,
                    negative_summary,
                ) = _aggregate_energy(rows)
                total = round(sum(type_map.values()), 2)
                values_str = " + ".join([f"{b['value']}" for b in breakdown])
                formula_with_values = f"= {values_str} = {total}"

                calc_issues = _build_negative_delta_issues(
                    negative_summary=negative_summary,
                    clamp_threshold=clamp_threshold,
                )
                all_issues = quality_issues + calc_issues
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

            bucket_rows, bucket_sql, bucket_error = _query_energy_by_bucket_type(
                self.db,
                ctx,
                clamp_threshold=clamp_threshold,
                query_cache=self._query_cache,
            )
            if bucket_error:
                return CalculationResult(
                    metric_name=self.metric_name, value=None, unit=self.unit,
                    status="failed", formula=self.formula,
                    quality_issues=[{"type": "error", "description": bucket_error}],
                )

            summary = _build_minimum_calculable_summary(bucket_rows or [], ctx)
            component_totals = summary["component_totals"]
            total = round(sum(float(v) for v in component_totals.values()), 2)
            values_str = " + ".join([
                str(component_totals["chiller"]),
                str(component_totals["chilled_pump"]),
                str(component_totals["cooling_pump"]),
                str(component_totals["tower"]),
            ])
            formula_with_values = f"= {values_str} = {total}"

            component_issues: List[Dict[str, Any]] = []
            missing_components = [
                COMPONENT_LABELS[key]
                for key, hours in summary["component_covered_hours"].items()
                if int(hours) == 0
            ]
            if missing_components:
                component_issues.append({
                    "type": "missing_component",
                    "description": f"Total energy denominator is missing components: {', '.join(missing_components)}",
                    "count": len(missing_components),
                    "details": {
                        "missing_components": missing_components,
                        "component_totals": {
                            COMPONENT_LABELS[k]: component_totals[k]
                            for k in component_totals
                        },
                    },
                })

            if int(summary["missing_hours"]) > 0:
                component_issues.append({
                    "type": "missing_time_bucket",
                    "description": (
                        f"Detected {summary['missing_hours']}/{summary['expected_hours']} hours "
                        "with incomplete component coverage."
                    ),
                    "count": int(summary["missing_hours"]),
                    "details": {
                        "expected_hours": int(summary["expected_hours"]),
                        "represented_hours": int(summary["represented_hours"]),
                        "intersection_hours": int(summary["intersection_hours"]),
                        "covered_hours_by_component": {
                            COMPONENT_LABELS[k]: int(v)
                            for k, v in summary["component_covered_hours"].items()
                        },
                        "missing_bucket_samples": summary["missing_bucket_samples"],
                        "missing_hours_without_any_component_record": int(
                            summary["missing_hours_without_any_component_record"]
                        ),
                    },
                })

            if summary["extra_types"]:
                component_issues.append({
                    "type": "scope_notice",
                    "description": "Found equipment types excluded from total-energy formula.",
                    "details": {"excluded_equipment_types": summary["extra_types"]},
                })

            component_issues.append({
                "type": "minimum_calculable_principle",
                "description": "Result uses hourly intersection of required components only.",
                "details": {
                    "required_components": [COMPONENT_LABELS[k] for k in COMPONENT_LABELS],
                    "intersection_hours": int(summary["intersection_hours"]),
                    "expected_hours": int(summary["expected_hours"]),
                },
            })

            component_issues.extend(_build_negative_delta_issues(
                negative_summary=summary["negative_summary"],
                clamp_threshold=clamp_threshold,
            ))

            all_issues = quality_issues + component_issues
            if int(summary["intersection_hours"]) == 0:
                return CalculationResult(
                    metric_name=self.metric_name,
                    value=None,
                    unit=self.unit,
                    status="no_data",
                    formula=self.formula,
                    formula_with_values="No hourly intersection across required components.",
                    sql_executed=bucket_sql,
                    input_records=int(summary["total_records"]),
                    valid_records=0,
                    data_source_field="agg_delta",
                    data_source_condition=(
                        "metric_name='energy'; strict_hourly_intersection="
                        "{chiller,chilled_pump,cooling_pump,tower}"
                    ),
                    quality_score=quality_score,
                    quality_issues=all_issues,
                    breakdown=summary["breakdown"],
                )

            status = "partial" if all_issues else "success"
            return CalculationResult(
                metric_name=self.metric_name,
                value=total,
                unit=self.unit,
                status=status,
                formula=self.formula,
                formula_with_values=formula_with_values,
                sql_executed=bucket_sql,
                input_records=int(summary["total_records"]),
                valid_records=int(summary["total_records"]),
                data_source_field="agg_delta",
                data_source_condition=(
                    "metric_name='energy'; strict_hourly_intersection="
                    "{chiller,chilled_pump,cooling_pump,tower}"
                ),
                quality_score=quality_score,
                quality_issues=all_issues,
                breakdown=summary["breakdown"],
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

            quality_score, quality_issues = self._check_quality_from_table(
                cursor, ctx, ["energy"])

            # Scope query keeps the old behavior.
            if ctx.equipment_type:
                (
                    type_map,
                    total_records,
                    breakdown,
                    component_totals,
                    component_record_counts,
                    extra_types,
                    negative_summary,
                ) = _aggregate_energy(rows)

                total = round(sum(component_totals.values()), 2)
                part = round(sum(v for k, v in type_map.items() if k in self._target_types), 2)

                calc_issues: List[Dict[str, Any]] = []

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
                        formula_with_values="= 0 / 0 (total energy is 0)",
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

            bucket_rows, bucket_sql, bucket_error = _query_energy_by_bucket_type(
                self.db,
                ctx,
                clamp_threshold=clamp_threshold,
                query_cache=self._query_cache,
            )
            if bucket_error:
                return CalculationResult(
                    metric_name=self.metric_name, value=None, unit=self.unit,
                    status="failed", formula=self.formula,
                    quality_issues=[{"type": "error", "description": bucket_error}],
                )

            # ----------------------------------------------------------
            # 最小可计算原则（宽松交集）：
            # 只要求 target 组件在该小时有数据 且 该小时总能耗 > 0
            # ----------------------------------------------------------
            expected_hours = max(
                1,
                int(ceil((ctx.time_end - ctx.time_start).total_seconds() / 3600)),
            )
            target_types = self._target_types

            # 按小时分组，记录各 equipment_type 的能耗
            per_bucket: Dict[Any, Dict[str, float]] = {}
            per_bucket_records: Dict[Any, int] = {}
            neg_clamped_count = 0
            neg_clamped_total = 0.0
            neg_severe_count = 0
            neg_severe_total = 0.0
            neg_severe_by_type: Dict[str, Dict[str, Any]] = {}
            extra_types: set = set()

            for row in (bucket_rows or []):
                eq_type = str(row.get("equipment_type") or "")
                comp = _component_key_for_type(eq_type)
                if not comp:
                    if eq_type:
                        extra_types.add(eq_type)
                    continue
                bucket = row.get("bucket_time")
                energy = float(row.get("total_energy") or 0.0)
                per_bucket.setdefault(bucket, {})[eq_type] = (
                    per_bucket.get(bucket, {}).get(eq_type, 0.0) + energy
                )
                per_bucket_records[bucket] = (
                    per_bucket_records.get(bucket, 0)
                    + int(row.get("record_count") or 0)
                )
                c_count = int(row.get("clamped_negative_count") or 0)
                c_total = float(row.get("clamped_negative_total") or 0.0)
                s_count = int(row.get("severe_negative_count") or 0)
                s_total = float(row.get("severe_negative_total") or 0.0)
                neg_clamped_count += c_count
                neg_clamped_total += c_total
                neg_severe_count += s_count
                neg_severe_total += s_total
                if s_count > 0:
                    item = neg_severe_by_type.setdefault(
                        eq_type, {"equipment_type": eq_type, "count": 0, "total": 0.0},
                    )
                    item["count"] += s_count
                    item["total"] += s_total

            # 筛选交集小时：target 有数据 且 该小时总能耗 > 0
            intersection_buckets: set = set()
            component_covered_hours: Dict[str, int] = {k: 0 for k in COMPONENT_LABELS}
            for bucket, type_energies in per_bucket.items():
                for eq_type in type_energies:
                    comp = _component_key_for_type(eq_type)
                    if comp:
                        component_covered_hours[comp] += 1
                has_target = any(t in type_energies for t in target_types)
                bucket_total = sum(type_energies.values())
                if has_target and bucket_total > 0:
                    intersection_buckets.add(bucket)

            intersection_hours = len(intersection_buckets)
            represented_hours = len(per_bucket)

            # 在交集小时上计算 part / total
            type_map: Dict[str, float] = {}
            total_records = 0
            for bucket in intersection_buckets:
                total_records += per_bucket_records.get(bucket, 0)
                for eq_type, energy in per_bucket[bucket].items():
                    type_map[eq_type] = type_map.get(eq_type, 0.0) + energy

            part = round(sum(v for k, v in type_map.items() if k in target_types), 2)
            total = round(sum(type_map.values()), 2)

            breakdown = [
                {"equipment_type": eq_type, "equipment_id": None, "value": round(value, 2)}
                for eq_type, value in sorted(type_map.items())
            ]

            # quality issues
            calc_issues: List[Dict[str, Any]] = []
            missing_components = [
                COMPONENT_LABELS[key]
                for key, hours in component_covered_hours.items()
                if hours == 0
            ]
            if missing_components:
                calc_issues.append({
                    "type": "missing_component",
                    "description": f"{self.metric_name} 分母口径缺少组件: {', '.join(missing_components)}",
                    "count": len(missing_components),
                    "details": {"missing_components": missing_components},
                })
            if represented_hours > intersection_hours:
                calc_issues.append({
                    "type": "missing_time_bucket",
                    "description": (
                        f"有 {represented_hours - intersection_hours}/{represented_hours} 个小时"
                        f"因目标组件({self._target_label})缺数据或总能耗<=0被排除。"
                    ),
                    "details": {
                        "expected_hours": expected_hours,
                        "represented_hours": represented_hours,
                        "intersection_hours": intersection_hours,
                        "covered_hours_by_component": {
                            COMPONENT_LABELS[k]: v
                            for k, v in component_covered_hours.items()
                        },
                    },
                })
            if extra_types:
                calc_issues.append({
                    "type": "scope_notice",
                    "description": "存在未纳入分母口径的设备类型",
                    "details": {"excluded_equipment_types": sorted(extra_types)},
                })
            calc_issues.append({
                "type": "minimum_calculable_principle",
                "description": (
                    f"按最小可计算原则，仅使用目标组件({self._target_label})有数据"
                    f"且总能耗>0的小时计算。交集={intersection_hours}/{expected_hours}小时。"
                ),
                "details": {
                    "target_component": self._target_label,
                    "intersection_hours": intersection_hours,
                    "expected_hours": expected_hours,
                },
            })
            neg_summary = {
                "clamped_negative_count": neg_clamped_count,
                "clamped_negative_total": round(neg_clamped_total, 2),
                "severe_negative_count": neg_severe_count,
                "severe_negative_total": round(neg_severe_total, 2),
                "severe_negative_by_type": sorted(
                    [
                        {"equipment_type": k, "count": int(v["count"]), "total": round(float(v["total"]), 2)}
                        for k, v in neg_severe_by_type.items()
                    ],
                    key=lambda x: abs(float(x["total"])),
                    reverse=True,
                )[:10],
            }
            calc_issues.extend(_build_negative_delta_issues(
                negative_summary=neg_summary,
                clamp_threshold=clamp_threshold,
            ))

            all_issues = quality_issues + calc_issues
            ds_condition = (
                f"metric_name='energy'; ratio={self._target_label}/"
                f"all; intersection=target_present_and_total>0"
            )

            if intersection_hours == 0:
                return CalculationResult(
                    metric_name=self.metric_name,
                    value=None,
                    unit=self.unit,
                    status="no_data",
                    formula=self.formula,
                    formula_with_values=f"目标组件({self._target_label})在所选时段内无能耗数据。",
                    sql_executed=bucket_sql,
                    input_records=total_records,
                    valid_records=0,
                    data_source_field="agg_delta",
                    data_source_condition=ds_condition,
                    quality_score=quality_score,
                    quality_issues=all_issues,
                    breakdown=breakdown,
                )

            if total == 0:
                all_issues = all_issues + [{
                    "type": "zero_denominator",
                    "description": "交集时段内系统总能耗为0，无法计算占比。",
                }]
                return CalculationResult(
                    metric_name=self.metric_name,
                    value=None,
                    unit=self.unit,
                    status="partial",
                    formula=self.formula,
                    formula_with_values="= 0 / 0 (交集时段总能耗为0)",
                    sql_executed=bucket_sql,
                    input_records=total_records,
                    valid_records=total_records,
                    data_source_field="agg_delta",
                    data_source_condition=ds_condition,
                    quality_score=quality_score,
                    quality_issues=all_issues,
                    breakdown=breakdown,
                )

            ratio = round(part / total * 100, 2)
            formula_with_values = f"= {part} / {total} x 100% = {ratio}%"
            status = "partial" if all_issues else "success"
            return CalculationResult(
                metric_name=self.metric_name,
                value=ratio,
                unit=self.unit,
                status=status,
                formula=self.formula,
                formula_with_values=formula_with_values,
                sql_executed=bucket_sql,
                input_records=total_records,
                valid_records=total_records,
                data_source_field="agg_delta",
                data_source_condition=ds_condition,
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






