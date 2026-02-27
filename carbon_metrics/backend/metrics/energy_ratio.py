"""能耗占比类指标计算"""
from math import ceil
from typing import Dict, List, Any, Optional, Tuple

from .base import BaseMetric, MetricContext, CalculationResult
from .energy import (
    CHILLER_TYPES, PUMP_TYPES, TOWER_TYPES,
    COMPONENT_RULES, COMPONENT_LABELS, STRICT_INTERSECTION_KEYS,
    _INCREMENTAL_DATA_THRESHOLD,
    _component_key_for_type,
    _query_energy_by_type,
    _query_energy_by_bucket_type,
    _aggregate_energy,
    _format_bucket,
    _build_minimum_calculable_summary,
    _build_negative_delta_issues,
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
            positive_clamp_threshold=self._positive_delta_clamp_threshold,
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
                    scoped_extra_types,
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
                positive_clamp_threshold=self._positive_delta_clamp_threshold,
                query_cache=self._query_cache,
            )
            if bucket_error:
                return CalculationResult(
                    metric_name=self.metric_name, value=None, unit=self.unit,
                    status="failed", formula=self.formula,
                    quality_issues=[{"type": "error", "description": bucket_error}],
                )

            # ----------------------------------------------------------
            # 最小可计算原则（严格交集）：
            # 要求该小时四类组件(chiller/chilled_pump/cooling_pump/tower)齐全，且总能耗 > 0
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
            # 筛选交集小时：四类组件齐全 且 该小时总能耗 > 0
            intersection_buckets: set = set()
            component_covered_hours: Dict[str, int] = {k: 0 for k in COMPONENT_LABELS}
            for bucket, type_energies in per_bucket.items():
                present_component_keys: set = set()
                for eq_type in type_energies:
                    comp = _component_key_for_type(eq_type)
                    if comp:
                        present_component_keys.add(comp)
                        component_covered_hours[comp] += 1
                bucket_total = sum(type_energies.values())
                if (
                    STRICT_INTERSECTION_KEYS.issubset(present_component_keys)
                    and bucket_total > 0
                ):
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
                        "因未满足四类组件严格交集或总能耗<=0被排除。"
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
                    "按最小可计算原则，仅使用满足四类组件严格交集"
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
                f"all; intersection=strict_all_4_components"
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
