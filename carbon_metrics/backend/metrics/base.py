"""
指标计算基类
定义指标计算的通用接口和数据结构
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Tuple, Dict, Any


@dataclass
class MetricContext:
    """指标计算上下文"""
    time_start: datetime
    time_end: datetime
    building_id: Optional[str] = None
    system_id: Optional[str] = None
    equipment_type: Optional[str] = None
    equipment_id: Optional[str] = None
    sub_equipment_id: Optional[str] = None


@dataclass
class CalculationResult:
    """计算结果数据类"""
    metric_name: str
    value: Optional[float]
    unit: str
    status: str  # success, partial, failed, no_data

    # 追溯信息
    formula: str = ""
    formula_with_values: str = ""
    sql_executed: str = ""

    # 数据统计
    input_records: int = 0
    valid_records: int = 0

    # 质量信息
    quality_score: float = 100.0
    quality_issues: List[Dict[str, Any]] = field(default_factory=list)

    # 数据来源
    data_source_field: str = "agg_avg"
    data_source_condition: str = ""

    # 分解明细
    breakdown: List[Dict[str, Any]] = field(default_factory=list)


class BaseMetric(ABC):
    """指标计算基类"""

    def __init__(
        self,
        db,
        query_cache: Optional[Dict[str, Any]] = None,
        include_dependency_diagnostics: bool = False,
    ):
        self.db = db
        self._query_cache = query_cache
        self._include_dependency_diagnostics = include_dependency_diagnostics

    @property
    @abstractmethod
    def metric_name(self) -> str:
        """指标名称"""
        pass

    @property
    @abstractmethod
    def unit(self) -> str:
        """单位"""
        pass

    @property
    @abstractmethod
    def formula(self) -> str:
        """计算公式描述"""
        pass

    @abstractmethod
    def calculate(self, ctx: MetricContext) -> CalculationResult:
        """执行计算"""
        pass

    def _build_where(self, ctx: MetricContext, metric_name: str,
                     equipment_type: Optional[str] = None,
                     extra_conditions: Optional[List[str]] = None) -> Tuple[str, List[Any]]:
        """构建通用 WHERE 子句和参数列表"""
        conditions = [
            "metric_name = %s",
            "bucket_time >= %s",
            "bucket_time < %s",
        ]
        params: List[Any] = [metric_name, ctx.time_start, ctx.time_end]

        eq_type = equipment_type or ctx.equipment_type
        if eq_type:
            conditions.append("equipment_type = %s")
            params.append(eq_type)
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
        if extra_conditions:
            conditions.extend(extra_conditions)

        return " AND ".join(conditions), params

    def _cached_fetchone(self, cursor, sql: str, params: List[Any]):
        """带缓存的 fetchone，相同 SQL+params 只查一次"""
        if self._query_cache is not None:
            key = ("fetchone", sql.strip(), tuple(str(p) for p in params))
            if key in self._query_cache:
                return self._query_cache[key]
            cursor.execute(sql, params)
            row = cursor.fetchone()
            self._query_cache[key] = row
            return row
        cursor.execute(sql, params)
        return cursor.fetchone()

    def _cached_fetchall(self, cursor, sql: str, params: List[Any]):
        """带缓存的 fetchall，相同 SQL+params 只查一次"""
        if self._query_cache is not None:
            key = ("fetchall", sql.strip(), tuple(str(p) for p in params))
            if key in self._query_cache:
                return self._query_cache[key]
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            self._query_cache[key] = rows
            return rows
        cursor.execute(sql, params)
        return cursor.fetchall()

    def _build_scope_conditions(
        self,
        ctx: MetricContext,
        equipment_type: Optional[str] = None,
        equipment_types: Optional[List[str]] = None,
    ) -> Tuple[List[str], List[Any]]:
        """构建不含 metric_name 的筛选条件"""
        conditions = [
            "bucket_time >= %s",
            "bucket_time < %s",
        ]
        params: List[Any] = [ctx.time_start, ctx.time_end]

        eq_type = equipment_type or ctx.equipment_type
        if eq_type:
            conditions.append("equipment_type = %s")
            params.append(eq_type)
        elif equipment_types:
            placeholders = ", ".join(["%s"] * len(equipment_types))
            conditions.append(f"equipment_type IN ({placeholders})")
            params.extend(equipment_types)
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

        return conditions, params

    def _get_dependency_counts(
        self,
        cursor,
        ctx: MetricContext,
        required_metrics: List[str],
        equipment_type: Optional[str] = None,
        equipment_types: Optional[List[str]] = None,
    ) -> Dict[str, int]:
        """查询依赖指标在 agg_hour 中的记录数"""
        if not required_metrics:
            return {}

        conditions, params = self._build_scope_conditions(
            ctx, equipment_type, equipment_types)
        placeholders = ", ".join(["%s"] * len(required_metrics))
        conditions.append(f"metric_name IN ({placeholders})")
        where_clause = " AND ".join(conditions)
        sql = f"""
            SELECT metric_name, COUNT(*) AS cnt
            FROM agg_hour
            WHERE {where_clause}
            GROUP BY metric_name
        """
        rows = self._cached_fetchall(cursor, sql, params + required_metrics)

        counts = {m: 0 for m in required_metrics}
        for row in rows:
            metric_name = row["metric_name"]
            if metric_name in counts:
                counts[metric_name] = int(row["cnt"] or 0)
        return counts

    def _get_scope_metric_counts(
        self,
        cursor,
        ctx: MetricContext,
        equipment_type: Optional[str] = None,
        equipment_types: Optional[List[str]] = None,
        limit: int = 20,
    ) -> Dict[str, int]:
        """查询当前筛选范围内可用的 metric_name 及记录数（用于 no_data 诊断）"""
        conditions, params = self._build_scope_conditions(
            ctx, equipment_type, equipment_types)
        where_clause = " AND ".join(conditions)
        sql = f"""
            SELECT metric_name, COUNT(*) AS cnt
            FROM agg_hour
            WHERE {where_clause}
            GROUP BY metric_name
            ORDER BY cnt DESC
            LIMIT %s
        """
        rows = self._cached_fetchall(cursor, sql, params + [limit])
        return {
            str(row["metric_name"]): int(row["cnt"] or 0)
            for row in rows
            if row.get("metric_name") is not None
        }

    @staticmethod
    def _build_metric_reason(
        agg_scope_count: int,
        agg_global_count: int,
        canonical_scope_count: int,
        canonical_global_count: int,
        raw_mapped_scope_count: int,
        mapped_point_count: int,
    ) -> str:
        if agg_scope_count > 0:
            return "present_in_scope_but_missing_dependency_flagged"
        if agg_global_count > 0:
            return "data_exists_in_db_but_filtered_out_by_scope"
        if canonical_scope_count > 0 or canonical_global_count > 0:
            return "canonical_exists_but_not_aggregated"
        if raw_mapped_scope_count > 0:
            return "raw_exists_and_mapped_but_not_canonicalized"
        if mapped_point_count > 0:
            return "mapping_exists_but_no_raw_in_scope"
        return "mapping_not_hit_or_metric_unrecognized"

    def _query_missing_metric_diagnostics(
        self,
        cursor,
        ctx: MetricContext,
        missing_metrics: List[str],
        equipment_type: Optional[str] = None,
        equipment_types: Optional[List[str]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        if not missing_metrics:
            return {}

        placeholders = ", ".join(["%s"] * len(missing_metrics))
        diagnostics: Dict[str, Dict[str, Any]] = {
            metric_name: {
                "agg_scope_count": 0,
                "agg_global_count": 0,
                "canonical_scope_count": 0,
                "canonical_global_count": 0,
                "raw_mapped_scope_count": 0,
                "mapped_point_count": 0,
                "reason": "unknown",
            }
            for metric_name in missing_metrics
        }

        scope_conditions, scope_params = self._build_scope_conditions(
            ctx, equipment_type, equipment_types)
        scope_conditions.append(f"metric_name IN ({placeholders})")
        scope_where = " AND ".join(scope_conditions)

        agg_scope_sql = f"""
            SELECT metric_name, COUNT(*) AS cnt
            FROM agg_hour
            WHERE {scope_where}
            GROUP BY metric_name
        """
        agg_scope_rows = self._cached_fetchall(
            cursor, agg_scope_sql, scope_params + missing_metrics)

        agg_global_sql = f"""
            SELECT metric_name, COUNT(*) AS cnt
            FROM agg_hour
            WHERE bucket_time >= %s AND bucket_time < %s
              AND metric_name IN ({placeholders})
            GROUP BY metric_name
        """
        agg_global_rows = self._cached_fetchall(
            cursor, agg_global_sql, [ctx.time_start, ctx.time_end] + missing_metrics)

        canonical_scope_conditions = [
            "ts >= %s",
            "ts < %s",
            f"metric_name IN ({placeholders})",
        ]
        canonical_scope_params: List[Any] = [ctx.time_start, ctx.time_end] + missing_metrics
        eq_type = equipment_type or ctx.equipment_type
        if eq_type:
            canonical_scope_conditions.append("equipment_type = %s")
            canonical_scope_params.append(eq_type)
        elif equipment_types:
            eq_placeholders = ", ".join(["%s"] * len(equipment_types))
            canonical_scope_conditions.append(f"equipment_type IN ({eq_placeholders})")
            canonical_scope_params.extend(equipment_types)
        if ctx.building_id:
            canonical_scope_conditions.append("building_id = %s")
            canonical_scope_params.append(ctx.building_id)
        if ctx.system_id:
            canonical_scope_conditions.append("system_id = %s")
            canonical_scope_params.append(ctx.system_id)
        if ctx.equipment_id:
            canonical_scope_conditions.append("equipment_id = %s")
            canonical_scope_params.append(ctx.equipment_id)
        if ctx.sub_equipment_id:
            canonical_scope_conditions.append("sub_equipment_id = %s")
            canonical_scope_params.append(ctx.sub_equipment_id)

        canonical_scope_sql = f"""
            SELECT metric_name, COUNT(*) AS cnt
            FROM canonical_measurement
            WHERE {" AND ".join(canonical_scope_conditions)}
            GROUP BY metric_name
        """
        canonical_scope_rows = self._cached_fetchall(
            cursor, canonical_scope_sql, canonical_scope_params)

        canonical_global_sql = f"""
            SELECT metric_name, COUNT(*) AS cnt
            FROM canonical_measurement
            WHERE ts >= %s AND ts < %s
              AND metric_name IN ({placeholders})
            GROUP BY metric_name
        """
        canonical_global_rows = self._cached_fetchall(
            cursor, canonical_global_sql, [ctx.time_start, ctx.time_end] + missing_metrics)

        mapping_conditions = ["is_active = 1", f"metric_name IN ({placeholders})"]
        mapping_params: List[Any] = missing_metrics.copy()
        if eq_type:
            mapping_conditions.append("equipment_type = %s")
            mapping_params.append(eq_type)
        elif equipment_types:
            eq_placeholders = ", ".join(["%s"] * len(equipment_types))
            mapping_conditions.append(f"equipment_type IN ({eq_placeholders})")
            mapping_params.extend(equipment_types)
        if ctx.building_id:
            mapping_conditions.append("building_id = %s")
            mapping_params.append(ctx.building_id)
        if ctx.system_id:
            mapping_conditions.append("system_id = %s")
            mapping_params.append(ctx.system_id)
        if ctx.equipment_id:
            mapping_conditions.append("equipment_id = %s")
            mapping_params.append(ctx.equipment_id)
        if ctx.sub_equipment_id:
            mapping_conditions.append("sub_equipment_id = %s")
            mapping_params.append(ctx.sub_equipment_id)

        mapping_sql = f"""
            SELECT metric_name, COUNT(*) AS cnt
            FROM point_mapping
            WHERE {" AND ".join(mapping_conditions)}
            GROUP BY metric_name
        """
        mapping_rows = self._cached_fetchall(cursor, mapping_sql, mapping_params)

        raw_mapping_conditions = [
            "rm.ts >= %s",
            "rm.ts < %s",
            "pm.is_active = 1",
            f"pm.metric_name IN ({placeholders})",
        ]
        raw_mapping_params: List[Any] = [ctx.time_start, ctx.time_end] + missing_metrics
        if eq_type:
            raw_mapping_conditions.append("pm.equipment_type = %s")
            raw_mapping_params.append(eq_type)
        elif equipment_types:
            eq_placeholders = ", ".join(["%s"] * len(equipment_types))
            raw_mapping_conditions.append(f"pm.equipment_type IN ({eq_placeholders})")
            raw_mapping_params.extend(equipment_types)
        if ctx.building_id:
            raw_mapping_conditions.append("pm.building_id = %s")
            raw_mapping_params.append(ctx.building_id)
        if ctx.system_id:
            raw_mapping_conditions.append("pm.system_id = %s")
            raw_mapping_params.append(ctx.system_id)
        if ctx.equipment_id:
            raw_mapping_conditions.append("pm.equipment_id = %s")
            raw_mapping_params.append(ctx.equipment_id)
        if ctx.sub_equipment_id:
            raw_mapping_conditions.append("pm.sub_equipment_id = %s")
            raw_mapping_params.append(ctx.sub_equipment_id)

        raw_mapped_sql = f"""
            SELECT pm.metric_name, COUNT(*) AS cnt
            FROM raw_measurement rm
            JOIN point_mapping pm
              ON pm.is_active = 1
             AND (
                 (pm.source_type = 'tag'
                  AND rm.source_type = 'tag'
                  AND pm.tag_name = rm.tag_name)
                 OR
                 (pm.source_type = 'device'
                  AND rm.source_type = 'device'
                  AND pm.device_path = rm.device_path
                  AND (pm.original_metric_name <=> rm.original_metric_name))
             )
            WHERE {" AND ".join(raw_mapping_conditions)}
            GROUP BY pm.metric_name
        """
        raw_mapped_rows = self._cached_fetchall(cursor, raw_mapped_sql, raw_mapping_params)

        def fill_counts(rows: List[Dict[str, Any]], key_name: str) -> None:
            for row in rows:
                metric_name = row.get("metric_name")
                if metric_name in diagnostics:
                    diagnostics[metric_name][key_name] = int(row.get("cnt") or 0)

        fill_counts(agg_scope_rows, "agg_scope_count")
        fill_counts(agg_global_rows, "agg_global_count")
        fill_counts(canonical_scope_rows, "canonical_scope_count")
        fill_counts(canonical_global_rows, "canonical_global_count")
        fill_counts(mapping_rows, "mapped_point_count")
        fill_counts(raw_mapped_rows, "raw_mapped_scope_count")

        for metric_name, item in diagnostics.items():
            item["reason"] = self._build_metric_reason(
                agg_scope_count=int(item["agg_scope_count"]),
                agg_global_count=int(item["agg_global_count"]),
                canonical_scope_count=int(item["canonical_scope_count"]),
                canonical_global_count=int(item["canonical_global_count"]),
                raw_mapped_scope_count=int(item["raw_mapped_scope_count"]),
                mapped_point_count=int(item["mapped_point_count"]),
            )

        unmapped_tag_sql = """
            SELECT rm.tag_name, COUNT(*) AS cnt
            FROM raw_measurement rm
            LEFT JOIN point_mapping pm
              ON pm.is_active = 1
             AND pm.source_type = 'tag'
             AND rm.source_type = 'tag'
             AND pm.tag_name = rm.tag_name
            WHERE rm.ts >= %s AND rm.ts < %s
              AND rm.source_type = 'tag'
              AND pm.id IS NULL
            GROUP BY rm.tag_name
            ORDER BY cnt DESC
            LIMIT 10
        """
        unmapped_tag_rows = self._cached_fetchall(
            cursor, unmapped_tag_sql, [ctx.time_start, ctx.time_end])
        unmapped_samples = [
            {"tag_name": str(row.get("tag_name") or ""), "count": int(row.get("cnt") or 0)}
            for row in unmapped_tag_rows
            if row.get("tag_name")
        ]
        for metric_name in diagnostics:
            diagnostics[metric_name]["unmapped_tag_samples"] = unmapped_samples

        return diagnostics

    def _build_missing_dependency_issues(
        self,
        cursor,
        ctx: MetricContext,
        required_metrics: List[str],
        equipment_type: Optional[str] = None,
        equipment_types: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """构建缺失依赖问题列表"""
        if not required_metrics:
            return []

        counts = self._get_dependency_counts(
            cursor, ctx, required_metrics, equipment_type, equipment_types)
        missing = [m for m in required_metrics if counts.get(m, 0) == 0]
        if not missing:
            return []

        details: Dict[str, Any] = {
            "required_metrics": required_metrics,
            "missing_metrics": missing,
            "metric_counts": counts,
            "available_metric_counts": self._get_scope_metric_counts(
                cursor, ctx, equipment_type, equipment_types),
            "scope": {
                "time_start": str(ctx.time_start),
                "time_end": str(ctx.time_end),
                "building_id": ctx.building_id,
                "system_id": ctx.system_id,
                "equipment_type": equipment_type or ctx.equipment_type,
                "equipment_types": equipment_types,
                "equipment_id": ctx.equipment_id,
                "sub_equipment_id": ctx.sub_equipment_id,
            },
        }
        if self._include_dependency_diagnostics:
            details["missing_metric_diagnostics"] = self._query_missing_metric_diagnostics(
                cursor=cursor,
                ctx=ctx,
                missing_metrics=missing,
                equipment_type=equipment_type,
                equipment_types=equipment_types,
            )
        return [{
            "type": "missing_dependency",
            "description": f"缺少依赖数据: {', '.join(missing)}",
            "count": len(missing),
            "details": details,
        }]

    def _check_quality_from_table(
        self,
        cursor,
        ctx: MetricContext,
        metric_names: List[str],
        equipment_type: Optional[str] = None,
        equipment_types: Optional[List[str]] = None,
    ) -> Tuple[float, List[Dict[str, Any]]]:
        """从 agg_hour_quality 计算质量分和质量问题"""
        if not metric_names:
            return 100.0, []

        conditions, params = self._build_scope_conditions(
            ctx, equipment_type, equipment_types)
        placeholders = ", ".join(["%s"] * len(metric_names))
        conditions.append(f"metric_name IN ({placeholders})")
        where_clause = " AND ".join(conditions)

        sql = f"""
            SELECT
                COUNT(*) AS total,
                COALESCE(AVG(quality_score), 0) AS avg_quality_score,
                COALESCE(AVG(completeness_rate), 0) AS avg_completeness_rate,
                COALESCE(SUM(gap_count), 0) AS total_gaps,
                COALESCE(SUM(negative_count), 0) AS total_negatives,
                COALESCE(SUM(jump_count), 0) AS total_jumps
            FROM agg_hour_quality
            WHERE {where_clause}
        """
        row = self._cached_fetchone(cursor, sql, params + metric_names)
        total = int(row["total"] or 0)

        if total == 0:
            return 100.0, []

        avg_quality_score = float(row["avg_quality_score"] or 0)
        avg_completeness_rate = float(row["avg_completeness_rate"] or 0)
        total_gaps = int(row["total_gaps"] or 0)
        total_negatives = int(row["total_negatives"] or 0)
        total_jumps = int(row["total_jumps"] or 0)

        issues: List[Dict[str, Any]] = []
        if avg_completeness_rate < 99.9:
            issues.append({
                "type": "completeness",
                "description": f"平均完整率 {round(avg_completeness_rate, 2)}%",
                "details": {"avg_completeness_rate": round(avg_completeness_rate, 2)},
            })
        if total_gaps > 0:
            issues.append({
                "type": "gap",
                "description": f"存在 {total_gaps} 个时间缺口",
                "count": total_gaps,
            })
        if total_negatives > 0:
            issues.append({
                "type": "negative",
                "description": f"存在 {total_negatives} 个负值",
                "count": total_negatives,
            })
        if total_jumps > 0:
            issues.append({
                "type": "jump",
                "description": f"存在 {total_jumps} 个异常跳变",
                "count": total_jumps,
            })

        return round(avg_quality_score, 2), issues

    @staticmethod
    def _status_from_issues(issues: List[Dict[str, Any]]) -> str:
        """根据问题列表生成统一状态语义"""
        return "partial" if issues else "success"
