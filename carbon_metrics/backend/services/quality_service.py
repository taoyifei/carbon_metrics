"""
数据质量查询服务
提供数据质量统计、明细查询、异常问题查询等功能
"""
from datetime import datetime
from typing import Optional, List, Dict, Any, Literal
from math import ceil
import logging
import time

from ..db import get_db

logger = logging.getLogger(__name__)


class QualityService:
    """数据质量查询服务"""

    def __init__(self):
        self.db = get_db()

    @staticmethod
    def _is_day(granularity: str) -> bool:
        return granularity == "day"

    @staticmethod
    def _issue_col(granularity: str, hour_col: str) -> str:
        """将 hour 表列名映射到 day 表列名"""
        if granularity != "day":
            return hour_col
        mapping = {
            "gap_count": "COALESCE(CEIL(total_gap_hours), 0)",
            "max_gap_seconds": "0",
            "negative_count": "total_negative_count",
            "jump_count": "total_jump_count",
            "out_of_range_count": "0",
            "expected_samples": "expected_hours",
            "actual_samples": "actual_hours",
        }
        return mapping.get(hour_col, hour_col)

    @staticmethod
    def _issue_where_col(granularity: str, hour_col: str) -> str:
        """将 hour 表 WHERE 条件列名映射到 day 表"""
        if granularity != "day":
            return hour_col
        mapping = {
            "gap_count": "total_gap_hours",
            "negative_count": "total_negative_count",
            "jump_count": "total_jump_count",
            "out_of_range_count": "0",  # day 表无此列
        }
        return mapping.get(hour_col, hour_col)

    def get_summary(
        self,
        time_start: datetime,
        time_end: datetime,
        building_id: Optional[str] = None,
        system_id: Optional[str] = None,
        equipment_type: Optional[str] = None,
        quality_level: Optional[str] = None,
        granularity: Literal["hour", "day"] = "hour",
    ) -> Dict[str, Any]:
        """
        获取数据质量汇总统计

        Args:
            time_start: 开始时间
            time_end: 结束时间
            building_id: 机楼筛选
            system_id: 系统筛选
            equipment_type: 设备类型筛选
            quality_level: 质量等级筛选 (good/warning/poor)
            granularity: 时间粒度 (hour/day)

        Returns:
            质量汇总统计字典
        """
        table = "agg_hour_quality" if granularity == "hour" else "agg_day_quality"

        conditions = ["bucket_time >= %s", "bucket_time < %s"]
        params: List[Any] = [time_start, time_end]

        if building_id:
            conditions.append("building_id = %s")
            params.append(building_id)
        if system_id:
            conditions.append("system_id = %s")
            params.append(system_id)
        if equipment_type:
            conditions.append("equipment_type = %s")
            params.append(equipment_type)
        if quality_level:
            conditions.append("quality_level = %s")
            params.append(quality_level)

        where_clause = " AND ".join(conditions)

        gap_col = self._issue_col(granularity, "gap_count")
        neg_col = self._issue_col(granularity, "negative_count")
        jmp_col = self._issue_col(granularity, "jump_count")

        sql = f"""
            SELECT
                COUNT(*) AS total_records,
                SUM(CASE WHEN quality_level = 'good' THEN 1 ELSE 0 END) AS good_count,
                SUM(CASE WHEN quality_level = 'warning' THEN 1 ELSE 0 END) AS warning_count,
                SUM(CASE WHEN quality_level = 'poor' THEN 1 ELSE 0 END) AS poor_count,
                COALESCE(AVG(quality_score), 0) AS avg_quality_score,
                COALESCE(AVG(completeness_rate), 0) AS avg_completeness_rate,
                COALESCE(SUM({gap_col}), 0) AS total_gaps,
                COALESCE(SUM({neg_col}), 0) AS total_negatives,
                COALESCE(SUM({jmp_col}), 0) AS total_jumps
            FROM {table}
            WHERE {where_clause}
        """

        try:
            with self.db.cursor() as cursor:
                cursor.execute(sql, params)
                row = cursor.fetchone()

                if row is None or row.get("total_records", 0) == 0:
                    return {
                        "total_records": 0,
                        "good_count": 0,
                        "warning_count": 0,
                        "poor_count": 0,
                        "avg_quality_score": 0.0,
                        "avg_completeness_rate": 0.0,
                        "total_gaps": 0,
                        "total_negatives": 0,
                        "total_jumps": 0,
                    }

                return {
                    "total_records": int(row["total_records"] or 0),
                    "good_count": int(row["good_count"] or 0),
                    "warning_count": int(row["warning_count"] or 0),
                    "poor_count": int(row["poor_count"] or 0),
                    "avg_quality_score": round(float(row["avg_quality_score"] or 0), 2),
                    "avg_completeness_rate": round(float(row["avg_completeness_rate"] or 0), 2),
                    "total_gaps": int(row["total_gaps"] or 0),
                    "total_negatives": int(row["total_negatives"] or 0),
                    "total_jumps": int(row["total_jumps"] or 0),
                }
        except Exception as e:
            logger.error(f"获取质量汇总失败: {e}")
            raise

    def get_list(
        self,
        time_start: datetime,
        time_end: datetime,
        building_id: Optional[str] = None,
        system_id: Optional[str] = None,
        equipment_type: Optional[str] = None,
        equipment_id: Optional[str] = None,
        quality_level: Optional[str] = None,
        granularity: Literal["hour", "day"] = "hour",
        page: int = 1,
        page_size: int = 20,
    ) -> Dict[str, Any]:
        """
        获取数据质量明细列表（分页）
        """
        table = "agg_hour_quality" if granularity == "hour" else "agg_day_quality"

        conditions = ["bucket_time >= %s", "bucket_time < %s"]
        params: List[Any] = [time_start, time_end]

        if building_id:
            conditions.append("building_id = %s")
            params.append(building_id)
        if system_id:
            conditions.append("system_id = %s")
            params.append(system_id)
        if equipment_type:
            conditions.append("equipment_type = %s")
            params.append(equipment_type)
        if equipment_id:
            conditions.append("equipment_id = %s")
            params.append(equipment_id)
        if quality_level:
            conditions.append("quality_level = %s")
            params.append(quality_level)

        where_clause = " AND ".join(conditions)

        # 查询总数
        count_sql = f"SELECT COUNT(*) AS total FROM {table} WHERE {where_clause}"

        # 查询数据
        offset = (page - 1) * page_size
        exp_col = self._issue_col(granularity, "expected_samples")
        act_col = self._issue_col(granularity, "actual_samples")
        gap_col = self._issue_col(granularity, "gap_count")
        maxgap_col = self._issue_col(granularity, "max_gap_seconds")
        neg_col = self._issue_col(granularity, "negative_count")
        jmp_col = self._issue_col(granularity, "jump_count")
        oor_col = self._issue_col(granularity, "out_of_range_count")

        data_sql = f"""
            SELECT
                bucket_time, building_id, system_id, equipment_type,
                equipment_id, sub_equipment_id, metric_name,
                quality_score, quality_level, completeness_rate,
                {exp_col} AS expected_samples, {act_col} AS actual_samples,
                {gap_col} AS gap_count, {maxgap_col} AS max_gap_seconds,
                {neg_col} AS negative_count,
                {jmp_col} AS jump_count, {oor_col} AS out_of_range_count,
                issues_json
            FROM {table}
            WHERE {where_clause}
            ORDER BY bucket_time DESC, quality_score ASC
            LIMIT %s OFFSET %s
        """

        try:
            with self.db.cursor() as cursor:
                # 获取总数
                cursor.execute(count_sql, params)
                total = cursor.fetchone()["total"]

                # 获取数据
                cursor.execute(data_sql, params + [page_size, offset])
                rows = cursor.fetchall()

                items = []
                for row in rows:
                    items.append(self._row_to_quality_record(row))

                total_pages = ceil(total / page_size) if total > 0 else 1

                return {
                    "items": items,
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": total_pages,
                }
        except Exception as e:
            logger.error(f"获取质量列表失败: {e}")
            raise

    def _row_to_quality_record(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """将数据库行转换为质量记录字典"""
        issues = []
        if row.get("issues_json"):
            try:
                import json
                issues = json.loads(row["issues_json"]) if isinstance(row["issues_json"], str) else row["issues_json"]
            except (json.JSONDecodeError, TypeError):
                issues = []

        return {
            "bucket_time": row["bucket_time"],
            "building_id": row["building_id"],
            "system_id": row["system_id"],
            "equipment_type": row["equipment_type"],
            "equipment_id": row.get("equipment_id"),
            "sub_equipment_id": row.get("sub_equipment_id"),
            "metric_name": row["metric_name"],
            "quality_score": float(row["quality_score"] or 0),
            "quality_level": row["quality_level"],
            "completeness_rate": float(row["completeness_rate"] or 0),
            "expected_samples": int(row.get("expected_samples") or 12),
            "actual_samples": int(row.get("actual_samples") or 0),
            "gap_count": int(row.get("gap_count") or 0),
            "max_gap_seconds": int(row.get("max_gap_seconds") or 0),
            "negative_count": int(row.get("negative_count") or 0),
            "jump_count": int(row.get("jump_count") or 0),
            "out_of_range_count": int(row.get("out_of_range_count") or 0),
            "issues": issues,
        }

    def get_issues(
        self,
        time_start: datetime,
        time_end: datetime,
        issue_type: Optional[str] = None,
        building_id: Optional[str] = None,
        system_id: Optional[str] = None,
        equipment_type: Optional[str] = None,
        severity: Optional[str] = None,
        granularity: Literal["hour", "day"] = "hour",
        page: int = 1,
        page_size: int = 20,
    ) -> Dict[str, Any]:
        """
        获取数据异常问题列表

        Args:
            issue_type: 问题类型 (gap/negative/jump/out_of_range)
            severity: 严重程度 (high/medium/low)
        """
        table = "agg_hour_quality" if granularity == "hour" else "agg_day_quality"

        conditions = ["bucket_time >= %s", "bucket_time < %s"]
        params: List[Any] = [time_start, time_end]

        # 根据问题类型筛选有问题的记录（day 表列名不同）
        gap_w = self._issue_where_col(granularity, "gap_count")
        neg_w = self._issue_where_col(granularity, "negative_count")
        jmp_w = self._issue_where_col(granularity, "jump_count")
        oor_w = self._issue_where_col(granularity, "out_of_range_count")

        issue_conditions = []
        if issue_type == "gap":
            issue_conditions.append(f"{gap_w} > 0")
        elif issue_type == "negative":
            issue_conditions.append(f"{neg_w} > 0")
        elif issue_type == "jump":
            issue_conditions.append(f"{jmp_w} > 0")
        elif issue_type == "out_of_range":
            issue_conditions.append(f"{oor_w} > 0")
        else:
            # 任意问题
            issue_conditions.append(
                f"({gap_w} > 0 OR {neg_w} > 0 OR {jmp_w} > 0 OR {oor_w} > 0)"
            )

        conditions.extend(issue_conditions)

        if building_id:
            conditions.append("building_id = %s")
            params.append(building_id)
        if system_id:
            conditions.append("system_id = %s")
            params.append(system_id)
        if equipment_type:
            conditions.append("equipment_type = %s")
            params.append(equipment_type)

        where_clause = " AND ".join(conditions)

        gap_col = self._issue_col(granularity, "gap_count")
        maxgap_col = self._issue_col(granularity, "max_gap_seconds")
        neg_col = self._issue_col(granularity, "negative_count")
        jmp_col = self._issue_col(granularity, "jump_count")
        oor_col = self._issue_col(granularity, "out_of_range_count")

        def _severity_case(expr: str) -> str:
            return (
                f"CASE WHEN {expr} >= 5 THEN 'high' "
                f"WHEN {expr} >= 2 THEN 'medium' ELSE 'low' END"
            )

        branches: List[str] = []
        branch_params: List[Any] = []

        def _add_branch(
            issue_kind: str,
            count_expr: str,
            where_expr: str,
            max_gap_expr: str = "0",
        ) -> None:
            branches.append(
                f"""
                SELECT
                    bucket_time, building_id, system_id, equipment_type,
                    equipment_id, sub_equipment_id, metric_name, quality_score,
                    '{issue_kind}' AS issue_type,
                    {count_expr} AS issue_count,
                    {max_gap_expr} AS max_gap_seconds,
                    {_severity_case(count_expr)} AS severity
                FROM {table}
                WHERE {where_clause} AND {where_expr} > 0
                """
            )
            branch_params.extend(params)

        if issue_type in (None, "gap"):
            _add_branch("gap", gap_col, gap_w, max_gap_expr=maxgap_col)
        if issue_type in (None, "negative"):
            _add_branch("negative", neg_col, neg_w)
        if issue_type in (None, "jump"):
            _add_branch("jump", jmp_col, jmp_w)
        if issue_type in (None, "out_of_range"):
            _add_branch("out_of_range", oor_col, oor_w)

        if not branches:
            return {
                "items": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "total_pages": 1,
            }

        union_sql = " UNION ALL ".join(branches)
        severity_clause = "WHERE severity = %s" if severity else ""
        severity_params: List[Any] = [severity] if severity else []

        count_sql = f"""
            SELECT COUNT(*) AS total
            FROM ({union_sql}) AS issue_rows
            {severity_clause}
        """

        data_sql = f"""
            SELECT
                bucket_time, building_id, system_id, equipment_type,
                equipment_id, sub_equipment_id, metric_name,
                issue_type, issue_count, max_gap_seconds, severity, quality_score
            FROM ({union_sql}) AS issue_rows
            {severity_clause}
            ORDER BY bucket_time DESC, quality_score ASC
            LIMIT %s OFFSET %s
        """

        try:
            with self.db.cursor() as cursor:
                scan_start = time.perf_counter()
                offset = (page - 1) * page_size

                cursor.execute(count_sql, branch_params + severity_params)
                total = int(cursor.fetchone()["total"] or 0)

                cursor.execute(
                    data_sql,
                    branch_params + severity_params + [page_size, offset],
                )
                rows = cursor.fetchall()
                items = [self._row_to_issue(item) for item in rows]

                total_pages = ceil(total / page_size) if total > 0 else 1
                elapsed_ms = int((time.perf_counter() - scan_start) * 1000)
                logger.info(
                    "issues_query total_issues=%s page=%s page_size=%s elapsed_ms=%s",
                    total, page, page_size, elapsed_ms
                )

                return {
                    "items": items,
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": total_pages,
                }
        except Exception as e:
            logger.error(f"获取异常问题列表失败: {e}")
            raise

    def _row_to_issue(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """将 issue 查询结果行映射为 API 结构"""
        issue_kind = row["issue_type"]
        issue_count = int(row.get("issue_count") or 0)
        max_gap_seconds = int(row.get("max_gap_seconds") or 0)

        if issue_kind == "gap":
            description = f"存在 {issue_count} 个时间缺口，最大缺口 {max_gap_seconds} 秒"
            details = {"max_gap_seconds": max_gap_seconds}
        elif issue_kind == "negative":
            description = f"存在 {issue_count} 个负值"
            details = {}
        elif issue_kind == "jump":
            description = f"存在 {issue_count} 个异常跳变"
            details = {}
        else:
            description = f"存在 {issue_count} 个超量程值"
            details = {}

        return {
            "bucket_time": row["bucket_time"],
            "building_id": row["building_id"],
            "system_id": row["system_id"],
            "equipment_type": row["equipment_type"],
            "equipment_id": row.get("equipment_id"),
            "sub_equipment_id": row.get("sub_equipment_id"),
            "metric_name": row["metric_name"],
            "issue_type": issue_kind,
            "description": description,
            "severity": row["severity"],
            "count": issue_count,
            "details": details,
        }

    def get_equipment_trend(
        self,
        equipment_id: str,
        time_start: datetime,
        time_end: datetime,
        metric_name: Optional[str] = None,
        granularity: Literal["hour", "day"] = "hour",
    ) -> List[Dict[str, Any]]:
        """
        获取单个设备的质量趋势
        """
        table = "agg_hour_quality" if granularity == "hour" else "agg_day_quality"

        conditions = [
            "equipment_id = %s",
            "bucket_time >= %s",
            "bucket_time < %s",
        ]
        params: List[Any] = [equipment_id, time_start, time_end]

        if metric_name:
            conditions.append("metric_name = %s")
            params.append(metric_name)

        where_clause = " AND ".join(conditions)

        gap_col = self._issue_col(granularity, "gap_count")
        neg_col = self._issue_col(granularity, "negative_count")
        jmp_col = self._issue_col(granularity, "jump_count")
        oor_col = self._issue_col(granularity, "out_of_range_count")

        sql = f"""
            SELECT
                bucket_time,
                AVG(quality_score) AS quality_score,
                AVG(completeness_rate) AS completeness_rate,
                SUM({gap_col} + {neg_col} + {jmp_col} + {oor_col}) AS issue_count
            FROM {table}
            WHERE {where_clause}
            GROUP BY bucket_time
            ORDER BY bucket_time
        """

        try:
            with self.db.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()

                return [
                    {
                        "bucket_time": row["bucket_time"],
                        "quality_score": round(float(row["quality_score"] or 0), 2),
                        "completeness_rate": round(float(row["completeness_rate"] or 0), 2),
                        "issue_count": int(row["issue_count"] or 0),
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.error(f"获取设备质量趋势失败: {e}")
            raise

    def get_equipment_list(self) -> List[Dict[str, Any]]:
        """获取所有设备列表"""
        sql = """
            SELECT DISTINCT
                building_id, system_id, equipment_type,
                equipment_id, sub_equipment_id
            FROM equipment_registry
            WHERE is_active = 1
            ORDER BY building_id, system_id, equipment_type, equipment_id
        """

        try:
            with self.db.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"获取设备列表失败: {e}")
            raise

    def get_raw_report(self) -> List[Dict[str, Any]]:
        """读取 data_quality_deep_report.csv 返回结构化数据"""
        import csv
        from pathlib import Path

        csv_path = Path(__file__).resolve().parents[4] / "docs" / "data_quality_deep_report.csv"

        if not csv_path.exists():
            logger.warning(f"质量报告文件不存在: {csv_path}")
            return []

        try:
            with open(csv_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                items = []
                skipped_rows = 0
                for row_no, row in enumerate(reader, start=2):
                    try:
                        items.append({
                            "table": row.get("table", ""),
                            "time_column": row.get("time_column", ""),
                            "value_column": row.get("value_column", ""),
                            "key_columns": row.get("key_columns", ""),
                            "total_rows": int(row.get("total_rows") or 0),
                            "key_count": int(row.get("key_count") or 0),
                            "time_start": row.get("time_start", ""),
                            "time_end": row.get("time_end", ""),
                            "min_value": float(row.get("min_value") or 0),
                            "max_value": float(row.get("max_value") or 0),
                            "negative_values": int(row.get("negative_values") or 0),
                            "mode_interval_seconds": int(float(row.get("mode_interval_seconds") or 0)),
                            "interval_irregular_rate": round(float(row.get("interval_irregular_rate") or 0), 4),
                            "max_gap_seconds": int(row.get("max_gap_seconds") or 0),
                            "gap_count": int(row.get("gap_count") or 0),
                            "duplicate_rows": int(row.get("duplicate_rows") or 0),
                            "jump_anomaly_count": int(row.get("jump_anomaly_count") or 0),
                        })
                    except (TypeError, ValueError) as e:
                        skipped_rows += 1
                        logger.warning(
                            "Skip invalid quality report row row_no=%s table=%s error=%s",
                            row_no, row.get("table", ""), str(e)
                        )
                if skipped_rows > 0:
                    logger.warning("Skipped %s invalid rows in quality report: %s", skipped_rows, csv_path)
                return items
        except Exception as e:
            logger.error(f"读取质量报告失败: {e}")
            raise
