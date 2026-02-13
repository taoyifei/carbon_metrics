"""
数据质量检测模块
"""
from __future__ import annotations

import pymysql

from .db import LOGGER


def compute_agg_hour_quality(conn: pymysql.Connection) -> None:
    """计算小时聚合数据的质量指标"""
    LOGGER.info("  [quality] 开始计算小时聚合质量...")

    # 清空旧数据
    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE agg_hour_quality")

    # 插入质量数据
    sql = """
        INSERT INTO agg_hour_quality
        (bucket_time, building_id, system_id, equipment_type, equipment_id,
         sub_equipment_id, metric_name, expected_samples, actual_samples,
         completeness_rate, negative_count, quality_score, quality_level)
        SELECT
            ah.bucket_time,
            ah.building_id,
            ah.system_id,
            ah.equipment_type,
            ah.equipment_id,
            ah.sub_equipment_id,
            ah.metric_name,
            12 AS expected_samples,
            ah.sample_count AS actual_samples,
            ROUND(LEAST(100, ah.sample_count * 100.0 / 12), 2) AS completeness_rate,
            CASE WHEN ah.agg_min < 0 THEN 1 ELSE 0 END AS negative_count,
            ROUND(
                LEAST(100, ah.sample_count * 100.0 / 12) *
                CASE WHEN ah.agg_min < 0 THEN 0.8 ELSE 1.0 END,
                2
            ) AS quality_score,
            CASE
                WHEN ah.sample_count >= 10 AND ah.agg_min >= 0 THEN 'good'
                WHEN ah.sample_count >= 6 THEN 'warning'
                ELSE 'poor'
            END AS quality_level
        FROM agg_hour ah
    """

    with conn.cursor() as cursor:
        cursor.execute(sql)
        affected_rows = cursor.rowcount
    conn.commit()

    # 统计质量分布
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT quality_level, COUNT(*) AS cnt
            FROM agg_hour_quality
            GROUP BY quality_level
        """)
        stats = cursor.fetchall()

    stats_str = ", ".join([f"{s['quality_level']}:{s['cnt']}" for s in stats])
    LOGGER.info(f"  [quality] 完成! 插入 {affected_rows} 条质量记录, 分布: {stats_str}")


def compute_agg_day_quality(conn: pymysql.Connection) -> None:
    """计算日聚合数据的质量指标"""
    LOGGER.info("  [quality] 开始计算日聚合质量...")

    # 清空旧数据
    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE agg_day_quality")

    # 从小时质量汇总到日质量
    sql = """
        INSERT INTO agg_day_quality
        (bucket_time, building_id, system_id, equipment_type, equipment_id,
         sub_equipment_id, metric_name, expected_hours, actual_hours,
         completeness_rate, total_negative_count, quality_score, quality_level)
        SELECT
            DATE(bucket_time) AS bucket_time,
            building_id,
            system_id,
            equipment_type,
            equipment_id,
            sub_equipment_id,
            metric_name,
            24 AS expected_hours,
            COUNT(*) AS actual_hours,
            ROUND(LEAST(100, COUNT(*) * 100.0 / 24), 2) AS completeness_rate,
            SUM(negative_count) AS total_negative_count,
            ROUND(AVG(quality_score), 2) AS quality_score,
            CASE
                WHEN COUNT(*) >= 22 AND SUM(negative_count) = 0 THEN 'good'
                WHEN COUNT(*) >= 12 THEN 'warning'
                ELSE 'poor'
            END AS quality_level
        FROM agg_hour_quality
        GROUP BY DATE(bucket_time), building_id, system_id, equipment_type,
                 equipment_id, sub_equipment_id, metric_name
    """

    with conn.cursor() as cursor:
        cursor.execute(sql)
        affected_rows = cursor.rowcount
    conn.commit()

    LOGGER.info(f"  [quality] 完成! 插入 {affected_rows} 条日质量记录")
