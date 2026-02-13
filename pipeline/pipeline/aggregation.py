"""Aggregation stage for canonical measurements."""
from __future__ import annotations

from datetime import datetime, timedelta

import pymysql

from .db import LOGGER


def _floor_to_hour(ts: datetime) -> datetime:
    return ts.replace(minute=0, second=0, microsecond=0)


def compute_agg_hour(conn: pymysql.Connection, chunk_hours: int = 24) -> None:
    """Compute hourly aggregates from canonical_measurement in time chunks."""
    chunk_hours = max(1, int(chunk_hours))
    LOGGER.info("  [agg_hour] start computing hourly aggregates (chunk_hours=%s)", chunk_hours)

    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS cnt, MIN(ts) AS min_ts, MAX(ts) AS max_ts FROM canonical_measurement")
        stats = cursor.fetchone()

    total_records = int(stats["cnt"] or 0)
    min_ts = stats["min_ts"]
    max_ts = stats["max_ts"]

    if total_records == 0 or min_ts is None or max_ts is None:
        LOGGER.info("  [agg_hour] no canonical data found, skip")
        return

    range_start = _floor_to_hour(min_ts)
    range_end = _floor_to_hour(max_ts) + timedelta(hours=1)
    total_hours = int((range_end - range_start).total_seconds() // 3600)
    total_chunks = (total_hours + chunk_hours - 1) // chunk_hours

    LOGGER.info(
        "  [agg_hour] rows=%s, range=%s -> %s, total_hours=%s, chunks=%s",
        total_records,
        range_start,
        range_end,
        total_hours,
        total_chunks,
    )

    sql = """
        INSERT INTO agg_hour (
            bucket_time,
            building_id,
            system_id,
            equipment_type,
            equipment_id,
            sub_equipment_id,
            metric_name,
            agg_avg,
            agg_min,
            agg_max,
            agg_sum,
            agg_delta,
            agg_first,
            agg_last,
            sample_count,
            quality_flags
        )
        WITH hourly_agg AS (
            SELECT
                DATE_FORMAT(cm.ts, '%%Y-%%m-%%d %%H:00:00') AS bucket_time,
                cm.building_id,
                cm.system_id,
                cm.equipment_type,
                cm.equipment_id,
                cm.sub_equipment_id,
                cm.metric_name,
                AVG(cm.value) AS agg_avg,
                MIN(cm.value) AS agg_min,
                MAX(cm.value) AS agg_max,
                SUM(cm.value) AS agg_sum,
                CAST(
                    SUBSTRING_INDEX(
                        GROUP_CONCAT(cm.value ORDER BY cm.ts ASC, cm.id ASC SEPARATOR ','),
                        ',',
                        1
                    ) AS DOUBLE
                ) AS agg_first,
                CAST(
                    SUBSTRING_INDEX(
                        GROUP_CONCAT(cm.value ORDER BY cm.ts DESC, cm.id DESC SEPARATOR ','),
                        ',',
                        1
                    ) AS DOUBLE
                ) AS agg_last,
                COUNT(*) AS sample_count
            FROM canonical_measurement cm
            WHERE cm.ts >= %s AND cm.ts < %s
            GROUP BY
                bucket_time,
                cm.building_id,
                cm.system_id,
                cm.equipment_type,
                cm.equipment_id,
                cm.sub_equipment_id,
                cm.metric_name
        )
        SELECT
            ha.bucket_time,
            ha.building_id,
            ha.system_id,
            ha.equipment_type,
            ha.equipment_id,
            ha.sub_equipment_id,
            ha.metric_name,
            ha.agg_avg,
            ha.agg_min,
            ha.agg_max,
            ha.agg_sum,
            CASE
                WHEN ha.agg_first IS NULL OR ha.agg_last IS NULL THEN NULL
                ELSE ha.agg_last - ha.agg_first
            END AS agg_delta,
            ha.agg_first,
            ha.agg_last,
            ha.sample_count,
            '' AS quality_flags
        FROM hourly_agg ha
        ON DUPLICATE KEY UPDATE
            agg_avg = VALUES(agg_avg),
            agg_min = VALUES(agg_min),
            agg_max = VALUES(agg_max),
            agg_sum = VALUES(agg_sum),
            agg_delta = VALUES(agg_delta),
            agg_first = VALUES(agg_first),
            agg_last = VALUES(agg_last),
            sample_count = VALUES(sample_count),
            computed_at = CURRENT_TIMESTAMP
    """

    current_start = range_start
    chunk_idx = 0
    total_affected = 0

    while current_start < range_end:
        current_end = min(current_start + timedelta(hours=chunk_hours), range_end)
        chunk_idx += 1
        LOGGER.info("  [agg_hour] chunk %s/%s: %s -> %s", chunk_idx, total_chunks, current_start, current_end)

        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM agg_hour WHERE bucket_time >= %s AND bucket_time < %s",
                (current_start, current_end),
            )
            cursor.execute(sql, (current_start, current_end))
            affected_rows = cursor.rowcount
        conn.commit()

        total_affected += max(affected_rows, 0)
        LOGGER.info("  [agg_hour] chunk %s/%s done, affected=%s", chunk_idx, total_chunks, affected_rows)
        current_start = current_end

    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS cnt FROM agg_hour")
        result_count = cursor.fetchone()["cnt"]

    LOGGER.info("  [agg_hour] done! total affected=%s, agg_hour rows=%s", total_affected, result_count)


def compute_agg_day(conn: pymysql.Connection) -> None:
    """Compute daily aggregates from agg_hour (MySQL 8+ window function)."""
    LOGGER.info("  [agg_day] start computing daily aggregates...")

    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE agg_day")

    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(DISTINCT DATE(bucket_time)) AS cnt FROM agg_hour")
        total_days = cursor.fetchone()["cnt"]
        cursor.execute("SELECT COUNT(*) AS cnt FROM agg_hour")
        total_hours = cursor.fetchone()["cnt"]
    LOGGER.info("  [agg_day] source rows=%s -> days=%s", total_hours, total_days)

    sql = """
        INSERT INTO agg_day
        (bucket_time, building_id, system_id, equipment_type, equipment_id, sub_equipment_id,
         metric_name, agg_avg, agg_min, agg_max, agg_sum, agg_delta, agg_first, agg_last,
         sample_count, quality_flags)
        SELECT
            day_date AS bucket_time,
            building_id, system_id, equipment_type, equipment_id, sub_equipment_id,
            metric_name,
            AVG(agg_avg) AS agg_avg,
            MIN(agg_min) AS agg_min,
            MAX(agg_max) AS agg_max,
            SUM(agg_sum) AS agg_sum,
            MAX(CASE WHEN rn_last = 1 THEN agg_last END) -
            MAX(CASE WHEN rn_first = 1 THEN agg_first END) AS agg_delta,
            MAX(CASE WHEN rn_first = 1 THEN agg_first END) AS agg_first,
            MAX(CASE WHEN rn_last = 1 THEN agg_last END) AS agg_last,
            SUM(sample_count) AS sample_count,
            '' AS quality_flags
        FROM (
            SELECT
                DATE(bucket_time) AS day_date,
                building_id, system_id, equipment_type, equipment_id, sub_equipment_id,
                metric_name, agg_avg, agg_min, agg_max, agg_sum, agg_delta,
                agg_first, agg_last, sample_count,
                ROW_NUMBER() OVER (
                    PARTITION BY DATE(bucket_time), building_id, system_id, equipment_type,
                                 equipment_id, sub_equipment_id, metric_name
                    ORDER BY bucket_time ASC
                ) AS rn_first,
                ROW_NUMBER() OVER (
                    PARTITION BY DATE(bucket_time), building_id, system_id, equipment_type,
                                 equipment_id, sub_equipment_id, metric_name
                    ORDER BY bucket_time DESC
                ) AS rn_last
            FROM agg_hour
        ) AS ranked
        GROUP BY day_date, building_id, system_id, equipment_type,
                 equipment_id, sub_equipment_id, metric_name
        ON DUPLICATE KEY UPDATE
            agg_avg = VALUES(agg_avg),
            agg_min = VALUES(agg_min),
            agg_max = VALUES(agg_max),
            agg_sum = VALUES(agg_sum),
            agg_delta = VALUES(agg_delta),
            agg_first = VALUES(agg_first),
            agg_last = VALUES(agg_last),
            sample_count = VALUES(sample_count),
            computed_at = CURRENT_TIMESTAMP
    """

    LOGGER.info("  [agg_day] executing SQL...")
    with conn.cursor() as cursor:
        cursor.execute(sql)
        affected_rows = cursor.rowcount
    conn.commit()

    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS cnt FROM agg_day")
        result_count = cursor.fetchone()["cnt"]
    LOGGER.info("  [agg_day] done! affected=%s, agg_day rows=%s", affected_rows, result_count)
