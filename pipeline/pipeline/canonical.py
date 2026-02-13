"""Build canonical_measurement from raw_measurement and point_mapping."""
from __future__ import annotations

import math

import pymysql

from .db import LOGGER


TAG_INSERT_SQL = """
    INSERT INTO canonical_measurement
    (ts, building_id, system_id, equipment_type, equipment_id, sub_equipment_id,
     point_key, metric_name, value, unit, quality_flags, raw_id, batch_id, mapping_version)
    SELECT
        rm.ts,
        pm.building_id,
        pm.system_id,
        pm.equipment_type,
        pm.equipment_id,
        pm.sub_equipment_id,
        CONCAT_WS(
            '|',
            COALESCE(pm.building_id, ''),
            COALESCE(pm.system_id, ''),
            COALESCE(pm.equipment_type, ''),
            COALESCE(pm.equipment_id, ''),
            COALESCE(pm.sub_equipment_id, ''),
            COALESCE(pm.metric_name, '')
        ) AS point_key,
        pm.metric_name,
        rm.value,
        rm.unit,
        '' AS quality_flags,
        rm.id AS raw_id,
        rm.batch_id,
        'v2.0.0' AS mapping_version
    FROM raw_measurement rm
    JOIN point_mapping pm
        ON pm.is_active = 1
       AND pm.source_type = 'tag'
       AND rm.source_type = 'tag'
       AND pm.tag_name = rm.tag_name
    WHERE rm.id > %s AND rm.id <= %s
"""


DEVICE_INSERT_SQL = """
    INSERT INTO canonical_measurement
    (ts, building_id, system_id, equipment_type, equipment_id, sub_equipment_id,
     point_key, metric_name, value, unit, quality_flags, raw_id, batch_id, mapping_version)
    SELECT
        rm.ts,
        pm.building_id,
        pm.system_id,
        pm.equipment_type,
        pm.equipment_id,
        pm.sub_equipment_id,
        CONCAT_WS(
            '|',
            COALESCE(pm.building_id, ''),
            COALESCE(pm.system_id, ''),
            COALESCE(pm.equipment_type, ''),
            COALESCE(pm.equipment_id, ''),
            COALESCE(pm.sub_equipment_id, ''),
            COALESCE(pm.metric_name, '')
        ) AS point_key,
        pm.metric_name,
        rm.value,
        rm.unit,
        '' AS quality_flags,
        rm.id AS raw_id,
        rm.batch_id,
        'v2.0.0' AS mapping_version
    FROM raw_measurement rm
    JOIN point_mapping pm
        ON pm.is_active = 1
       AND pm.source_type = 'device'
       AND rm.source_type = 'device'
       AND pm.device_path = rm.device_path
       AND (pm.original_metric_name <=> rm.original_metric_name)
    WHERE rm.id > %s AND rm.id <= %s
"""


def build_canonical(conn: pymysql.Connection, batch_size: int = 200000) -> None:
    """Rebuild canonical_measurement in large ID chunks using SQL joins."""
    batch_size = max(10000, int(batch_size))

    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS cnt, MIN(id) AS min_id, MAX(id) AS max_id FROM raw_measurement")
        stats = cursor.fetchone()

    total_raw = int(stats["cnt"] or 0)
    min_id = stats["min_id"]
    max_id = stats["max_id"]

    if total_raw == 0 or min_id is None or max_id is None:
        LOGGER.info("  [canonical] no raw data found, skip")
        return

    LOGGER.info(
        "  [canonical] rebuilding from raw_measurement rows=%s, id_range=(%s,%s), batch_size=%s",
        total_raw,
        min_id,
        max_id,
        batch_size,
    )

    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE canonical_measurement")
    conn.commit()

    current_id = int(min_id) - 1
    total_chunks = int(math.ceil((int(max_id) - current_id) / batch_size))
    inserted_total = 0

    for chunk_idx in range(1, total_chunks + 1):
        end_id = min(current_id + batch_size, int(max_id))

        with conn.cursor() as cursor:
            cursor.execute(TAG_INSERT_SQL, (current_id, end_id))
            inserted_tag = max(cursor.rowcount, 0)

            cursor.execute(DEVICE_INSERT_SQL, (current_id, end_id))
            inserted_device = max(cursor.rowcount, 0)

        conn.commit()

        inserted_chunk = inserted_tag + inserted_device
        inserted_total += inserted_chunk

        LOGGER.info(
            "  [canonical] chunk %s/%s id=(%s,%s] inserted=%s (tag=%s, device=%s)",
            chunk_idx,
            total_chunks,
            current_id,
            end_id,
            inserted_chunk,
            inserted_tag,
            inserted_device,
        )

        current_id = end_id

    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS cnt FROM canonical_measurement")
        canonical_count = int(cursor.fetchone()["cnt"] or 0)

    LOGGER.info(
        "  [canonical] done! inserted_total=%s, canonical_measurement rows=%s",
        inserted_total,
        canonical_count,
    )
