"""
数据导入模块
"""
from __future__ import annotations

import json
import logging
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pymysql

from .db import LOGGER
from .models import SourceConfig
from .utils import (
    normalize_building,
    system_id_from_building,
    infer_equipment_id,
    classify_pump_type,
    to_decimal,
    to_date,
)


RAW_INSERT_CHUNK_SIZE = 5000


def is_file_already_ingested(conn: pymysql.Connection, filename: str) -> bool:
    """检查文件是否已导入过（按 source_file 去重）"""
    sql = "SELECT 1 FROM raw_measurement WHERE source_file = %s LIMIT 1"
    with conn.cursor() as cursor:
        cursor.execute(sql, (filename,))
        return cursor.fetchone() is not None


def load_source_config(conn: pymysql.Connection) -> List[SourceConfig]:
    """加载数据源配置"""
    sql = """
        SELECT id, source_name, directory_pattern, filename_pattern, schema_type,
               target_equipment_type, target_metric_name, time_column, value_column, key_column
        FROM source_config
        WHERE is_active = 1
        ORDER BY priority ASC, id ASC
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
    configs: List[SourceConfig] = []
    for row in rows:
        configs.append(SourceConfig(**row))
    return configs


def list_excel_files(base_dir: Path) -> List[Path]:
    """列出目录下所有Excel文件"""
    if not base_dir.exists():
        return []
    files = []
    for path in base_dir.rglob("*.xlsx"):
        if path.name.startswith("~"):
            continue
        files.append(path)
    return files


def match_source_files(
    config: SourceConfig,
    energy_dir: Path,
    params_dir: Path,
) -> List[Path]:
    """匹配源文件"""
    if config.schema_type == "params":
        files = list_excel_files(params_dir)
        if not config.filename_pattern:
            return files
        pattern = re.compile(config.filename_pattern)
        return [f for f in files if pattern.search(f.name)]

    files = list_excel_files(energy_dir)
    pattern_dir = re.compile(config.directory_pattern)
    matched = []
    for file_path in files:
        rel = str(file_path.relative_to(energy_dir))
        if not pattern_dir.search(rel):
            continue
        if config.filename_pattern:
            if not re.search(config.filename_pattern, file_path.name):
                continue
        matched.append(file_path)
    return matched


def insert_equipment_registry(
    conn: pymysql.Connection,
    rows: List[Dict[str, Any]],
) -> int:
    """插入设备注册信息"""
    if not rows:
        return 0
    sql = """
        INSERT INTO equipment_registry
        (building_id, system_id, equipment_type, equipment_id, equipment_name,
         device_code, brand, model, serial_number, location, room,
         rated_power_kw, rated_voltage, production_date, extended_params,
         parent_equipment_id, remarks, source_file)
        VALUES
        (%(building_id)s, %(system_id)s, %(equipment_type)s, %(equipment_id)s, %(equipment_name)s,
         %(device_code)s, %(brand)s, %(model)s, %(serial_number)s, %(location)s, %(room)s,
         %(rated_power_kw)s, %(rated_voltage)s, %(production_date)s, %(extended_params)s,
         %(parent_equipment_id)s, %(remarks)s, %(source_file)s)
        ON DUPLICATE KEY UPDATE
            equipment_name = VALUES(equipment_name),
            updated_at = CURRENT_TIMESTAMP
    """
    with conn.cursor() as cursor:
        cursor.executemany(sql, rows)
    conn.commit()
    return len(rows)


def build_batch_id(prefix: str) -> str:
    """生成批次ID"""
    return f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"


def insert_ingest_batch(
    conn: pymysql.Connection,
    batch_id: str,
    source_config_id: int,
    source_directory: str,
    source_files: List[str],
) -> None:
    """插入导入批次记录"""
    sql = """
        INSERT INTO ingest_batch
        (batch_id, source_config_id, source_directory, source_files, total_files, status, started_at)
        VALUES (%s, %s, %s, %s, %s, 'running', NOW())
    """
    with conn.cursor() as cursor:
        cursor.execute(
            sql,
            (
                batch_id,
                source_config_id,
                source_directory,
                json.dumps(source_files, ensure_ascii=False),
                len(source_files),
            ),
        )
    conn.commit()


def finalize_ingest_batch(
    conn: pymysql.Connection,
    batch_id: str,
    total_rows: int,
    success_rows: int,
    error_rows: int,
    status: str,
    error_message: Optional[str] = None,
) -> None:
    """完成导入批次"""
    sql = """
        UPDATE ingest_batch
        SET total_rows = %s,
            success_rows = %s,
            error_rows = %s,
            status = %s,
            error_message = %s,
            completed_at = NOW()
        WHERE batch_id = %s
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (total_rows, success_rows, error_rows, status, error_message, batch_id))
    conn.commit()


def insert_raw_measurement(
    conn: pymysql.Connection,
    rows: List[Tuple[Any, ...]],
    chunk_size: int = RAW_INSERT_CHUNK_SIZE,
) -> int:
    """鎻掑叆鍘熷娴嬮噺鏁版嵁"""
    if not rows:
        return 0

    chunk_size = max(1000, int(chunk_size))
    sql = """
        INSERT INTO raw_measurement
        (batch_id, source_config_id, source_file, source_type, tag_name,
         device_path, location_path, original_metric_name, ts, value, unit, extra_json)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    inserted = 0
    with conn.cursor() as cursor:
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            cursor.executemany(sql, chunk)
            inserted += max(cursor.rowcount, 0)
    conn.commit()
    return inserted


def ingest_tag_file(
    file_path: Path,
    config: SourceConfig,
    conn: pymysql.Connection,
    batch_id: str,
) -> int:
    """导入标签文件"""
    df = pd.read_excel(file_path)
    df.columns = [str(c).strip() for c in df.columns]
    required = ["点名", "采集时间", "采集值"]
    if not all(col in df.columns for col in required):
        LOGGER.warning("Missing columns in %s", file_path.name)
        return 0

    df = df.rename(columns={"点名": "tag_name", "采集时间": "ts", "采集值": "value", "单位": "unit"})
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["ts", "value", "tag_name"])
    df = df.astype(object).where(pd.notnull(df), None)

    inserted_total = 0
    rows_batch: List[Tuple[Any, ...]] = []
    for row in df.itertuples(index=False):
        rows_batch.append(
            (
                batch_id,
                config.id,
                file_path.name,
                "tag",
                getattr(row, "tag_name", None),
                None,
                None,
                None,
                getattr(row, "ts", None),
                getattr(row, "value", None),
                getattr(row, "unit", None),
                None,
            )
        )

        if len(rows_batch) >= RAW_INSERT_CHUNK_SIZE:
            inserted_total += insert_raw_measurement(conn, rows_batch, chunk_size=RAW_INSERT_CHUNK_SIZE)
            rows_batch = []

    if rows_batch:
        inserted_total += insert_raw_measurement(conn, rows_batch, chunk_size=RAW_INSERT_CHUNK_SIZE)

    return inserted_total


def ingest_device_file(
    file_path: Path,
    config: SourceConfig,
    conn: pymysql.Connection,
    batch_id: str,
) -> int:
    """瀵煎叆璁惧鏂囦欢"""
    df = pd.read_excel(file_path, header=None)
    if df.shape[0] < 4:
        return 0

    location_path = str(df.iloc[0, 2])
    device_path = str(df.iloc[1, 2])
    metric_name = str(df.iloc[2, 2])

    data_df = df.iloc[3:].copy()
    data_df = data_df.iloc[:, [1, 2]]
    data_df.columns = ["ts", "value"]
    data_df = data_df.dropna(subset=["ts"])
    data_df["ts"] = pd.to_datetime(data_df["ts"], errors="coerce")
    data_df["value"] = pd.to_numeric(data_df["value"], errors="coerce")
    data_df = data_df.dropna(subset=["ts", "value"])
    data_df = data_df.astype(object).where(pd.notnull(data_df), None)

    inserted_total = 0
    rows_batch: List[Tuple[Any, ...]] = []
    for row in data_df.itertuples(index=False):
        rows_batch.append(
            (
                batch_id,
                config.id,
                file_path.name,
                "device",
                None,
                device_path,
                location_path,
                metric_name,
                getattr(row, "ts", None),
                getattr(row, "value", None),
                None,
                None,
            )
        )

        if len(rows_batch) >= RAW_INSERT_CHUNK_SIZE:
            inserted_total += insert_raw_measurement(conn, rows_batch, chunk_size=RAW_INSERT_CHUNK_SIZE)
            rows_batch = []

    if rows_batch:
        inserted_total += insert_raw_measurement(conn, rows_batch, chunk_size=RAW_INSERT_CHUNK_SIZE)

    return inserted_total


def ingest_params_file(file_path: Path, config: SourceConfig, conn: pymysql.Connection) -> int:
    """导入参数文件"""
    df = pd.read_excel(file_path)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.astype(object).where(pd.notnull(df), None)
    rows: List[Dict[str, Any]] = []

    if config.source_name == "pump_params":
        mapping = {
            "序号": "orig_id",
            "编号": "device_code",
            "设备名称": "device_name",
            "楼": "building",
            "房间号": "room",
            "位置": "location",
            "品牌": "brand",
            "型号": "model",
            "序列号": "serial_number",
            "功率（KW）": "power_kw",
            "电压(V/PH/HZ)": "voltage",
            "最大电流": "max_current",
            "电机转速（r/min）": "motor_speed_rpm",
            "生产日期": "production_date",
            "扬程（M）": "head_m",
            "流量（m³/h）": "flow_rate_m3h",
            "水泵转速（r/min）": "pump_speed_rpm",
            "备注": "remarks",
        }
        df = df.rename(columns=mapping)
        for _, row in df.iterrows():
            building_id = normalize_building(row.get("building"))
            system_id = system_id_from_building(building_id, row.get("building"))
            equipment_type = classify_pump_type(row.get("device_code"), row.get("device_name"))
            equipment_id = infer_equipment_id(
                "pump", row.get("orig_id"), row.get("device_code"),
                system_id=system_id,
                equipment_type=equipment_type,
                equipment_name=row.get("device_name"),
            )
            if equipment_id is None:
                LOGGER.warning(
                    "跳过无法识别ID的水泵行: device_name=%s file=%s",
                    row.get("device_name"), file_path.name,
                )
                continue
            ext = {
                "head_m": to_decimal(row.get("head_m")),
                "flow_rate_m3h": to_decimal(row.get("flow_rate_m3h")),
                "motor_speed_rpm": to_decimal(row.get("motor_speed_rpm")),
                "pump_speed_rpm": to_decimal(row.get("pump_speed_rpm")),
                "max_current_a": to_decimal(row.get("max_current")),
            }
            rows.append({
                "building_id": building_id,
                "system_id": system_id,
                "equipment_type": equipment_type,
                "equipment_id": equipment_id,
                "equipment_name": row.get("device_name"),
                "device_code": row.get("device_code"),
                "brand": row.get("brand"),
                "model": row.get("model"),
                "serial_number": row.get("serial_number"),
                "location": row.get("location"),
                "room": row.get("room"),
                "rated_power_kw": to_decimal(row.get("power_kw")),
                "rated_voltage": row.get("voltage"),
                "production_date": to_date(row.get("production_date")),
                "extended_params": json.dumps(ext, ensure_ascii=False),
                "parent_equipment_id": None,
                "remarks": row.get("remarks"),
                "source_file": file_path.name,
            })

    elif config.source_name == "chiller_params":
        mapping = {
            "序号": "orig_id",
            "编号": "device_code",
            "设备名称": "device_name",
            "楼": "building",
            "位置": "location",
            "品牌": "brand",
            "型号": "model",
            "序列号": "serial_number",
            "功率（KW）": "power_kw",
            "电压(V/PH/HZ)": "voltage",
            "制冷量（KW）": "cooling_capacity_kw",
            "生产日期": "production_date",
            "制冷剂/充注量（KG）": "refrigerant_charge_kg",
            "蒸发器出水口温度（℃）": "evaporator_outlet_temp_c",
            "冷凝器进回水口温度（℃）": "condenser_inlet_outlet_temp_c",
            "备注": "remarks",
        }
        df = df.rename(columns=mapping)
        for _, row in df.iterrows():
            building_id = normalize_building(row.get("building"))
            system_id = system_id_from_building(building_id, row.get("building"))
            equipment_id = infer_equipment_id(
                "chiller", row.get("orig_id"), row.get("device_code"),
                system_id=system_id,
                equipment_type="chiller",
                equipment_name=row.get("device_name"),
            )
            if equipment_id is None:
                LOGGER.warning(
                    "跳过无法识别ID的冷机行: device_name=%s file=%s",
                    row.get("device_name"), file_path.name,
                )
                continue
            ext = {
                "cooling_capacity_kw": to_decimal(row.get("cooling_capacity_kw")),
                "refrigerant_charge_kg": row.get("refrigerant_charge_kg"),
                "evaporator_outlet_temp_c": row.get("evaporator_outlet_temp_c"),
                "condenser_inlet_outlet_temp_c": row.get("condenser_inlet_outlet_temp_c"),
            }
            rows.append({
                "building_id": building_id,
                "system_id": system_id,
                "equipment_type": "chiller",
                "equipment_id": equipment_id,
                "equipment_name": row.get("device_name"),
                "device_code": row.get("device_code"),
                "brand": row.get("brand"),
                "model": row.get("model"),
                "serial_number": row.get("serial_number"),
                "location": row.get("location"),
                "room": None,
                "rated_power_kw": to_decimal(row.get("power_kw")),
                "rated_voltage": row.get("voltage"),
                "production_date": to_date(row.get("production_date")),
                "extended_params": json.dumps(ext, ensure_ascii=False),
                "parent_equipment_id": None,
                "remarks": row.get("remarks"),
                "source_file": file_path.name,
            })

    elif config.source_name == "tower_params":
        mapping = {
            "楼": "building",
            "制冷机房": "chiller_room",
            "冷塔编号": "tower_code",
            "风机数（台）": "fan_count",
            "型号": "model",
            "类别": "category",
            "冷却能力（kcal\h）": "cooling_capacity_kcal_h",
            "水处理量（m³\h）": "water_treatment_capacity_m3h",
            "填料规格（mm）": "fill_spec_mm",
            "备注": "remarks",
        }
        df = df.rename(columns=mapping)
        for _, row in df.iterrows():
            building_id = normalize_building(row.get("building"))
            system_id = system_id_from_building(building_id, row.get("chiller_room"))
            equipment_id = infer_equipment_id(
                "tower", row.get("tower_code"),
                system_id=system_id,
                equipment_type="cooling_tower",
                equipment_name=row.get("tower_code"),
            )
            if equipment_id is None:
                LOGGER.warning(
                    "跳过无法识别ID的冷却塔行: tower_code=%s file=%s",
                    row.get("tower_code"), file_path.name,
                )
                continue
            ext = {
                "fan_count": to_decimal(row.get("fan_count")),
                "cooling_capacity_kcal_h": to_decimal(row.get("cooling_capacity_kcal_h")),
                "water_treatment_capacity_m3h": to_decimal(row.get("water_treatment_capacity_m3h")),
                "fill_spec_mm": row.get("fill_spec_mm"),
                "category": row.get("category"),
            }
            rows.append({
                "building_id": building_id,
                "system_id": system_id,
                "equipment_type": "cooling_tower",
                "equipment_id": equipment_id,
                "equipment_name": row.get("tower_code"),
                "device_code": row.get("tower_code"),
                "brand": None,
                "model": row.get("model"),
                "serial_number": None,
                "location": row.get("chiller_room"),
                "room": None,
                "rated_power_kw": None,
                "rated_voltage": None,
                "production_date": None,
                "extended_params": json.dumps(ext, ensure_ascii=False),
                "parent_equipment_id": None,
                "remarks": row.get("remarks"),
                "source_file": file_path.name,
            })

    else:
        LOGGER.warning("Unknown params source: %s", config.source_name)
        return 0

    return insert_equipment_registry(conn, rows)


def ingest_sources(
    base_dir: Path,
    conn: pymysql.Connection,
    energy_dir: Optional[str] = None,
    params_dir: Optional[str] = None,
) -> None:
    """Main ingest function."""
    energy_root = Path(energy_dir) if energy_dir else (base_dir / "能耗数据反馈")
    params_root = Path(params_dir) if params_dir else (base_dir / "设备参数")
    LOGGER.info("Ingest directories: energy=%s, params=%s", energy_root, params_root)

    if not energy_root.exists():
        LOGGER.warning("Energy directory not found: %s", energy_root)
    if not params_root.exists():
        LOGGER.warning("Params directory not found: %s", params_root)

    configs = load_source_config(conn)

    for config in configs:
        files = match_source_files(config, energy_root, params_root)
        if not files:
            LOGGER.info(
                "  [ingest] source=%s schema=%s matched_files=0",
                config.source_name,
                config.schema_type,
            )
            continue

        LOGGER.info(
            "  [ingest] source=%s schema=%s matched_files=%s",
            config.source_name,
            config.schema_type,
            len(files),
        )

        batch_id = build_batch_id(config.source_name.upper())
        insert_ingest_batch(
            conn, batch_id, config.id,
            str(energy_root if config.schema_type != "params" else params_root),
            [f.name for f in files],
        )

        total_rows = 0
        success_rows = 0
        error_rows = 0
        status = "success"
        error_message = None

        for idx, file_path in enumerate(files, start=1):
            LOGGER.info(
                "  [ingest] source=%s file=%s/%s name=%s",
                config.source_name,
                idx,
                len(files),
                file_path.name,
            )
            try:
                if config.schema_type != "params" and is_file_already_ingested(conn, file_path.name):
                    LOGGER.info(
                        "  [ingest] SKIP already ingested: %s",
                        file_path.name,
                    )
                    continue

                if config.schema_type == "params":
                    inserted = ingest_params_file(file_path, config, conn)
                elif config.schema_type == "tag":
                    inserted = ingest_tag_file(file_path, config, conn, batch_id)
                elif config.schema_type == "device":
                    inserted = ingest_device_file(file_path, config, conn, batch_id)
                else:
                    inserted = 0

                total_rows += inserted
                success_rows += inserted
                LOGGER.info(
                    "  [ingest] source=%s file_done=%s/%s inserted=%s cumulative=%s",
                    config.source_name,
                    idx,
                    len(files),
                    inserted,
                    success_rows,
                )
            except Exception as exc:
                LOGGER.error("Failed processing %s: %s", file_path.name, exc)
                status = "partial"
                error_rows += 1
                error_message = str(exc)

        finalize_ingest_batch(
            conn, batch_id,
            total_rows=total_rows,
            success_rows=success_rows,
            error_rows=error_rows,
            status=status,
            error_message=error_message,
        )

        LOGGER.info(
            "  [ingest] source=%s batch=%s done status=%s total_rows=%s errors=%s",
            config.source_name,
            batch_id,
            status,
            success_rows,
            error_rows,
        )
