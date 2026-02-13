"""
点位映射模块
"""
from __future__ import annotations

import re
from typing import Any, List, Optional, Tuple

import pymysql

from .db import LOGGER
from .models import MappingResult


# 指标元数据：(类型, 聚合方法)
METRIC_META = {
    "power": ("instant", "avg"),
    "energy": ("cumulative", "delta"),
    "chilled_supply_temp": ("instant", "avg"),
    "chilled_return_temp": ("instant", "avg"),
    "cooling_supply_temp": ("instant", "avg"),
    "cooling_return_temp": ("instant", "avg"),
    "chilled_flow": ("instant", "avg"),
    "cooling_flow": ("instant", "avg"),
    "frequency": ("instant", "avg"),
    "load_rate": ("instant", "avg"),
    "run_status": ("status", "last"),
    "runtime": ("cumulative", "delta"),
}


def _extract_building_system_from_tag(tag_name: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract building_id/system_id from tag text such as G111 or 121."""
    for seg in [s.strip() for s in tag_name.split('.') if s.strip()]:
        m = re.fullmatch(r"G(\d{3})", seg)
        if m:
            code = m.group(1)
            return f"G{code[:2]}", f"G{code[:2]}-{int(code[2])}"

        m = re.fullmatch(r"(\d{3})", seg)
        if m:
            code = m.group(1)
            return f"G{code[:2]}", f"G{code[:2]}-{int(code[2])}"

    m = re.search(r"G(\d{2})(\d)", tag_name)
    if m:
        return f"G{m.group(1)}", f"G{m.group(1)}-{int(m.group(2))}"

    m = re.search(r"(\d{2})楼", tag_name)
    if m:
        building_id = f"G{m.group(1)}"
        return building_id, f"{building_id}-1"

    return None, None


def _extract_index(text: str, pattern: str) -> Optional[str]:
    m = re.search(pattern, text)
    if not m:
        return None
    return f"{int(m.group(1)):02d}"


def parse_tag_name(tag_name: str) -> MappingResult:
    """Parse tag_name into standard hierarchy + metric_name."""
    if not tag_name:
        return MappingResult(None, None, None, None, None, None, "low")

    raw = str(tag_name).strip()
    compact = re.sub(r"\s+", "", raw)

    building_id, system_id = _extract_building_system_from_tag(compact)
    metric_name: Optional[str] = None
    equipment_type = "system"
    equipment_id: Optional[str] = None
    sub_equipment_id: Optional[str] = None
    confidence = "high"

    if "频率" in compact:
        metric_name = "frequency"
        pump_no = _extract_index(compact, r"(\d+)号")
        if "冷冻泵" in compact or "冷冻水泵" in compact:
            equipment_type = "chilled_pump"
        elif "冷却泵" in compact or "冷却水泵" in compact:
            equipment_type = "cooling_pump"
        else:
            equipment_type = "pump"
            confidence = "medium"
        if pump_no:
            equipment_id = f"pump_{pump_no}"

    elif "电流百分比" in compact or "负载率" in compact or "负荷率" in compact:
        metric_name = "load_rate"
        equipment_type = "chiller"
        chiller_no = _extract_index(compact, r"(\d+)号冷机")
        if chiller_no:
            equipment_id = f"chiller_{chiller_no}"

    elif "运行时间" in compact or "模式时间" in compact:
        metric_name = "runtime"
        if "冷冻泵" in compact or "冷冻水泵" in compact:
            equipment_type = "chilled_pump"
            pump_no = _extract_index(compact, r"(\d+)号")
            if pump_no:
                equipment_id = f"pump_{pump_no}"
        elif "冷却泵" in compact or "冷却水泵" in compact:
            equipment_type = "cooling_pump"
            pump_no = _extract_index(compact, r"(\d+)号")
            if pump_no:
                equipment_id = f"pump_{pump_no}"
        elif "冷却塔" in compact:
            equipment_type = "cooling_tower"
            tower_no = _extract_index(compact, r"冷却塔(\d+)")
            fan_no = _extract_index(compact, r"(\d+)号风机")
            if tower_no:
                equipment_id = f"tower_{tower_no}"
            if fan_no:
                sub_equipment_id = f"fan_{fan_no}"
        elif "冷机" in compact:
            equipment_type = "chiller"
            chiller_no = _extract_index(compact, r"(\d+)号冷机")
            if chiller_no:
                equipment_id = f"chiller_{chiller_no}"
        else:
            equipment_type = "system"
            confidence = "medium"

    elif "流量" in compact:
        equipment_type = "system"
        if "冷冻水" in compact:
            metric_name = "chilled_flow"
        elif "冷却水" in compact:
            metric_name = "cooling_flow"

    elif "温度" in compact:
        equipment_type = "system"
        if "冷冻水" in compact:
            if "供水" in compact:
                metric_name = "chilled_supply_temp"
            elif "回水" in compact:
                metric_name = "chilled_return_temp"
        elif "冷却水" in compact:
            # 规范：上塔=回水(热水进塔)，下塔=供水(冷水出塔)
            if "上塔" in compact or "回水" in compact or "进塔" in compact:
                metric_name = "cooling_return_temp"
            elif "下塔" in compact or "供水" in compact or "出塔" in compact:
                metric_name = "cooling_supply_temp"
            else:
                metric_name = "cooling_supply_temp"
                confidence = "medium"

    if not all([building_id, system_id, equipment_type, metric_name]):
        return MappingResult(None, None, None, None, None, None, "low")

    return MappingResult(
        building_id,
        system_id,
        equipment_type,
        equipment_id,
        sub_equipment_id,
        metric_name,
        confidence,
    )


def parse_filename(filename: str) -> MappingResult:
    """从文件名解析层级信息"""
    # 提取 metric_name (电量/功率)
    metric_name = None
    if "电量" in filename:
        metric_name = "energy"
    elif "功率" in filename:
        metric_name = "power"

    # 模式0: G12-3冷塔总用电
    pattern_tower_total = re.match(r'^G(\d+)-(\d+)冷塔总用电(主|备)', filename)
    if pattern_tower_total:
        building = f"G{pattern_tower_total.group(1)}"
        system = f"{building}-{pattern_tower_total.group(2)}"
        main_backup = "main" if pattern_tower_total.group(3) == "主" else "backup"
        return MappingResult(building, system, "cooling_tower", "tower_total", main_backup, metric_name, "high")

    # 模式1: 闭式冷塔
    pattern_closed_tower = re.match(r'^G(\d+)-(\d+)闭式冷塔(\d+)#(主|备)', filename)
    if pattern_closed_tower:
        building = f"G{pattern_closed_tower.group(1)}"
        system = f"{building}-{pattern_closed_tower.group(2)}"
        tower_num = pattern_closed_tower.group(3)
        main_backup = "main" if pattern_closed_tower.group(4) == "主" else "backup"
        return MappingResult(building, system, "cooling_tower_closed", f"tower_{tower_num.zfill(2)}", main_backup, metric_name, "high")

    # 模式2: 开式冷塔
    pattern_open_tower = re.match(r'^G(\d+)-(\d+)开式冷塔(\d+)#', filename)
    if pattern_open_tower:
        building = f"G{pattern_open_tower.group(1)}"
        system = f"{building}-{pattern_open_tower.group(2)}"
        tower_num = pattern_open_tower.group(3)
        return MappingResult(building, system, "cooling_tower", f"tower_{tower_num.zfill(2)}", None, metric_name, "high")

    # 模式3: 普通冷塔带风机
    pattern_tower_fan = re.match(r'^G(\d+)-(\d+)冷塔(\d+)#风机(\d+)-(\d+)', filename)
    if pattern_tower_fan:
        building = f"G{pattern_tower_fan.group(1)}"
        system = f"{building}-{pattern_tower_fan.group(2)}"
        tower_num = pattern_tower_fan.group(3)
        fan_start = pattern_tower_fan.group(4)
        fan_end = pattern_tower_fan.group(5)
        return MappingResult(building, system, "cooling_tower", f"tower_{tower_num.zfill(2)}", f"fan_{fan_start.zfill(2)}-{fan_end.zfill(2)}", metric_name, "high")

    # 模式3.5: 普通冷塔无风机范围
    pattern_tower_simple = re.match(r'^G(\d+)-(\d+)冷塔(\d+)#', filename)
    if pattern_tower_simple:
        building = f"G{pattern_tower_simple.group(1)}"
        system = f"{building}-{pattern_tower_simple.group(2)}"
        tower_num = pattern_tower_simple.group(3)
        return MappingResult(building, system, "cooling_tower", f"tower_{tower_num.zfill(2)}", None, metric_name, "high")

    # 模式4: 冷机带主/备
    pattern_chiller_mb = re.match(r'^G(\d+)-(\d+)冷机(\d+)#(主|备)', filename)
    if pattern_chiller_mb:
        building = f"G{pattern_chiller_mb.group(1)}"
        system = f"{building}-{pattern_chiller_mb.group(2)}"
        chiller_num = pattern_chiller_mb.group(3)
        main_backup = "main" if pattern_chiller_mb.group(4) == "主" else "backup"
        return MappingResult(building, system, "chiller", f"chiller_{chiller_num.zfill(2)}", main_backup, metric_name, "high")

    # 模式5: 普通冷机/水泵
    pattern_basic = re.match(r'^G(\d+)-(\d+)(冷机|冷冻泵|冷却泵)(\d+)#', filename)
    if pattern_basic:
        building = f"G{pattern_basic.group(1)}"
        system = f"{building}-{pattern_basic.group(2)}"
        eq_type_map = {"冷机": "chiller", "冷冻泵": "chilled_pump", "冷却泵": "cooling_pump"}
        eq_type = eq_type_map[pattern_basic.group(3)]
        eq_num = pattern_basic.group(4)
        prefix = "chiller" if eq_type == "chiller" else "pump"
        return MappingResult(building, system, eq_type, f"{prefix}_{eq_num.zfill(2)}", None, metric_name, "high")

    return MappingResult(None, None, None, None, None, None, "low")


def parse_device_path(
    device_path: str,
    metric_name: Optional[str],
    source_file: Optional[str] = None
) -> MappingResult:
    """解析设备路径"""
    std_metric = "unknown"
    metric_text = metric_name or ""
    if "电度" in metric_text or "电量" in metric_text:
        std_metric = "energy"
    elif "功率" in metric_text:
        std_metric = "power"

    pattern1 = re.search(r"(冷却水泵|冷冻水泵)(\d+)G(\d+)_(\d+)", device_path)
    if pattern1:
        pump_type = "cooling_pump" if "冷却" in pattern1.group(1) else "chilled_pump"
        pump_num = pattern1.group(2)
        building = f"G{pattern1.group(3)}"
        system_num = pattern1.group(4)
        return MappingResult(building, f"{building}-{system_num}", pump_type, f"pump_{pump_num.zfill(2)}", None, std_metric, "high")

    pattern2 = re.search(r"冷却塔G(\d+)_(\d+).*_(\d+)_(\d+)$", device_path)
    if pattern2:
        building = f"G{pattern2.group(1)}"
        system_num = pattern2.group(2)
        tower_num = pattern2.group(3)
        fan_num = pattern2.group(4)
        return MappingResult(building, f"{building}-{system_num}", "tower_fan", f"tower_{tower_num.zfill(2)}", f"fan_{fan_num.zfill(2)}", std_metric, "high")

    pattern3 = re.search(r"冷机(\d+)G(\d+)_(\d+)", device_path)
    if pattern3:
        chiller_num = pattern3.group(1)
        building = f"G{pattern3.group(2)}"
        system_num = pattern3.group(3)
        return MappingResult(building, f"{building}-{system_num}", "chiller", f"chiller_{chiller_num.zfill(2)}", None, std_metric, "high")

    # 如果 device_path 解析失败，尝试从文件名解析
    if source_file:
        filename_result = parse_filename(source_file)
        if filename_result.confidence == "high":
            final_metric = filename_result.metric_name or std_metric
            return MappingResult(
                filename_result.building_id, filename_result.system_id,
                filename_result.equipment_type, filename_result.equipment_id,
                filename_result.sub_equipment_id, final_metric, "medium"
            )

    return MappingResult(None, None, None, None, None, std_metric, "low")


def build_point_mapping(conn: pymysql.Connection) -> None:
    """构建点位映射"""
    sql = """
        SELECT source_type, tag_name, device_path, original_metric_name, source_file
        FROM raw_measurement
        GROUP BY source_type, tag_name, device_path, original_metric_name, source_file
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        points = cursor.fetchall()

    insert_sql = """
        INSERT INTO point_mapping
        (source_type, tag_name, device_path, original_metric_name, building_id, system_id,
         equipment_type, equipment_id, sub_equipment_id, metric_name, metric_category,
         agg_method, unit, confidence)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            building_id = VALUES(building_id),
            system_id = VALUES(system_id),
            metric_name = VALUES(metric_name),
            confidence = VALUES(confidence),
            updated_at = CURRENT_TIMESTAMP
    """

    rows: List[Tuple[Any, ...]] = []
    for point in points:
        if point["source_type"] == "tag":
            parsed = parse_tag_name(point["tag_name"] or "")
        else:
            parsed = parse_device_path(
                point["device_path"] or "",
                point["original_metric_name"],
                point.get("source_file")
            )

        # 跳过无法解析的点位
        if not all([parsed.building_id, parsed.system_id, parsed.equipment_type, parsed.metric_name]):
            LOGGER.warning("Skipping unmapped point: tag=%s, device=%s", point["tag_name"], point["device_path"])
            continue

        metric_category, agg_method = METRIC_META.get(parsed.metric_name or "", ("instant", "avg"))
        rows.append((
            point["source_type"],
            point["tag_name"],
            point["device_path"],
            point["original_metric_name"],
            parsed.building_id,
            parsed.system_id,
            parsed.equipment_type,
            parsed.equipment_id,
            parsed.sub_equipment_id,
            parsed.metric_name,
            metric_category,
            agg_method,
            None,
            parsed.confidence,
        ))

    if rows:
        with conn.cursor() as cursor:
            cursor.executemany(insert_sql, rows)
        conn.commit()
