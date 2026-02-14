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


def _extract_index_with_patterns(text: str, patterns: List[str]) -> Optional[str]:
    for pattern in patterns:
        idx = _extract_index(text, pattern)
        if idx:
            return idx
    return None


def _extract_chiller_no(text: str) -> Optional[str]:
    # Cover chiller id variants: 1_no, no1, 1#, and 1_prefix forms.
    patterns = [
        r"(\d+)号冷机",
        r"(\d+)#冷机",
        r"冷机(\d+)",
        r"(?:^|[.])(\d+)_冷机",
        r"(?:^|[.])(\d+)_冷机电流百分比",
        r"(?:^|[.])(\d+)_冷机累计运行时间",
    ]
    chiller_no = _extract_index_with_patterns(text, patterns)
    if chiller_no:
        return chiller_no

    for segment in reversed([seg for seg in text.split(".") if seg]):
        if "冷机" not in segment:
            continue
        m = re.match(r"(\d+)[_#号]", segment)
        if m:
            return f"{int(m.group(1)):02d}"
    return None


def _extract_pump_no(text: str) -> Optional[str]:
    patterns = [
        r"(\d+)号冷冻",
        r"(\d+)号冷却",
        r"(\d+)#冷冻",
        r"(\d+)#冷却",
        r"冷冻水泵(\d+)",
        r"冷却水泵(\d+)",
        r"(\d+)号",
    ]
    return _extract_index_with_patterns(text, patterns)


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
        pump_no = _extract_pump_no(compact)
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
        chiller_no = _extract_chiller_no(compact)
        if chiller_no:
            equipment_id = f"chiller_{chiller_no}"

    elif "运行时间" in compact or "模式时间" in compact:
        metric_name = "runtime"
        if "冷冻泵" in compact or "冷冻水泵" in compact:
            equipment_type = "chilled_pump"
            pump_no = _extract_pump_no(compact)
            if pump_no:
                equipment_id = f"pump_{pump_no}"
        elif "冷却泵" in compact or "冷却水泵" in compact:
            equipment_type = "cooling_pump"
            pump_no = _extract_pump_no(compact)
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
            chiller_no = _extract_chiller_no(compact)
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


def _normalize_device_building_id(code: str) -> str:
    """Normalize legacy device_path building tokens to canonical IDs."""
    token = str(code).strip()
    if token == "1":
        return "G11"
    if token == "2":
        return "G12"
    return f"G{token}"


def parse_device_path(
    device_path: str,
    metric_name: Optional[str],
    source_file: Optional[str] = None
) -> MappingResult:
    """Parse device_path with filename-aware correction."""
    std_metric = "unknown"
    metric_text = metric_name or ""
    if "\u7535\u5ea6" in metric_text or "\u7535\u91cf" in metric_text:
        std_metric = "energy"
    elif "\u529f\u7387" in metric_text:
        std_metric = "power"

    filename_result = MappingResult(None, None, None, None, None, None, "low")
    if source_file:
        filename_result = parse_filename(source_file)

    parsed_from_path: Optional[MappingResult] = None

    pattern1 = re.search("(\u51b7\u5374\u6c34\u6cf5|\u51b7\u51bb\u6c34\u6cf5)(\\d+)G(\\d+)_(\\d+)", device_path)
    if pattern1:
        pump_type = "cooling_pump" if "\u51b7\u5374" in pattern1.group(1) else "chilled_pump"
        pump_num = pattern1.group(2)
        building = _normalize_device_building_id(pattern1.group(3))
        system_num = pattern1.group(4)
        parsed_from_path = MappingResult(
            building,
            f"{building}-{system_num}",
            pump_type,
            f"pump_{pump_num.zfill(2)}",
            None,
            std_metric,
            "medium",
        )

    pattern2 = re.search("\u51b7\u5374\u5854G(\\d+)_(\\d+).*_(\\d+)_(\\d+)$", device_path)
    if pattern2 and parsed_from_path is None:
        building = _normalize_device_building_id(pattern2.group(1))
        system_num = pattern2.group(2)
        tower_num = pattern2.group(3)
        fan_num = pattern2.group(4)
        parsed_from_path = MappingResult(
            building,
            f"{building}-{system_num}",
            "tower_fan",
            f"tower_{tower_num.zfill(2)}",
            f"fan_{fan_num.zfill(2)}",
            std_metric,
            "medium",
        )

    pattern3 = re.search("\u51b7\u673a(\\d+)G(\\d+)_(\\d+)", device_path)
    if pattern3 and parsed_from_path is None:
        chiller_num = pattern3.group(1)
        building = _normalize_device_building_id(pattern3.group(2))
        system_num = pattern3.group(3)
        parsed_from_path = MappingResult(
            building,
            f"{building}-{system_num}",
            "chiller",
            f"chiller_{chiller_num.zfill(2)}",
            None,
            std_metric,
            "medium",
        )

    if parsed_from_path is not None:
        if filename_result.confidence == "high":
            final_metric = filename_result.metric_name or parsed_from_path.metric_name or std_metric
            return MappingResult(
                filename_result.building_id,
                filename_result.system_id,
                parsed_from_path.equipment_type,
                parsed_from_path.equipment_id,
                parsed_from_path.sub_equipment_id or filename_result.sub_equipment_id,
                final_metric,
                "high",
            )
        return parsed_from_path

    if filename_result.confidence == "high":
        final_metric = filename_result.metric_name or std_metric
        return MappingResult(
            filename_result.building_id,
            filename_result.system_id,
            filename_result.equipment_type,
            filename_result.equipment_id,
            filename_result.sub_equipment_id,
            final_metric,
            "medium",
        )

    return MappingResult(None, None, None, None, None, std_metric, "low")


def _log_chiller_core_mapping_audit(conn: pymysql.Connection) -> None:
    metrics = ("load_rate", "runtime", "power", "cooling_capacity")
    placeholders = ", ".join(["%s"] * len(metrics))

    summary_sql = f"""
        SELECT
            building_id,
            system_id,
            metric_name,
            COUNT(*) AS null_count
        FROM point_mapping
        WHERE equipment_type = 'chiller'
          AND metric_name IN ({placeholders})
          AND (equipment_id IS NULL OR equipment_id = '')
        GROUP BY building_id, system_id, metric_name
        ORDER BY null_count DESC, building_id, system_id, metric_name
        LIMIT 50
    """
    suspicious_count_sql = f"""
        SELECT COUNT(*) AS suspicious_count
        FROM point_mapping
        WHERE equipment_type = 'chiller'
          AND source_type = 'tag'
          AND metric_name IN ({placeholders})
          AND (equipment_id IS NULL OR equipment_id = '')
          AND tag_name REGEXP '[0-9]+[_#]'
    """
    suspicious_sample_sql = f"""
        SELECT
            building_id,
            system_id,
            metric_name,
            tag_name
        FROM point_mapping
        WHERE equipment_type = 'chiller'
          AND source_type = 'tag'
          AND metric_name IN ({placeholders})
          AND (equipment_id IS NULL OR equipment_id = '')
          AND tag_name REGEXP '[0-9]+[_#]'
        ORDER BY building_id, system_id, metric_name, tag_name
        LIMIT 20
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(summary_sql, list(metrics))
            summary_rows = cursor.fetchall()
            total_null = sum(int(row["null_count"] or 0) for row in summary_rows)
            if total_null > 0:
                LOGGER.warning(
                    "Chiller core mapping audit: %s rows still have NULL equipment_id.",
                    total_null,
                )
                for row in summary_rows:
                    LOGGER.warning(
                        "  %s | %s | %s -> %s",
                        row.get("building_id"),
                        row.get("system_id"),
                        row.get("metric_name"),
                        row.get("null_count"),
                    )
            else:
                LOGGER.info(
                    "Chiller core mapping audit: no NULL equipment_id rows for core metrics."
                )

            cursor.execute(suspicious_count_sql, list(metrics))
            suspicious = cursor.fetchone() or {}
            suspicious_count = int(suspicious.get("suspicious_count") or 0)
            if suspicious_count > 0:
                LOGGER.warning(
                    "Chiller core mapping audit: %s parseable-looking NULL rows found.",
                    suspicious_count,
                )
                cursor.execute(suspicious_sample_sql, list(metrics))
                for row in cursor.fetchall():
                    LOGGER.warning(
                        "  suspicious tag -> %s | %s | %s | %s",
                        row.get("building_id"),
                        row.get("system_id"),
                        row.get("metric_name"),
                        row.get("tag_name"),
                    )
    except Exception:
        LOGGER.exception("Failed to run chiller core mapping audit")


def build_point_mapping(conn: pymysql.Connection) -> None:
    """Build point mapping."""
    tag_sql = """
        SELECT
            'tag' AS source_type,
            tag_name,
            NULL AS device_path,
            original_metric_name,
            source_file
        FROM raw_measurement FORCE INDEX (idx_raw_tag_map)
        WHERE source_type = 'tag' AND tag_name IS NOT NULL
    """
    device_sql = """
        SELECT
            'device' AS source_type,
            NULL AS tag_name,
            device_path,
            original_metric_name,
            source_file
        FROM raw_measurement FORCE INDEX (idx_raw_device_map)
        WHERE source_type = 'device' AND device_path IS NOT NULL
    """

    insert_sql = """
        INSERT INTO point_mapping
        (source_type, tag_name, device_path, original_metric_name, building_id, system_id,
         equipment_type, equipment_id, sub_equipment_id, metric_name, metric_category,
         agg_method, unit, confidence)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            building_id = VALUES(building_id),
            system_id = VALUES(system_id),
            equipment_type = VALUES(equipment_type),
            equipment_id = VALUES(equipment_id),
            sub_equipment_id = VALUES(sub_equipment_id),
            metric_name = VALUES(metric_name),
            metric_category = VALUES(metric_category),
            agg_method = VALUES(agg_method),
            unit = VALUES(unit),
            confidence = VALUES(confidence),
            updated_at = CURRENT_TIMESTAMP
    """

    def _confidence_rank(confidence: str) -> int:
        return {"low": 0, "medium": 1, "high": 2}.get(str(confidence).lower(), 0)

    def _mapping_score(parsed: MappingResult) -> int:
        score = _confidence_rank(parsed.confidence) * 100
        if parsed.building_id in {"G11", "G12"}:
            score += 20
        if parsed.system_id and parsed.building_id and str(parsed.system_id).startswith(f"{parsed.building_id}-"):
            score += 10
        if parsed.equipment_id:
            score += 5
        return score

    best_rows = {}

    def _update_best_rows(point: dict) -> None:
        if point["source_type"] == "tag":
            parsed = parse_tag_name(point["tag_name"] or "")
        else:
            parsed = parse_device_path(
                point["device_path"] or "",
                point["original_metric_name"],
                point.get("source_file")
            )

        # Skip unmapped points
        if not all([parsed.building_id, parsed.system_id, parsed.equipment_type, parsed.metric_name]):
            LOGGER.warning("Skipping unmapped point: tag=%s, device=%s", point["tag_name"], point["device_path"])
            return

        mapping_key = (
            point["source_type"],
            point["tag_name"],
            point["device_path"],
            point["original_metric_name"],
        )

        candidate = {
            "point": point,
            "parsed": parsed,
            "score": _mapping_score(parsed),
            "source_file": str(point.get("source_file") or ""),
        }
        chosen = best_rows.get(mapping_key)
        if (
            chosen is None
            or candidate["score"] > chosen["score"]
            or (
                candidate["score"] == chosen["score"]
                and candidate["source_file"] < chosen["source_file"]
            )
        ):
            best_rows[mapping_key] = candidate

    seen_point_keys = set()

    def _consume_points(query: str, source_label: str, fetch_size: int = 50000) -> int:
        scanned = 0
        distinct = 0
        with conn.cursor(pymysql.cursors.SSDictCursor) as cursor:
            cursor.execute(query)
            while True:
                batch = cursor.fetchmany(fetch_size)
                if not batch:
                    break
                for point in batch:
                    scanned += 1
                    dedup_key = (
                        point.get("source_type"),
                        point.get("tag_name"),
                        point.get("device_path"),
                        point.get("original_metric_name"),
                        point.get("source_file"),
                    )
                    if dedup_key in seen_point_keys:
                        continue
                    seen_point_keys.add(dedup_key)
                    distinct += 1
                    _update_best_rows(point)
                LOGGER.info(
                    "Scanned %s %s rows, distinct candidates=%s",
                    scanned,
                    source_label,
                    distinct,
                )
        LOGGER.info(
            "Finished %s scan: scanned=%s, distinct=%s",
            source_label,
            scanned,
            distinct,
        )
        return distinct

    tag_count = _consume_points(tag_sql, "tag")
    device_count = _consume_points(device_sql, "device")
    LOGGER.info(
        "Distinct mapping candidates prepared: tag=%s, device=%s, merged=%s",
        tag_count,
        device_count,
        len(best_rows),
    )

    rows: List[Tuple[Any, ...]] = []
    for chosen in best_rows.values():
        point = chosen["point"]
        parsed = chosen["parsed"]
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

    _log_chiller_core_mapping_audit(conn)
