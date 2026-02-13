"""
通用工具函数
"""
from __future__ import annotations

import hashlib
import logging
import re
from typing import Any, Optional

import pandas as pd


def normalize_building(value: Any) -> str:
    """标准化机楼ID"""
    if value is None:
        return "G00"
    text = str(value).strip().upper()
    if text.startswith("G") and text[1:].isdigit():
        return text
    digits = re.findall(r"\d+", text)
    if digits:
        return f"G{digits[0].zfill(2)}"
    return "G00"


def system_id_from_building(building_id: str, hint: Optional[str]) -> str:
    """从机楼ID生成系统ID"""
    if hint:
        digits = re.findall(r"\d+", str(hint))
        if digits:
            return f"{building_id}-{digits[-1]}"
    return f"{building_id}-1"


def infer_equipment_id(
    prefix: str,
    *candidates: Any,
    system_id: Optional[str] = None,
    equipment_type: Optional[str] = None,
    equipment_name: Optional[str] = None,
) -> Optional[str]:
    """推断设备ID，失败时用确定性哈希，全空则返回 None"""
    _log = logging.getLogger(__name__)

    for candidate in candidates:
        if candidate is None:
            continue
        text = str(candidate).strip()
        digits = re.findall(r"\d+", text)
        if digits:
            return f"{prefix}_{digits[0].zfill(2)}"

    # 确定性 fallback：用可用上下文生成稳定哈希
    parts = [str(v) for v in (system_id, equipment_type, equipment_name) if v]
    if not parts:
        _log.warning(
            "infer_equipment_id: 跳过，无可用上下文. prefix=%s candidates=%s",
            prefix, candidates,
        )
        return None

    key = "|".join(parts)
    suffix = hashlib.md5(key.encode("utf-8")).hexdigest()[:6]
    eq_id = f"{prefix}_h{suffix}"
    _log.info("infer_equipment_id: 确定性 fallback eq_id=%s key=%r", eq_id, key)
    return eq_id


def classify_pump_type(device_code: Any, device_name: Any = None) -> str:
    """根据编号或设备名称判断泵类型"""
    code_text = str(device_code) if device_code is not None else ""
    name_text = str(device_name) if device_name is not None else ""
    combined = code_text + name_text

    # 优先匹配特殊泵类型（顺序重要：先匹配更具体的）
    if "闭式塔" in combined or "闭式冷" in combined:
        return "closed_tower_pump"
    if "用户侧" in combined:
        return "user_side_pump"
    if "水源侧" in combined:
        return "source_side_pump"
    if "余热回收一次" in combined or "余热一次" in combined:
        return "heat_recovery_primary_pump"
    if "余热回收二次" in combined or "余热二次" in combined:
        return "heat_recovery_secondary_pump"
    if "消防" in combined:
        return "fire_pump"

    # 普通冷冻/冷却泵
    if "冷冻" in combined:
        return "chilled_pump"
    if "冷却" in combined:
        return "cooling_pump"

    # 无法识别时返回 unknown_pump
    return "unknown_pump"


def to_decimal(value: Any) -> Optional[float]:
    """转换为浮点数"""
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except Exception:
        return None


def to_date(value: Any) -> Optional[str]:
    """转换为日期字符串"""
    if value is None:
        return None
    try:
        return pd.to_datetime(value).date().isoformat()
    except Exception:
        return None
