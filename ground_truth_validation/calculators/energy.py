from datetime import datetime
from typing import Dict, Any, Optional, Set
import polars as pl
import logging

logger = logging.getLogger(__name__)
COOLING_CAPACITY_FACTOR = 1.1628
NEGATIVE_DELTA_CLAMP = -0.1
POSITIVE_DELTA_CLAMP = 1000.0

# Constants and helper from backend/metrics/energy.py, adapted for GT tags
# 实际的tag名称需要根据Excel文件内容进行调整
CHILLER_TAGS = {"冷机电量"}
PUMP_TAGS = {"冷冻泵电量", "冷却泵电量"}
TOWER_TAGS = {"冷却塔风机电量", "冷却塔电量"} # 根据实际Excel文件tag调整

COMPONENT_RULES_GT = [
    {
        "key": "chiller",
        "label": "Chiller Energy",
        "tags": CHILLER_TAGS,
    },
    {
        "key": "chilled_pump",
        "label": "Chilled Pump Energy",
        "tags": {"冷冻泵电量"},
    },
    {
        "key": "cooling_pump",
        "label": "Cooling Pump Energy",
        "tags": {"冷却泵电量"},
    },
    {
        "key": "tower",
        "label": "Tower Energy",
        "tags": TOWER_TAGS,
    },
]
STRICT_INTERSECTION_KEYS_GT = frozenset(rule["key"] for rule in COMPONENT_RULES_GT)

def _component_key_for_tag_gt(tag: str) -> Optional[str]:
    """Map a GT tag (Excel column 1) to a component key."""
    for rule in COMPONENT_RULES_GT:
        if tag in rule["tags"]:
            return rule["key"]
    return None

class EnergyCalculator:
from datetime import datetime
from typing import Dict, Any
import polars as pl
import logging

logger = logging.getLogger(__name__)
COOLING_CAPACITY_FACTOR = 1.1628
NEGATIVE_DELTA_CLAMP = -0.1
POSITIVE_DELTA_CLAMP = 1000.0


class EnergyCalculator:
    """Calculate energy metrics from Excel data."""
    
    def __init__(self, excel_reader):
        self.reader = excel_reader
    
    def calculate_total_energy(self, time_start: datetime, time_end: datetime, building_id: Optional[str] = None, system_id: Optional[str] = None) -> Dict[str, Any]:
        """系统总电量 with strict intersection (GT version)."""
        df = self.reader.read_metric_data("energy", time_start, time_end, building_id, system_id)
        if df.is_empty():
            logger.info("calculate_total_energy: No raw data found.")
            return {"value": None, "status": "no_data", "unit": "kWh"}

        # 1. 时间戳截断为小时
        df = df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))

        # 2. 过滤异常值 (NEGATIVE_DELTA_CLAMP, POSITIVE_DELTA_CLAMP)
        df = df.filter((pl.col("value") >= NEGATIVE_DELTA_CLAMP) & (pl.col("value") <= POSITIVE_DELTA_CLAMP))
        if df.is_empty():
            logger.info("calculate_total_energy: No valid data after clamping.")
            return {"value": None, "status": "no_data", "unit": "kWh"}

        # 3. 从 tag 推断 component_key
        # ExcelReader读取时会把第一列设为tag，这里根据tag内容映射到component_key
        df = df.with_columns(
            pl.col("tag").apply(_component_key_for_tag_gt).alias("component_key")
        )
        df = df.filter(pl.col("component_key").is_not_null())
        if df.is_empty():
            logger.info("calculate_total_energy: No data after component key mapping (tags not matching component rules).")
            return {"value": None, "status": "no_data", "unit": "kWh"}

        # 4. 按小时和 component_key 聚合能耗
        # 这里聚合是为了得到每个小时每个组件的总能耗，用于后续的严格交集判断
        hourly_component_energy = df.group_by(["hour", "component_key"]).agg(
            pl.col("value").sum().alias("energy")
        )
        if hourly_component_energy.is_empty():
            logger.info("calculate_total_energy: No hourly component energy after aggregation.")
            return {"value": None, "status": "no_data", "unit": "kWh"}

        # 5. 将 component_key 转换为列，方便检查小时完整性
        # pivot操作会将'component_key'的唯一值作为新的列名
        pivot_df = hourly_component_energy.pivot(
            index="hour",
            columns="component_key",
            values="energy",
            aggregate_function="first" # 每个小时每个组件只有一个值，所以取first或sum皆可
        ).fill_null(0) # 缺失的组件能耗填充0

        # 6. 识别满足严格交集条件的小时
        
        # 检查是否所有必需组件列都存在于pivot_df中
        present_required_keys = [k for k in STRICT_INTERSECTION_KEYS_GT if k in pivot_df.columns]
        
        # 如果有任何一个必需的component_key在数据中完全不存在，则直接返回no_data
        if len(present_required_keys) < len(STRICT_INTERSECTION_KEYS_GT):
            missing_keys = STRICT_INTERSECTION_KEYS_GT.difference(present_required_keys)
            logger.warning(f"calculate_total_energy: Missing some required component keys in data: {missing_keys}. Returning no_data.")
            return {"value": None, "status": "no_data", "unit": "kWh"}

        # 判断每个小时是否所有STRICT_INTERSECTION_KEYS_GT组件都有数据 (即pivot_df中对应列的值不为0)
        # 使用 all_horizontal 来检查同一行中所有指定列是否大于0 (表示有数据)
        complete_hours_mask = pl.all_horizontal([pl.col(k) > 0 for k in STRICT_INTERSECTION_KEYS_GT])
        
        # 筛选出完整小时的数据
        intersected_hourly_df = pivot_df.filter(complete_hours_mask)
        if intersected_hourly_df.is_empty():
            logger.info("calculate_total_energy: No complete hours found after strict intersection.")
            return {"value": None, "status": "no_data", "unit": "kWh"}
        
        # 7. 对所有必需组件在完整小时内的能耗进行求和
        # 对筛选后的DataFrame中所有STRICT_INTERSECTION_KEYS_GT列进行水平求和，然后对结果列求和得到最终总值
        total_energy_sum = intersected_hourly_df.select(
            pl.sum_horizontal(STRICT_INTERSECTION_KEYS_GT)
        ).sum().item() 
        
        return {"value": round(total_energy_sum, 2), "status": "success", "unit": "kWh"}
    
    def calculate_chiller_energy_ratio(self, time_start: datetime, time_end: datetime, building_id: str = "G11", system_id: str = "1") -> Dict[str, Any]:
        """冷机能耗占比 with strict intersection."""
        df = self.reader.read_metric_data("energy", time_start, time_end, building_id, system_id)
        if df.is_empty():
            return {"value": None, "status": "no_data", "unit": "%"}
        
        df = df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
        df = df.filter((pl.col("value") >= NEGATIVE_DELTA_CLAMP) & (pl.col("value") <= POSITIVE_DELTA_CLAMP))
        
        df = df.with_columns(
            pl.when(pl.col("tag").str.contains("冷机")).then(pl.lit("chiller"))
            .when(pl.col("tag").str.contains("冷冻")).then(pl.lit("chilled_pump"))
            .when(pl.col("tag").str.contains("冷却")).then(pl.lit("cooling_pump"))
            .when(pl.col("tag").str.contains("冷塔|风机")).then(pl.lit("tower"))
            .alias("eq_type")
        )
        df = df.filter(pl.col("eq_type").is_not_null())
        
        hourly = df.group_by(["hour", "eq_type"]).agg(pl.col("value").sum().alias("energy"))
        pivot = hourly.pivot(index="hour", columns="eq_type", values="energy").fill_null(0)
        
        required = ["chiller", "chilled_pump", "cooling_pump", "tower"]
        if not all(c in pivot.columns for c in required):
            return {"value": None, "status": "no_data", "unit": "%"}
        
        pivot = pivot.with_columns(pl.sum_horizontal(required).alias("total"))
        pivot = pivot.filter(pl.col("total") > 0)
        if pivot.is_empty():
            return {"value": None, "status": "no_data", "unit": "%"}
        
        chiller_sum = pivot["chiller"].sum()
        total_sum = pivot["total"].sum()
        ratio = (chiller_sum / total_sum * 100) if total_sum > 0 else None
        return {"value": round(ratio, 2) if ratio else None, "status": "success" if ratio else "no_data", "unit": "%"}
    
    def calculate_pump_energy_ratio(self, time_start: datetime, time_end: datetime, building_id: str = "G11", system_id: str = "1") -> Dict[str, Any]:
        """水泵能耗占比 with strict intersection."""
        df = self.reader.read_metric_data("energy", time_start, time_end, building_id, system_id)
        if df.is_empty():
            return {"value": None, "status": "no_data", "unit": "%"}
        
        df = df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
        df = df.filter((pl.col("value") >= NEGATIVE_DELTA_CLAMP) & (pl.col("value") <= POSITIVE_DELTA_CLAMP))
        
        df = df.with_columns(
            pl.when(pl.col("tag").str.contains("冷机")).then(pl.lit("chiller"))
            .when(pl.col("tag").str.contains("冷冻")).then(pl.lit("chilled_pump"))
            .when(pl.col("tag").str.contains("冷却")).then(pl.lit("cooling_pump"))
            .when(pl.col("tag").str.contains("冷塔|风机")).then(pl.lit("tower"))
            .alias("eq_type")
        )
        df = df.filter(pl.col("eq_type").is_not_null())
        
        hourly = df.group_by(["hour", "eq_type"]).agg(pl.col("value").sum().alias("energy"))
        pivot = hourly.pivot(index="hour", columns="eq_type", values="energy").fill_null(0)
        
        required = ["chiller", "chilled_pump", "cooling_pump", "tower"]
        if not all(c in pivot.columns for c in required):
            return {"value": None, "status": "no_data", "unit": "%"}
        
        pivot = pivot.with_columns(pl.sum_horizontal(required).alias("total"))
        pivot = pivot.filter(pl.col("total") > 0)
        if pivot.is_empty():
            return {"value": None, "status": "no_data", "unit": "%"}
        
        pump_sum = pivot["chilled_pump"].sum() + pivot["cooling_pump"].sum()
        total_sum = pivot["total"].sum()
        ratio = (pump_sum / total_sum * 100) if total_sum > 0 else None
        return {"value": round(ratio, 2) if ratio else None, "status": "success" if ratio else "no_data", "unit": "%"}
    
    def calculate_tower_energy_ratio(self, time_start: datetime, time_end: datetime, building_id: str = "G11", system_id: str = "1") -> Dict[str, Any]:
        """风机能耗占比 with strict intersection."""
        df = self.reader.read_metric_data("energy", time_start, time_end, building_id, system_id)
        if df.is_empty():
            return {"value": None, "status": "no_data", "unit": "%"}
        
        df = df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
        df = df.filter((pl.col("value") >= NEGATIVE_DELTA_CLAMP) & (pl.col("value") <= POSITIVE_DELTA_CLAMP))
        
        df = df.with_columns(
            pl.when(pl.col("tag").str.contains("冷机")).then(pl.lit("chiller"))
            .when(pl.col("tag").str.contains("冷冻")).then(pl.lit("chilled_pump"))
            .when(pl.col("tag").str.contains("冷却")).then(pl.lit("cooling_pump"))
            .when(pl.col("tag").str.contains("冷塔|风机")).then(pl.lit("tower"))
            .alias("eq_type")
        )
        df = df.filter(pl.col("eq_type").is_not_null())
        
        hourly = df.group_by(["hour", "eq_type"]).agg(pl.col("value").sum().alias("energy"))
        pivot = hourly.pivot(index="hour", columns="eq_type", values="energy").fill_null(0)
        
        required = ["chiller", "chilled_pump", "cooling_pump", "tower"]
        if not all(c in pivot.columns for c in required):
            return {"value": None, "status": "no_data", "unit": "%"}
        
        pivot = pivot.with_columns(pl.sum_horizontal(required).alias("total"))
        pivot = pivot.filter(pl.col("total") > 0)
        if pivot.is_empty():
            return {"value": None, "status": "no_data", "unit": "%"}
        
        tower_sum = pivot["tower"].sum()
        total_sum = pivot["total"].sum()
        ratio = (tower_sum / total_sum * 100) if total_sum > 0 else None
        return {"value": round(ratio, 2) if ratio else None, "status": "success" if ratio else "no_data", "unit": "%"}
