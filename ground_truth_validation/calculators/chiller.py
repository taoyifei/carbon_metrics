"""Chiller metrics calculator (Ground Truth)."""
from datetime import datetime
from typing import Dict, Any
import polars as pl
COOLING_CAPACITY_FACTOR = 1.1628
MIN_POWER_KW = 20
class ChillerCalculator:
    """Calculate chiller metrics from Excel data."""
    
    def __init__(self, excel_reader):
        self.reader = excel_reader
    
    def calculate_avg_load_rate(self, time_start: datetime, time_end: datetime) -> Dict[str, Any]:
        """冷机平均负载率 = AVG(load_rate)."""
        df = self.reader.read_metric_data("load_rate", time_start, time_end)
        if df.is_empty():
            return {"value": None, "status": "no_data", "unit": "%"}
        avg = df["value"].mean()
        return {"value": round(avg, 2), "status": "success", "unit": "%"}
    
    def calculate_max_load_rate(self, time_start: datetime, time_end: datetime) -> Dict[str, Any]:
        """冷机最大负载率 = MAX(load_rate)."""
        df = self.reader.read_metric_data("load_rate", time_start, time_end)
        if df.is_empty():
            return {"value": None, "status": "no_data", "unit": "%"}
        max_val = df["value"].max()
        return {"value": round(max_val, 2), "status": "success", "unit": "%"}
    
    def calculate_load_fluctuation(self, time_start: datetime, time_end: datetime) -> Dict[str, Any]:
        """冷机负载波动系数 = STDDEV/AVG."""
        df = self.reader.read_metric_data("load_rate", time_start, time_end)
        if df.is_empty():
            return {"value": None, "status": "no_data", "unit": ""}
        avg = df["value"].mean()
        std = df["value"].std()
        if avg == 0:
            return {"value": None, "status": "no_data", "unit": ""}
        coef = std / avg
        return {"value": round(coef, 4), "status": "success", "unit": ""}
    
    def calculate_cop(self, time_start: datetime, time_end: datetime) -> Dict[str, Any]:
        """冷机COP = SUM(cooling_capacity) / SUM(power) with min power filter."""
        flow_df = self.reader.read_metric_data("chilled_flow", time_start, time_end)
        supply_df = self.reader.read_metric_data("chilled_supply_temp", time_start, time_end)
        return_df = self.reader.read_metric_data("chilled_return_temp", time_start, time_end)
        power_df = self.reader.read_metric_data("power", time_start, time_end)
        
        if flow_df.is_empty() or supply_df.is_empty() or return_df.is_empty() or power_df.is_empty():
            return {"value": None, "status": "no_data", "unit": ""}
        
        flow_df = flow_df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
        supply_df = supply_df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
        return_df = return_df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
        power_df = power_df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
        
        flow_hourly = flow_df.group_by("hour").agg(pl.col("value").mean().alias("flow"))
        supply_hourly = supply_df.group_by("hour").agg(pl.col("value").mean().alias("supply"))
        return_hourly = return_df.group_by("hour").agg(pl.col("value").mean().alias("return"))
        power_hourly = power_df.group_by("hour").agg(pl.col("value").mean().alias("power"))
        
        joined = flow_hourly.join(supply_hourly, on="hour", how="inner").join(return_hourly, on="hour", how="inner").join(power_hourly, on="hour", how="inner")
        if joined.is_empty():
            return {"value": None, "status": "no_data", "unit": ""}
        
        joined = joined.with_columns((pl.col("flow") * (pl.col("return") - pl.col("supply")) * COOLING_CAPACITY_FACTOR).alias("capacity"))
        joined = joined.filter(pl.col("power") >= MIN_POWER_KW)
        if joined.is_empty():
            return {"value": None, "status": "no_data", "unit": ""}
        
        total_capacity = joined["capacity"].sum()
        total_power = joined["power"].sum()
        cop = total_capacity / total_power if total_power > 0 else None
        return {"value": round(cop, 2) if cop else None, "status": "success" if cop else "no_data", "unit": ""}
    
    def calculate_system_cop(self, time_start: datetime, time_end: datetime) -> Dict[str, Any]:
        """制冷系统COP = SUM(cooling_capacity) / SUM(total_power)."""
        flow_df = self.reader.read_metric_data("chilled_flow", time_start, time_end)
        supply_df = self.reader.read_metric_data("chilled_supply_temp", time_start, time_end)
        return_df = self.reader.read_metric_data("chilled_return_temp", time_start, time_end)
        power_df = self.reader.read_metric_data("power", time_start, time_end)
        
        if flow_df.is_empty() or supply_df.is_empty() or return_df.is_empty() or power_df.is_empty():
            return {"value": None, "status": "no_data", "unit": ""}
        
        flow_df = flow_df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
        supply_df = supply_df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
        return_df = return_df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
        power_df = power_df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
        
        flow_hourly = flow_df.group_by("hour").agg(pl.col("value").mean().alias("flow"))
        supply_hourly = supply_df.group_by("hour").agg(pl.col("value").mean().alias("supply"))
        return_hourly = return_df.group_by("hour").agg(pl.col("value").mean().alias("return"))
        power_hourly = power_df.group_by("hour").agg(pl.col("value").sum().alias("power"))
        
        joined = flow_hourly.join(supply_hourly, on="hour", how="inner").join(return_hourly, on="hour", how="inner").join(power_hourly, on="hour", how="inner")
        if joined.is_empty():
            return {"value": None, "status": "no_data", "unit": ""}
        
        joined = joined.with_columns((pl.col("flow") * (pl.col("return") - pl.col("supply")) * COOLING_CAPACITY_FACTOR).alias("capacity"))
        total_capacity = joined["capacity"].sum()
        total_power = joined["power"].sum()
        cop = total_capacity / total_power if total_power > 0 else None
        return {"value": round(cop, 2) if cop else None, "status": "success" if cop else "no_data", "unit": ""}
