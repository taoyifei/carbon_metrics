"""Flow metrics calculator (Ground Truth)."""
from datetime import datetime
from typing import Dict, Any
import polars as pl
COOLING_CAPACITY_FACTOR = 1.1628
class FlowCalculator:
    """Calculate flow metrics from Excel data."""
    
    def __init__(self, excel_reader):
        self.reader = excel_reader
    
    def calculate_avg_flow(self, metric_name: str, time_start: datetime, time_end: datetime) -> Dict[str, Any]:
        """Generic average flow calculation."""
        df = self.reader.read_metric_data(metric_name, time_start, time_end)
        if df.is_empty():
            return {"value": None, "status": "no_data", "unit": "m³/h"}
        avg = df["value"].mean()
        return {"value": round(avg, 2), "status": "success", "unit": "m³/h"}
    
    def calculate_cooling_capacity(self, time_start: datetime, time_end: datetime) -> Dict[str, Any]:
        """制冷量 = AVG(flow × ΔT × 1.1628) with hourly intersection."""
        flow_df = self.reader.read_metric_data("chilled_flow", time_start, time_end)
        supply_df = self.reader.read_metric_data("chilled_supply_temp", time_start, time_end)
        return_df = self.reader.read_metric_data("chilled_return_temp", time_start, time_end)
        
        if flow_df.is_empty() or supply_df.is_empty() or return_df.is_empty():
            return {"value": None, "status": "no_data", "unit": "kW"}
        
        flow_df = flow_df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
        supply_df = supply_df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
        return_df = return_df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
        
        flow_hourly = flow_df.group_by("hour").agg(pl.col("value").mean().alias("flow"))
        supply_hourly = supply_df.group_by("hour").agg(pl.col("value").mean().alias("supply"))
        return_hourly = return_df.group_by("hour").agg(pl.col("value").mean().alias("return"))
        
        joined = flow_hourly.join(supply_hourly, on="hour", how="inner").join(return_hourly, on="hour", how="inner")
        if joined.is_empty():
            return {"value": None, "status": "no_data", "unit": "kW"}
        
        joined = joined.with_columns((pl.col("flow") * (pl.col("return") - pl.col("supply")) * COOLING_CAPACITY_FACTOR).alias("capacity"))
        avg_capacity = joined["capacity"].mean()
        return {"value": round(avg_capacity, 2), "status": "success", "unit": "kW"}
