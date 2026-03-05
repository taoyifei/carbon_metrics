"""Temperature metrics calculator (Ground Truth)."""
from datetime import datetime
from typing import Dict, Any
import polars as pl

class TemperatureCalculator:
    """Calculate temperature metrics from Excel data."""
    
    def __init__(self, excel_reader):
        self.reader = excel_reader
    
    def calculate_avg_temp(self, metric_name: str, time_start: datetime, time_end: datetime) -> Dict[str, Any]:
        """Generic average temperature calculation."""
        df = self.reader.read_metric_data(metric_name, time_start, time_end)
        if df.is_empty():
            return {"value": None, "status": "no_data", "unit": "°C"}
        avg = df["value"].mean()
        return {"value": round(avg, 2), "status": "success", "unit": "°C"}
    
    def calculate_temp_diff(self, supply_metric: str, return_metric: str, time_start: datetime, time_end: datetime) -> Dict[str, Any]:
        """Temperature difference with hourly intersection."""
        supply_df = self.reader.read_metric_data(supply_metric, time_start, time_end)
        return_df = self.reader.read_metric_data(return_metric, time_start, time_end)
        
        if supply_df.is_empty() or return_df.is_empty():
            return {"value": None, "status": "no_data", "unit": "°C"}
        
        supply_df = supply_df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
        return_df = return_df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
        
        supply_hourly = supply_df.group_by("hour").agg(pl.col("value").mean().alias("supply"))
        return_hourly = return_df.group_by("hour").agg(pl.col("value").mean().alias("return"))
        
        joined = supply_hourly.join(return_hourly, on="hour", how="inner")
        if joined.is_empty():
            return {"value": None, "status": "no_data", "unit": "°C"}
        
        joined = joined.with_columns((pl.col("return") - pl.col("supply")).alias("diff"))
        avg_diff = joined["diff"].mean()
        return {"value": round(avg_diff, 2), "status": "success", "unit": "°C"}
