"""Tower metrics calculator (Ground Truth)."""
from datetime import datetime
from typing import Dict, Any
import polars as pl
class TowerCalculator:
    """Calculate tower metrics from Excel data."""
    
    def __init__(self, excel_reader):
        self.reader = excel_reader
    
    def calculate_temp_diff(self, time_start: datetime, time_end: datetime) -> Dict[str, Any]:
        """冷却水温差 = AVG(return - supply) by hour."""
        supply_df = self.reader.read_metric_data("cooling_supply_temp", time_start, time_end)
        return_df = self.reader.read_metric_data("cooling_return_temp", time_start, time_end)
        
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
    
    def calculate_avg_power(self, time_start: datetime, time_end: datetime) -> Dict[str, Any]:
        """冷塔风机功率 = AVG(power)."""
        df = self.reader.read_metric_data("power", time_start, time_end)
        if df.is_empty():
            return {"value": None, "status": "no_data", "unit": "kW"}
        
        df = df.filter(pl.col("tag").str.contains("冷塔|风机"))
        if df.is_empty():
            return {"value": None, "status": "no_data", "unit": "kW"}
        
        avg = df["value"].mean()
        return {"value": round(avg, 2), "status": "success", "unit": "kW"}
    
    def calculate_efficiency(self, time_start: datetime, time_end: datetime) -> Dict[str, Any]:
        """冷塔效率 = AVG(ΔT / energy)."""
        supply_df = self.reader.read_metric_data("cooling_supply_temp", time_start, time_end)
        return_df = self.reader.read_metric_data("cooling_return_temp", time_start, time_end)
        energy_df = self.reader.read_metric_data("energy", time_start, time_end)
        
        if supply_df.is_empty() or return_df.is_empty() or energy_df.is_empty():
            return {"value": None, "status": "no_data", "unit": "°C/kWh"}
        
        energy_df = energy_df.filter(pl.col("tag").str.contains("冷塔|风机"))
        if energy_df.is_empty():
            return {"value": None, "status": "no_data", "unit": "°C/kWh"}
        
        supply_df = supply_df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
        return_df = return_df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
        energy_df = energy_df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
        
        supply_hourly = supply_df.group_by("hour").agg(pl.col("value").mean().alias("supply"))
        return_hourly = return_df.group_by("hour").agg(pl.col("value").mean().alias("return"))
        energy_hourly = energy_df.group_by("hour").agg(pl.col("value").sum().alias("energy"))
        
        joined = supply_hourly.join(return_hourly, on="hour", how="inner").join(energy_hourly, on="hour", how="inner")
        if joined.is_empty():
            return {"value": None, "status": "no_data", "unit": "°C/kWh"}
        
        joined = joined.with_columns((pl.col("return") - pl.col("supply")).alias("diff"))
        joined = joined.filter(pl.col("energy") > 0)
        if joined.is_empty():
            return {"value": None, "status": "no_data", "unit": "°C/kWh"}
        
        joined = joined.with_columns((pl.col("diff") / pl.col("energy")).alias("efficiency"))
        avg_eff = joined["efficiency"].mean()
        return {"value": round(avg_eff, 4), "status": "success", "unit": "°C/kWh"}
