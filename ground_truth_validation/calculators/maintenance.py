"""Maintenance metrics calculator (Ground Truth)."""
from datetime import datetime
from typing import Dict, Any
import polars as pl
class MaintenanceCalculator:
    """Calculate maintenance metrics from Excel data."""
    
    def __init__(self, excel_reader):
        self.reader = excel_reader
    
    def calculate_overload_risk(self, time_start: datetime, time_end: datetime) -> Dict[str, Any]:
        """过载风险指数 = AVG((load - 80) / 80) WHERE load > 80."""
        df = self.reader.read_metric_data("load_rate", time_start, time_end)
        if df.is_empty():
            return {"value": None, "status": "no_data", "unit": ""}
        
        df = df.filter(pl.col("value") > 80)
        if df.is_empty():
            return {"value": None, "status": "no_data", "unit": ""}
        
        df = df.with_columns(((pl.col("value") - 80) / 80).alias("risk"))
        avg_risk = df["risk"].mean()
        return {"value": round(avg_risk, 4), "status": "success", "unit": ""}
