"""Stability metrics calculator (Ground Truth)."""
from datetime import datetime
from typing import Dict, Any
import polars as pl
class StabilityCalculator:
    """Calculate stability metrics from Excel data."""
    
    def __init__(self, excel_reader):
        self.reader = excel_reader
    
    def calculate_runtime_ratio(self, equipment_type: str, time_start: datetime, time_end: datetime) -> Dict[str, Any]:
        """运行时长占比 = SUM(runtime) / COUNT × 100%."""
        df = self.reader.read_metric_data("runtime", time_start, time_end)
        if df.is_empty():
            return {"value": None, "status": "no_data", "unit": "%"}
        
        df = df.filter(pl.col("tag").str.contains(equipment_type))
        if df.is_empty():
            return {"value": None, "status": "no_data", "unit": "%"}
        
        total_runtime = df["value"].sum()
        record_count = len(df)
        
        if record_count == 0:
            return {"value": None, "status": "no_data", "unit": "%"}
        
        ratio = (total_runtime / record_count) * 100
        return {"value": round(ratio, 2), "status": "success", "unit": "%"}
