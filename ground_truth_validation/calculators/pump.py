"""Pump metrics calculator (Ground Truth)."""
from datetime import datetime
from typing import Dict, Any
import polars as pl
class PumpCalculator:
    """Calculate pump metrics from Excel data."""
    
    def __init__(self, excel_reader):
        self.reader = excel_reader
    
    def calculate_avg_frequency(self, pump_type: str, time_start: datetime, time_end: datetime) -> Dict[str, Any]:
        """Average pump frequency."""
        df = self.reader.read_metric_data("frequency", time_start, time_end)
        if df.is_empty():
            return {"value": None, "status": "no_data", "unit": "Hz"}
        
        df = df.filter(pl.col("tag").str.contains(pump_type))
        if df.is_empty():
            return {"value": None, "status": "no_data", "unit": "Hz"}
        
        avg = df["value"].mean()
        return {"value": round(avg, 2), "status": "success", "unit": "Hz"}
    
    def calculate_energy_density(self, pump_type: str, flow_metric: str, time_start: datetime, time_end: datetime) -> Dict[str, Any]:
        """Pump energy density = SUM(energy) / SUM(flow)."""
        energy_df = self.reader.read_metric_data("energy", time_start, time_end)
        flow_df = self.reader.read_metric_data(flow_metric, time_start, time_end)
        
        if energy_df.is_empty() or flow_df.is_empty():
            return {"value": None, "status": "no_data", "unit": "kWh/m³"}
        
        energy_df = energy_df.filter(pl.col("tag").str.contains(pump_type))
        if energy_df.is_empty():
            return {"value": None, "status": "no_data", "unit": "kWh/m³"}
        
        total_energy = energy_df["value"].sum()
        total_flow = flow_df["value"].sum()
        
        if total_flow == 0:
            return {"value": None, "status": "no_data", "unit": "kWh/m³"}
        
        density = total_energy / total_flow
        return {"value": round(density, 4), "status": "success", "unit": "kWh/m³"}
