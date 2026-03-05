"""Metric orchestrator - registry of all 27 metrics."""
from datetime import datetime
from typing import Dict, Any
import logging

from calculators.energy import EnergyCalculator
from calculators.temperature import TemperatureCalculator
from calculators.flow import FlowCalculator
from calculators.chiller import ChillerCalculator
from calculators.pump import PumpCalculator
from calculators.tower import TowerCalculator
from calculators.stability import StabilityCalculator
from calculators.maintenance import MaintenanceCalculator

logger = logging.getLogger(__name__)


class MetricOrchestrator:
    """Orchestrate all 27 metric calculations."""
    
    def __init__(self, excel_reader):
        self.reader = excel_reader
        self.energy = EnergyCalculator(excel_reader)
        self.temperature = TemperatureCalculator(excel_reader)
        self.flow = FlowCalculator(excel_reader)
        self.chiller = ChillerCalculator(excel_reader)
        self.pump = PumpCalculator(excel_reader)
        self.tower = TowerCalculator(excel_reader)
        self.stability = StabilityCalculator(excel_reader)
        self.maintenance = MaintenanceCalculator(excel_reader)
    
    def calculate_metric(
        self,
        metric_name: str,
        time_start: datetime,
        time_end: datetime,
        building_id: str = "G11",
        system_id: str = "1"
    ) -> Dict[str, Any]:
        """Calculate a single metric by name."""
        # Energy metrics
        if metric_name == "系统总电量":
            return self.energy.calculate_total_energy(time_start, time_end, building_id, system_id)
        
        # Temperature metrics
        if metric_name in ["冷冻供水平均温度", "冷冻回水平均温度", "冷却供水平均温度", "冷却回水平均温度"]:
            metric_map = {
                "冷冻供水平均温度": "chilled_supply_temp",
                "冷冻回水平均温度": "chilled_return_temp",
                "冷却供水平均温度": "cooling_supply_temp",
                "冷却回水平均温度": "cooling_return_temp"
            }
            return self.temperature.calculate_avg_temp(metric_map[metric_name], time_start, time_end)
        
        # Flow metrics
        if metric_name in ["冷冻水平均流量", "冷却水平均流量"]:
            metric_map = {
                "冷冻水平均流量": "chilled_flow",
                "冷却水平均流量": "cooling_flow"
            }
            return self.flow.calculate_avg_flow(metric_map[metric_name], time_start, time_end)
        
        # Placeholder for other metrics
        return {"value": None, "status": "not_implemented", "unit": ""}
