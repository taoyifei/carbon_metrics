"""
指标模块
"""
from .base import BaseMetric, MetricContext, CalculationResult
from .energy import (
    TotalEnergyMetric,
    ChillerEnergyRatioMetric,
    PumpEnergyRatioMetric,
    TowerEnergyRatioMetric,
)
from .temperature import (
    ChilledSupplyTempMetric,
    ChilledReturnTempMetric,
    CoolingSupplyTempMetric,
    CoolingReturnTempMetric,
    ChilledWaterDeltaTMetric,
)
from .flow import ChilledFlowMetric, CoolingFlowMetric, CoolingCapacityMetric
from .chiller import (
    ChillerAvgLoadMetric,
    ChillerMaxLoadMetric,
    ChillerLoadCvMetric,
    ChillerCopMetric,
    SystemCopMetric,
)
from .pump import ChilledPumpFrequencyMetric, CoolingPumpFrequencyMetric, ChilledPumpEnergyDensityMetric, CoolingPumpEnergyDensityMetric
from .tower import CoolingWaterDeltaTMetric, TowerFanPowerMetric, TowerEfficiencyMetric
from .stability import ChillerRuntimeRatioMetric, TowerFanRuntimeRatioMetric
from .maintenance import ChillerOverloadRiskMetric

__all__ = [
    "BaseMetric",
    "MetricContext",
    "CalculationResult",
    # 能耗结构
    "TotalEnergyMetric",
    "ChillerEnergyRatioMetric",
    "PumpEnergyRatioMetric",
    "TowerEnergyRatioMetric",
    # 温度
    "ChilledSupplyTempMetric",
    "ChilledReturnTempMetric",
    "CoolingSupplyTempMetric",
    "CoolingReturnTempMetric",
    "ChilledWaterDeltaTMetric",
    # 流量
    "ChilledFlowMetric",
    "CoolingFlowMetric",
    "CoolingCapacityMetric",
    # 冷机效率
    "ChillerAvgLoadMetric",
    "ChillerMaxLoadMetric",
    "ChillerLoadCvMetric",
    "ChillerCopMetric",
    "SystemCopMetric",
    # 水泵效率
    "ChilledPumpFrequencyMetric",
    "CoolingPumpFrequencyMetric",
    "ChilledPumpEnergyDensityMetric",
    "CoolingPumpEnergyDensityMetric",
    # 冷却塔效率
    "CoolingWaterDeltaTMetric",
    "TowerFanPowerMetric",
    "TowerEfficiencyMetric",
    # 运行稳定性
    "ChillerRuntimeRatioMetric",
    "TowerFanRuntimeRatioMetric",
    # 预测性维护
    "ChillerOverloadRiskMetric",
]
