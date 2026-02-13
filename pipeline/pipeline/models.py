"""
数据模型定义
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class SourceConfig:
    """数据源配置"""
    id: int
    source_name: str
    directory_pattern: str
    filename_pattern: Optional[str]
    schema_type: str
    target_equipment_type: str
    target_metric_name: Optional[str]
    time_column: Optional[str]
    value_column: Optional[str]
    key_column: Optional[str]


@dataclass
class MappingResult:
    """映射结果"""
    building_id: Optional[str]
    system_id: Optional[str]
    equipment_type: Optional[str]
    equipment_id: Optional[str]
    sub_equipment_id: Optional[str]
    metric_name: Optional[str]
    confidence: str


@dataclass
class MetricDefinition:
    """指标定义数据类"""
    metric_code: str
    metric_name: str
    category_code: str
    formula: str
    required_metrics: List[str]
    applicable_levels: List[int]
    time_granularity: List[str]
    agg_method: str
    unit: Optional[str]
    baseline_value: Optional[float] = None
