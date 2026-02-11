"""
Pydantic 数据模型
定义 API 请求和响应的数据结构
"""
from datetime import datetime
from typing import Optional, List, Dict, Any, Literal
from pydantic import BaseModel, Field


# ============================================================
# 指标计算相关模型
# ============================================================

class MetricRequest(BaseModel):
    """指标计算请求"""
    metric_name: str = Field(..., description="指标名称")
    time_start: datetime = Field(..., description="开始时间")
    time_end: datetime = Field(..., description="结束时间")
    equipment_type: Optional[str] = Field(None, description="设备类型筛选")
    equipment_id: Optional[str] = Field(None, description="设备ID筛选")
    sub_equipment_id: Optional[str] = Field(None, description="子设备ID筛选")


class MetricBatchRequest(BaseModel):
    """指标批量计算请求"""
    metric_names: Optional[List[str]] = Field(
        None, description="指标名称列表，为空时默认计算全部可用指标"
    )
    time_start: datetime = Field(..., description="开始时间")
    time_end: datetime = Field(..., description="结束时间")
    building_id: Optional[str] = Field(None, description="机楼筛选")
    system_id: Optional[str] = Field(None, description="系统筛选")
    equipment_type: Optional[str] = Field(None, description="设备类型筛选")
    equipment_id: Optional[str] = Field(None, description="设备ID筛选")
    sub_equipment_id: Optional[str] = Field(None, description="子设备ID筛选")


class DataSource(BaseModel):
    """数据来源信息"""
    table: str
    field: str
    condition: str
    time_range: List[str]
    total_records: int
    valid_records: int


class QualityIssue(BaseModel):
    """质量问题"""
    type: str  # gap, negative, jump, sparse
    description: str
    count: Optional[int] = None
    start: Optional[str] = None
    end: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


class MetricTrace(BaseModel):
    """计算追溯信息"""
    formula: str
    formula_with_values: str
    data_source: DataSource
    sql: str


class MetricBreakdown(BaseModel):
    """指标分解明细"""
    equipment_type: str
    equipment_id: Optional[str]
    value: float


class MetricResult(BaseModel):
    """指标计算结果"""
    metric_name: str
    value: Optional[float]
    unit: str
    status: Literal["success", "partial", "failed", "no_data"]
    quality_score: float = Field(ge=0, le=100)

    trace: Optional[MetricTrace] = None
    quality_issues: List[QualityIssue] = Field(default_factory=list)
    breakdown: List[MetricBreakdown] = Field(default_factory=list)

    computed_at: datetime = Field(default_factory=datetime.now)


class MetricBatchResponse(BaseModel):
    """指标批量计算响应"""
    items: List[MetricResult]
    total: int


class MetricCoverageItem(BaseModel):
    """指标可计算覆盖条目"""
    metric_name: str
    status: Literal["success", "partial", "failed", "no_data"]
    has_value: bool
    quality_score: float = Field(ge=0, le=100)
    issue_count: int = Field(ge=0)
    input_records: int = Field(default=0, ge=0)
    valid_records: int = Field(default=0, ge=0)
    issue_types: List[str] = Field(default_factory=list)
    missing_dependencies: List[str] = Field(default_factory=list)


class MetricCoverageSummary(BaseModel):
    """指标覆盖汇总"""
    total_metrics: int = Field(ge=0)
    calculable_count: int = Field(ge=0)
    success_count: int = Field(ge=0)
    partial_count: int = Field(ge=0)
    no_data_count: int = Field(ge=0)
    failed_count: int = Field(ge=0)
    calculable_rate: float = Field(ge=0, le=100)


class MetricCoverageOverview(BaseModel):
    """指标数据覆盖概览"""
    time_start: datetime
    time_end: datetime
    summary: MetricCoverageSummary
    available_metric_counts: Dict[str, int] = Field(default_factory=dict)
    metric_input_counts: Dict[str, int] = Field(default_factory=dict)
    missing_dependencies: List[str] = Field(default_factory=list)
    missing_dependency_counts: Dict[str, int] = Field(default_factory=dict)
    calculable_metrics: List[str] = Field(default_factory=list)
    no_data_metrics: List[str] = Field(default_factory=list)
    failed_metrics: List[str] = Field(default_factory=list)
    items: List[MetricCoverageItem] = Field(default_factory=list)


# ============================================================
# 数据质量相关模型
# ============================================================

class QualitySummary(BaseModel):
    """数据质量汇总统计"""
    total_records: int = Field(default=0, description="总记录数")
    good_count: int = Field(default=0, description="质量良好记录数")
    warning_count: int = Field(default=0, description="质量警告记录数")
    poor_count: int = Field(default=0, description="质量差记录数")
    avg_quality_score: float = Field(default=0.0, ge=0, le=100, description="平均质量评分")
    avg_completeness_rate: float = Field(default=0.0, ge=0, le=100, description="平均完整率")
    total_gaps: int = Field(default=0, description="总缺口数")
    total_negatives: int = Field(default=0, description="总负值数")
    total_jumps: int = Field(default=0, description="总跳变数")


class QualityRecord(BaseModel):
    """数据质量记录"""
    bucket_time: datetime
    building_id: str
    system_id: str
    equipment_type: str
    equipment_id: Optional[str] = None
    sub_equipment_id: Optional[str] = None
    metric_name: str
    quality_score: float = Field(ge=0, le=100)
    quality_level: Literal["good", "warning", "poor"]
    completeness_rate: float = Field(ge=0)
    expected_samples: int = Field(default=12)
    actual_samples: int = Field(default=0)
    gap_count: int = Field(default=0)
    max_gap_seconds: int = Field(default=0)
    negative_count: int = Field(default=0)
    jump_count: int = Field(default=0)
    out_of_range_count: int = Field(default=0)
    issues: List[Dict[str, Any]] = Field(default_factory=list)


class DataIssue(BaseModel):
    """数据异常问题"""
    issue_type: Literal["gap", "negative", "jump", "out_of_range"]
    bucket_time: datetime
    building_id: str
    system_id: str
    equipment_type: str
    equipment_id: Optional[str] = None
    sub_equipment_id: Optional[str] = None
    metric_name: str
    description: str
    severity: Literal["high", "medium", "low"] = "medium"
    count: int = Field(default=1)
    details: Dict[str, Any] = Field(default_factory=dict)


class QualityTrend(BaseModel):
    """设备质量趋势"""
    bucket_time: datetime
    quality_score: float
    completeness_rate: float
    issue_count: int


class PaginatedQualityResponse(BaseModel):
    """分页质量响应"""
    items: List[QualityRecord]
    total: int
    page: int = Field(ge=1)
    page_size: int = Field(ge=1, le=100)
    total_pages: int


class PaginatedIssueResponse(BaseModel):
    """分页异常响应"""
    items: List[DataIssue]
    total: int
    page: int = Field(ge=1)
    page_size: int = Field(ge=1, le=100)
    total_pages: int


class RawQualityReportItem(BaseModel):
    """原始数据质量报告条目"""
    table: str
    time_column: str
    value_column: str
    key_columns: str
    total_rows: int
    key_count: int
    time_start: str
    time_end: str
    min_value: float
    max_value: float
    negative_values: int
    mode_interval_seconds: int
    interval_irregular_rate: float
    max_gap_seconds: int
    gap_count: int
    duplicate_rows: int
    jump_anomaly_count: int
