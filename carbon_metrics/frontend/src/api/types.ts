// === Metrics API Types ===

export interface DataSource {
  table: string;
  field: string;
  condition: string;
  time_range: string[];
  total_records: number;
  valid_records: number;
}

export interface QualityIssue {
  type: string;
  description: string;
  count?: number;
  start?: string;
  end?: string;
  details?: Record<string, unknown>;
}

export interface MetricTrace {
  formula: string;
  formula_with_values: string;
  data_source: DataSource;
  sql: string;
}

export interface MetricBreakdown {
  equipment_type: string;
  equipment_id: string | null;
  value: number;
}

export type MetricStatus = 'success' | 'partial' | 'failed' | 'no_data';

export interface MetricResult {
  metric_name: string;
  value: number | null;
  unit: string;
  status: MetricStatus;
  quality_score: number;
  trace: MetricTrace | null;
  quality_issues: QualityIssue[];
  breakdown: MetricBreakdown[];
  computed_at: string;
}

export interface MetricListResponse {
  metrics: string[];
}

export interface MetricBatchResponse {
  items: MetricResult[];
  total: number;
}

export interface MetricCoverageItem {
  metric_name: string;
  status: MetricStatus;
  has_value: boolean;
  quality_score: number;
  issue_count: number;
  input_records: number;
  valid_records: number;
  issue_types: string[];
  missing_dependencies: string[];
}

export interface MetricCoverageSummary {
  total_metrics: number;
  calculable_count: number;
  success_count: number;
  partial_count: number;
  no_data_count: number;
  failed_count: number;
  calculable_rate: number;
}

export interface MetricCoverageOverview {
  time_start: string;
  time_end: string;
  summary: MetricCoverageSummary;
  available_metric_counts: Record<string, number>;
  missing_dependencies: string[];
  missing_dependency_counts: Record<string, number>;
  calculable_metrics: string[];
  no_data_metrics: string[];
  failed_metrics: string[];
  items: MetricCoverageItem[];
}

// === Quality API Types ===

export interface QualitySummary {
  total_records: number;
  good_count: number;
  warning_count: number;
  poor_count: number;
  avg_quality_score: number;
  avg_completeness_rate: number;
  total_gaps: number;
  total_negatives: number;
  total_jumps: number;
}

export type QualityLevel = 'good' | 'warning' | 'poor';
export type IssueType = 'gap' | 'negative' | 'jump' | 'out_of_range';
export type Severity = 'high' | 'medium' | 'low';
export type Granularity = 'hour' | 'day';

export interface QualityRecord {
  bucket_time: string;
  building_id: string;
  system_id: string;
  equipment_type: string;
  equipment_id: string | null;
  sub_equipment_id: string | null;
  metric_name: string;
  quality_score: number;
  quality_level: QualityLevel;
  completeness_rate: number;
  expected_samples: number;
  actual_samples: number;
  gap_count: number;
  max_gap_seconds: number;
  negative_count: number;
  jump_count: number;
  out_of_range_count: number;
  issues: Record<string, unknown>[];
}

export interface DataIssue {
  issue_type: IssueType;
  bucket_time: string;
  building_id: string;
  system_id: string;
  equipment_type: string;
  equipment_id: string | null;
  sub_equipment_id: string | null;
  metric_name: string;
  description: string;
  severity: Severity;
  count: number;
  details: Record<string, unknown>;
}

export interface QualityTrend {
  bucket_time: string;
  quality_score: number;
  completeness_rate: number;
  issue_count: number;
}

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
}

export interface RawQualityReportItem {
  table: string;
  time_column: string;
  value_column: string;
  key_columns: string;
  total_rows: number;
  key_count: number;
  time_start: string;
  time_end: string;
  min_value: number;
  max_value: number;
  negative_values: number;
  mode_interval_seconds: number;
  interval_irregular_rate: number;
  max_gap_seconds: number;
  gap_count: number;
  duplicate_rows: number;
  jump_anomaly_count: number;
}

// === Common Filter Types ===

export interface TimeRange {
  time_start: string;
  time_end: string;
}

export interface CommonFilters extends TimeRange {
  building_id?: string;
  system_id?: string;
  equipment_type?: string;
  equipment_id?: string;
  sub_equipment_id?: string;
}

export interface QualityFilters extends CommonFilters {
  quality_level?: QualityLevel;
  granularity?: Granularity;
}

export interface IssueFilters extends CommonFilters {
  issue_type?: IssueType;
  severity?: Severity;
  granularity?: Granularity;
}

export interface PaginationParams {
  page: number;
  page_size: number;
}
