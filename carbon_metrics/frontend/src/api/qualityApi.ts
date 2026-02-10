import apiClient from './client';
import type {
  QualitySummary,
  QualityFilters,
  IssueFilters,
  PaginatedResponse,
  QualityRecord,
  DataIssue,
  QualityTrend,
  RawQualityReportItem,
  PaginationParams,
  Granularity,
} from './types';

export async function fetchQualitySummary(
  filters: QualityFilters,
): Promise<QualitySummary> {
  return apiClient.get('/quality/summary', { params: filters });
}

export async function fetchQualityList(
  filters: QualityFilters & PaginationParams,
): Promise<PaginatedResponse<QualityRecord>> {
  return apiClient.get('/quality/list', { params: filters });
}

export async function fetchQualityIssues(
  filters: IssueFilters & PaginationParams,
): Promise<PaginatedResponse<DataIssue>> {
  return apiClient.get('/quality/issues', { params: filters });
}

export async function fetchEquipmentTrend(
  equipment_id: string,
  time_start: string,
  time_end: string,
  metric_name?: string,
  granularity?: Granularity,
): Promise<{ equipment_id: string; trend: QualityTrend[] }> {
  return apiClient.get(
    `/quality/equipment/${encodeURIComponent(equipment_id)}/trend`,
    {
      params: { time_start, time_end, metric_name, granularity },
    },
  );
}

export async function fetchRawQualityReport(): Promise<{
  items: RawQualityReportItem[];
  total: number;
}> {
  return apiClient.get('/quality/raw-report');
}
