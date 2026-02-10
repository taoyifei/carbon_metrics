import apiClient from './client';
import type {
  MetricBatchResponse,
  MetricListResponse,
  MetricResult,
  MetricCoverageOverview,
  CommonFilters,
} from './types';

export async function fetchMetricList(): Promise<MetricListResponse> {
  return apiClient.get('/metrics/list');
}

export async function calculateMetric(
  metric_name: string,
  filters: CommonFilters,
): Promise<MetricResult> {
  return apiClient.get('/metrics/calculate', {
    params: { metric_name, ...filters },
  });
}

export async function calculateMetricBatch(
  metric_names: string[],
  filters: CommonFilters,
): Promise<MetricBatchResponse> {
  return apiClient.post('/metrics/calculate_batch', {
    metric_names,
    ...filters,
  });
}

export async function fetchMetricCoverage(
  filters: CommonFilters,
): Promise<MetricCoverageOverview> {
  return apiClient.get('/metrics/coverage', {
    params: filters,
  });
}

export interface EquipmentItem {
  equipment_id: string;
  equipment_type: string;
}

export async function fetchEquipmentIds(
  equipment_type?: string,
): Promise<{ items: EquipmentItem[]; total: number }> {
  return apiClient.get('/equipment/ids', {
    params: equipment_type ? { equipment_type } : {},
  });
}
