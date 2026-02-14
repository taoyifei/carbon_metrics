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

export type SubEquipmentScopeValue = 'main' | 'backup' | 'null';

export interface SubEquipmentScopeOverview {
  available_scopes: SubEquipmentScopeValue[];
  counts: Record<SubEquipmentScopeValue, number>;
}

export interface SubEquipmentScopeFilters {
  building_id?: string;
  system_id?: string;
  equipment_type?: string;
  equipment_id?: string;
}

export async function fetchSubEquipmentScopes(
  filters: SubEquipmentScopeFilters,
): Promise<SubEquipmentScopeOverview> {
  return apiClient.get('/equipment/sub-scopes', {
    params: filters,
  });
}
