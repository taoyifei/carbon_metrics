import { useState, useEffect } from 'react';
import { useQuery, useQueries } from '@tanstack/react-query';
import {
  fetchMetricList,
  calculateMetric,
  calculateMetricBatch,
  fetchMetricCoverage,
  fetchEquipmentIds,
  fetchSubEquipmentScopes,
  type SubEquipmentScopeFilters,
} from '../api/metricsApi';
import type { CommonFilters } from '../api/types';

export function useMetricList() {
  return useQuery({
    queryKey: ['metrics', 'list'],
    queryFn: fetchMetricList,
    staleTime: 5 * 60 * 1000,
  });
}

export function useMetricCalculate(
  metric_name: string,
  filters: CommonFilters,
  enabled = true,
) {
  const [debouncedMetricName, setDebouncedMetricName] = useState(metric_name);
  const [debouncedFilters, setDebouncedFilters] = useState(filters);

  useEffect(() => {
    const timer = setTimeout(() => setDebouncedMetricName(metric_name), 800);
    return () => clearTimeout(timer);
  }, [metric_name]);

  useEffect(() => {
    const timer = setTimeout(() => setDebouncedFilters(filters), 800);
    return () => clearTimeout(timer);
  }, [JSON.stringify(filters)]);

  return useQuery({
    queryKey: ['metrics', 'calculate', debouncedMetricName, debouncedFilters],
    queryFn: () => calculateMetric(debouncedMetricName, debouncedFilters),
    enabled:
      enabled &&
      !!debouncedMetricName &&
      !!debouncedFilters.time_start &&
      !!debouncedFilters.time_end,
    staleTime: 60 * 1000,
  });
}

export function useMetricCalculateBySubScopes(
  metric_name: string,
  filters: CommonFilters,
  scopes: Array<'main' | 'backup' | '__NULL__'>,
  enabled = true,
) {
  const [debouncedMetricName, setDebouncedMetricName] = useState(metric_name);
  const [debouncedFilters, setDebouncedFilters] = useState(filters);

  useEffect(() => {
    const timer = setTimeout(() => setDebouncedMetricName(metric_name), 800);
    return () => clearTimeout(timer);
  }, [metric_name]);

  useEffect(() => {
    const timer = setTimeout(() => setDebouncedFilters(filters), 800);
    return () => clearTimeout(timer);
  }, [JSON.stringify(filters)]);

  const baseFilters = {
    ...debouncedFilters,
    sub_equipment_id: undefined,
  };
  const ready =
    enabled &&
    !!debouncedMetricName &&
    !!debouncedFilters.time_start &&
    !!debouncedFilters.time_end;
  return useQueries({
    queries: scopes.map((scope) => ({
      queryKey: [
        'metrics',
        'calculate',
        debouncedMetricName,
        { ...baseFilters, sub_equipment_id: scope },
      ],
      queryFn: () =>
        calculateMetric(debouncedMetricName, {
          ...baseFilters,
          sub_equipment_id: scope,
        }),
      enabled: ready,
      staleTime: 60 * 1000,
    })),
  });
}

export function useEquipmentIds(equipmentType?: string, enabled = true) {
  return useQuery({
    queryKey: ['equipment', 'ids', equipmentType],
    queryFn: () => fetchEquipmentIds(equipmentType),
    enabled,
    staleTime: 5 * 60 * 1000,
  });
}

export function useSubEquipmentScopes(
  filters: SubEquipmentScopeFilters,
  enabled = true,
) {
  return useQuery({
    queryKey: ['equipment', 'sub_scopes', filters],
    queryFn: () => fetchSubEquipmentScopes(filters),
    enabled,
    staleTime: 3 * 60 * 1000,
  });
}

export function useMetricBatchCalculate(
  metricNames: string[],
  filters: CommonFilters,
  enabled = true,
) {
  return useQuery({
    queryKey: ['metrics', 'calculate_batch', metricNames, filters],
    queryFn: () => calculateMetricBatch(metricNames, filters),
    enabled:
      enabled && metricNames.length > 0 && !!filters.time_start && !!filters.time_end,
    staleTime: 60 * 1000,
  });
}

export function useMetricCoverage(
  filters: CommonFilters,
  enabled = true,
) {
  const [debouncedFilters, setDebouncedFilters] = useState(filters);

  useEffect(() => {
    const timer = setTimeout(() => setDebouncedFilters(filters), 800);
    return () => clearTimeout(timer);
  }, [JSON.stringify(filters)]);

  return useQuery({
    queryKey: ['metrics', 'coverage', debouncedFilters],
    queryFn: () => fetchMetricCoverage(debouncedFilters),
    enabled: enabled && !!debouncedFilters.time_start && !!debouncedFilters.time_end,
    staleTime: 3 * 60 * 1000,
  });
}

export function useBatchMetricCalculate(
  metricNames: string[],
  filters: CommonFilters,
) {
  const enabled = !!filters.time_start && !!filters.time_end;
  return useQueries({
    queries: metricNames.map((name) => ({
      queryKey: ['metrics', 'calculate', name, filters],
      queryFn: () => calculateMetric(name, filters),
      enabled: enabled && !!name,
      staleTime: 60 * 1000,
    })),
  });
}
