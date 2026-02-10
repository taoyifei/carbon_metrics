import { useQuery } from '@tanstack/react-query';
import {
  fetchQualitySummary,
  fetchQualityList,
  fetchQualityIssues,
  fetchRawQualityReport,
} from '../api/qualityApi';
import type {
  QualityFilters,
  IssueFilters,
  PaginationParams,
} from '../api/types';

export function useQualitySummary(filters: QualityFilters, enabled = true) {
  return useQuery({
    queryKey: ['quality', 'summary', filters],
    queryFn: () => fetchQualitySummary(filters),
    enabled: enabled && !!filters.time_start && !!filters.time_end,
  });
}

export function useQualityList(
  filters: QualityFilters & PaginationParams,
  enabled = true,
) {
  return useQuery({
    queryKey: ['quality', 'list', filters],
    queryFn: () => fetchQualityList(filters),
    enabled: enabled && !!filters.time_start && !!filters.time_end,
  });
}

export function useQualityIssues(
  filters: IssueFilters & PaginationParams,
  enabled = true,
) {
  return useQuery({
    queryKey: ['quality', 'issues', filters],
    queryFn: () => fetchQualityIssues(filters),
    enabled: enabled && !!filters.time_start && !!filters.time_end,
  });
}

export function useRawQualityReport() {
  return useQuery({
    queryKey: ['quality', 'raw-report'],
    queryFn: fetchRawQualityReport,
    staleTime: 10 * 60 * 1000,
  });
}
