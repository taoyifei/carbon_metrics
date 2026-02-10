import type { ReactNode } from 'react';
import { Alert, Card, Space, Table, Typography } from 'antd';
import type { MetricStatus, QualityIssue } from '../../api/types';

interface Props {
  status: MetricStatus;
  issues: QualityIssue[];
}

interface SimpleRow {
  key: string;
  value: string;
}

interface DependencyRow {
  key: string;
  metric_name: string;
  record_count: number | string;
}

interface MissingDiagnosticRow {
  key: string;
  metric_name: string;
  reason: string;
  agg_scope_count: number;
  agg_global_count: number;
  canonical_scope_count: number;
  canonical_global_count: number;
  raw_mapped_scope_count: number;
  mapped_point_count: number;
}

interface UnmappedTagRow {
  key: string;
  tag_name: string;
  count: number;
}

type IssueGroup = 'partial' | 'no_data';

const { Text } = Typography;

const GROUP_CONFIG: Record<IssueGroup, { title: string; subtitle: string }> = {
  partial: {
    title: 'partial 原因',
    subtitle: '指标可计算，但存在覆盖不足或质量风险',
  },
  no_data: {
    title: 'no_data 原因',
    subtitle: '指标无法计算，当前依赖数据缺失',
  },
};

const DIAGNOSTIC_REASON_LABELS: Record<string, string> = {
  present_in_scope_but_missing_dependency_flagged: '范围内有数据，但被依赖判定拦截',
  data_exists_in_db_but_filtered_out_by_scope: '库里有数据，但被当前筛选范围过滤掉',
  canonical_exists_but_not_aggregated: 'canonical 有数据，但聚合层无数据',
  raw_exists_and_mapped_but_not_canonicalized: 'raw 有数据且命中 mapping，但 canonical 无数据',
  mapping_exists_but_no_raw_in_scope: 'mapping 已配置，但当前范围 raw 无数据',
  mapping_not_hit_or_metric_unrecognized: 'mapping 未命中或口径名未识别',
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function toStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is string => typeof item === 'string');
}

function toSafeNumber(value: unknown): number {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === 'string') {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return 0;
}

function toReasonLabel(reason: unknown): string {
  if (typeof reason !== 'string' || !reason) {
    return '未知原因';
  }
  return DIAGNOSTIC_REASON_LABELS[reason] ?? reason;
}

function getIssueGroup(issue: QualityIssue, status: MetricStatus): IssueGroup {
  if (status === 'no_data') return 'no_data';
  if (issue.type === 'missing_dependency') return 'no_data';
  return 'partial';
}

function buildDependencyRows(details: Record<string, unknown>): DependencyRow[] {
  const missingMetrics = toStringArray(details.missing_metrics);
  if (!missingMetrics.length) return [];

  const metricCounts = isRecord(details.metric_counts) ? details.metric_counts : {};
  return missingMetrics.map((metricName) => {
    const rawCount = metricCounts[metricName];
    return {
      key: metricName,
      metric_name: metricName,
      record_count: typeof rawCount === 'number' ? rawCount : '-',
    };
  });
}

function buildAvailableRows(details: Record<string, unknown>): DependencyRow[] {
  const availableMetricCounts = isRecord(details.available_metric_counts)
    ? details.available_metric_counts
    : {};
  return Object.entries(availableMetricCounts)
    .map(([metricName, count]) => ({
      key: metricName,
      metric_name: metricName,
      record_count: typeof count === 'number' ? count : '-',
    }))
    .sort((a, b) => {
      const left = typeof a.record_count === 'number' ? a.record_count : -1;
      const right = typeof b.record_count === 'number' ? b.record_count : -1;
      return right - left;
    });
}

function buildMissingComponentRows(details: Record<string, unknown>): SimpleRow[] {
  const missingComponents = toStringArray(details.missing_components);
  return missingComponents.map((component) => ({ key: component, value: component }));
}

function buildMissingDiagnosticRows(details: Record<string, unknown>): MissingDiagnosticRow[] {
  const diagnostics = isRecord(details.missing_metric_diagnostics)
    ? details.missing_metric_diagnostics
    : {};

  return Object.entries(diagnostics).map(([metricName, row]) => {
    const detailRow = isRecord(row) ? row : {};
    return {
      key: metricName,
      metric_name: metricName,
      reason: toReasonLabel(detailRow.reason),
      agg_scope_count: toSafeNumber(detailRow.agg_scope_count),
      agg_global_count: toSafeNumber(detailRow.agg_global_count),
      canonical_scope_count: toSafeNumber(detailRow.canonical_scope_count),
      canonical_global_count: toSafeNumber(detailRow.canonical_global_count),
      raw_mapped_scope_count: toSafeNumber(detailRow.raw_mapped_scope_count),
      mapped_point_count: toSafeNumber(detailRow.mapped_point_count),
    };
  });
}

function buildUnmappedTagRows(details: Record<string, unknown>): UnmappedTagRow[] {
  const diagnostics = isRecord(details.missing_metric_diagnostics)
    ? details.missing_metric_diagnostics
    : {};

  const rowsByTag = new Map<string, number>();
  Object.values(diagnostics).forEach((row) => {
    if (!isRecord(row) || !Array.isArray(row.unmapped_tag_samples)) {
      return;
    }

    row.unmapped_tag_samples.forEach((sample) => {
      if (!isRecord(sample) || typeof sample.tag_name !== 'string') {
        return;
      }
      const count = toSafeNumber(sample.count);
      const old = rowsByTag.get(sample.tag_name) ?? 0;
      rowsByTag.set(sample.tag_name, Math.max(old, count));
    });
  });

  return Array.from(rowsByTag.entries())
    .map(([tagName, count]) => ({
      key: tagName,
      tag_name: tagName,
      count,
    }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 10);
}

function getIssueDetailBlock(issue: QualityIssue): ReactNode {
  const details = isRecord(issue.details) ? issue.details : undefined;
  const lines: string[] = [];

  if (typeof issue.count === 'number') {
    lines.push(`影响记录数: ${issue.count}`);
  }

  if (details) {
    const missingBucketSamples = toStringArray(details.missing_bucket_samples);
    if (missingBucketSamples.length > 0) {
      lines.push(`缺失时段样例: ${missingBucketSamples.slice(0, 5).join(' | ')}`);
    }

    const excludedTypes = toStringArray(details.excluded_equipment_types);
    if (excludedTypes.length > 0) {
      lines.push(`未纳入口径设备类型: ${excludedTypes.join(', ')}`);
    }

    const avgCompleteness = details.avg_completeness_rate;
    if (typeof avgCompleteness === 'number') {
      lines.push(`平均完整率: ${avgCompleteness}%`);
    }
  }

  const dependencyRows = details ? buildDependencyRows(details) : [];
  const availableRows = details ? buildAvailableRows(details) : [];
  const missingComponentRows = details ? buildMissingComponentRows(details) : [];
  const missingDiagnosticRows = details ? buildMissingDiagnosticRows(details) : [];
  const unmappedTagRows = details ? buildUnmappedTagRows(details) : [];

  if (
    !lines.length
    && !dependencyRows.length
    && !missingComponentRows.length
    && !availableRows.length
    && !missingDiagnosticRows.length
    && !unmappedTagRows.length
  ) {
    return undefined;
  }

  return (
    <Space direction="vertical" style={{ width: '100%' }} size={8}>
      {lines.map((line) => (
        <div key={line}>{line}</div>
      ))}

      {dependencyRows.length > 0 && (
        <div>
          <Text strong>缺失依赖指标</Text>
          <Table<DependencyRow>
            size="small"
            pagination={false}
            rowKey="key"
            style={{ marginTop: 6 }}
            columns={[
              { title: '指标', dataIndex: 'metric_name', key: 'metric_name' },
              { title: '记录数', dataIndex: 'record_count', key: 'record_count', width: 120 },
            ]}
            dataSource={dependencyRows}
          />
        </div>
      )}

      {availableRows.length > 0 && (
        <div>
          <Text strong>当前范围已有指标</Text>
          <Table<DependencyRow>
            size="small"
            pagination={false}
            rowKey="key"
            style={{ marginTop: 6 }}
            columns={[
              { title: 'metric_name', dataIndex: 'metric_name', key: 'metric_name' },
              { title: '记录数', dataIndex: 'record_count', key: 'record_count', width: 120 },
            ]}
            dataSource={availableRows}
          />
        </div>
      )}

      {missingComponentRows.length > 0 && (
        <div>
          <Text strong>缺失组件</Text>
          <Table<SimpleRow>
            size="small"
            pagination={false}
            rowKey="key"
            style={{ marginTop: 6 }}
            columns={[{ title: '组件', dataIndex: 'value', key: 'value' }]}
            dataSource={missingComponentRows}
          />
        </div>
      )}

      {missingDiagnosticRows.length > 0 && (
        <div>
          <Text strong>缺失原因诊断（DB / mapping / raw）</Text>
          <Table<MissingDiagnosticRow>
            size="small"
            pagination={false}
            rowKey="key"
            style={{ marginTop: 6 }}
            scroll={{ x: 1080 }}
            columns={[
              { title: '指标', dataIndex: 'metric_name', key: 'metric_name', width: 140 },
              { title: '诊断结论', dataIndex: 'reason', key: 'reason', width: 240 },
              { title: 'agg范围内', dataIndex: 'agg_scope_count', key: 'agg_scope_count', width: 90 },
              { title: 'agg全局', dataIndex: 'agg_global_count', key: 'agg_global_count', width: 90 },
              { title: 'canonical范围内', dataIndex: 'canonical_scope_count', key: 'canonical_scope_count', width: 120 },
              { title: 'canonical全局', dataIndex: 'canonical_global_count', key: 'canonical_global_count', width: 120 },
              { title: 'raw+mapping范围内', dataIndex: 'raw_mapped_scope_count', key: 'raw_mapped_scope_count', width: 130 },
              { title: 'mapping配置数', dataIndex: 'mapped_point_count', key: 'mapped_point_count', width: 100 },
            ]}
            dataSource={missingDiagnosticRows}
          />
        </div>
      )}

      {unmappedTagRows.length > 0 && (
        <div>
          <Text strong>未命中 mapping 的 tag 样本（Top）</Text>
          <Table<UnmappedTagRow>
            size="small"
            pagination={false}
            rowKey="key"
            style={{ marginTop: 6 }}
            columns={[
              { title: 'tag_name', dataIndex: 'tag_name', key: 'tag_name' },
              { title: 'raw记录数', dataIndex: 'count', key: 'count', width: 120 },
            ]}
            dataSource={unmappedTagRows}
          />
        </div>
      )}
    </Space>
  );
}

export default function QualityIssuesPanel({ status, issues }: Props) {
  if (!issues.length) return null;

  const groupedIssues = issues.reduce<Record<IssueGroup, QualityIssue[]>>(
    (acc, issue) => {
      const group = getIssueGroup(issue, status);
      acc[group].push(issue);
      return acc;
    },
    { partial: [], no_data: [] },
  );

  const renderOrder: IssueGroup[] =
    status === 'no_data' ? ['no_data', 'partial'] : ['partial', 'no_data'];

  return (
    <div style={{ marginTop: 16 }}>
      <Space direction="vertical" style={{ width: '100%' }}>
        {renderOrder.map((group) => {
          const groupItems = groupedIssues[group];
          if (!groupItems.length) return null;

          return (
            <Card
              key={group}
              size="small"
              title={GROUP_CONFIG[group].title}
              extra={<Text type="secondary">{GROUP_CONFIG[group].subtitle}</Text>}
            >
              <Space direction="vertical" style={{ width: '100%' }}>
                {groupItems.map((issue, i) => (
                  <Alert
                    key={`${group}-${issue.type}-${i}`}
                    type={issue.type === 'error' ? 'error' : 'warning'}
                    showIcon
                    message={issue.description}
                    description={getIssueDetailBlock(issue)}
                  />
                ))}
              </Space>
            </Card>
          );
        })}
      </Space>
    </div>
  );
}
