import { useState, type ReactNode } from 'react';
import { Alert, Card, Space, Switch, Table, Typography } from 'antd';
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

interface MissingBucketRow {
  key: string;
  metric_name: string;
  bucket_time: string;
  expected_samples: number;
  actual_samples: number;
  completeness_rate: number;
}

interface MissingBucketDeviceRow {
  key: string;
  metric_name: string;
  bucket_time: string;
  building_id: string;
  system_id: string;
  equipment_type: string;
  equipment_id: string;
  sub_equipment_id: string;
  expected_samples: number;
  actual_samples: number;
  completeness_rate: number;
}

interface SevereNegativeTypeRow {
  key: string;
  equipment_type: string;
  count: number;
  total: number;
}

interface SensorBiasRow {
  key: string;
  point: string;
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

function buildMissingBucketRows(details: Record<string, unknown>): MissingBucketRow[] {
  const raw = details.missing_bucket_samples;
  if (!Array.isArray(raw)) {
    return [];
  }

  return raw
    .map((item, index) => {
      if (!isRecord(item)) {
        return null;
      }
      return {
        key: `${String(item.metric_name ?? '')}-${String(item.bucket_time ?? '')}-${index}`,
        metric_name: String(item.metric_name ?? ''),
        bucket_time: String(item.bucket_time ?? ''),
        expected_samples: toSafeNumber(item.expected_samples),
        actual_samples: toSafeNumber(item.actual_samples),
        completeness_rate: toSafeNumber(item.completeness_rate),
      };
    })
    .filter((item): item is MissingBucketRow => item !== null);
}

function buildMissingBucketDeviceRows(details: Record<string, unknown>): MissingBucketDeviceRow[] {
  const raw = details.missing_bucket_device_samples;
  if (!Array.isArray(raw)) {
    return [];
  }

  return raw
    .map((item, index) => {
      if (!isRecord(item)) {
        return null;
      }
      return {
        key: `${String(item.metric_name ?? '')}-${String(item.bucket_time ?? '')}-${String(item.equipment_id ?? '')}-${index}`,
        metric_name: String(item.metric_name ?? ''),
        bucket_time: String(item.bucket_time ?? ''),
        building_id: String(item.building_id ?? ''),
        system_id: String(item.system_id ?? ''),
        equipment_type: String(item.equipment_type ?? ''),
        equipment_id: String(item.equipment_id ?? ''),
        sub_equipment_id: String(item.sub_equipment_id ?? ''),
        expected_samples: toSafeNumber(item.expected_samples),
        actual_samples: toSafeNumber(item.actual_samples),
        completeness_rate: toSafeNumber(item.completeness_rate),
      };
    })
    .filter((item): item is MissingBucketDeviceRow => item !== null);
}

function buildSevereNegativeTypeRows(details: Record<string, unknown>): SevereNegativeTypeRow[] {
  const raw = details.severe_negative_by_type;
  if (!Array.isArray(raw)) {
    return [];
  }
  return raw
    .map((item, index) => {
      if (!isRecord(item)) {
        return null;
      }
      return {
        key: `${String(item.equipment_type ?? '')}-${index}`,
        equipment_type: String(item.equipment_type ?? ''),
        count: toSafeNumber(item.count),
        total: toSafeNumber(item.total),
      };
    })
    .filter((item): item is SevereNegativeTypeRow => item !== null);
}

function buildSensorBiasRows(details: Record<string, unknown>): SensorBiasRow[] {
  const raw = details.sensor_bias_points;
  if (!Array.isArray(raw)) {
    return [];
  }
  return raw
    .map((item, index) => {
      if (!isRecord(item)) {
        return null;
      }
      const devicePath = String(item.device_path ?? '').trim();
      if (!devicePath) {
        return null;
      }
      const negativeCount = toSafeNumber(item.negative_count);
      const totalCount = toSafeNumber(item.total_count);
      const ratio = toSafeNumber(item.negative_ratio);
      const minValue = toSafeNumber(item.min_value);
      const summary = `${devicePath}（负值 ${negativeCount}/${totalCount}, ${ratio.toFixed(2)}%, min=${minValue.toFixed(2)}）`;
      return {
        key: `${devicePath}-${index}`,
        point: summary,
      };
    })
    .filter((item): item is SensorBiasRow => item !== null);
}

function IssueDetailBlock({ issue }: { issue: QualityIssue }): ReactNode {
  const [showDeviceDetails, setShowDeviceDetails] = useState(false);
  const details = isRecord(issue.details) ? issue.details : undefined;
  const lines: string[] = [];

  if (typeof issue.count === 'number') {
    lines.push(`影响记录数: ${issue.count}`);
  }

  if (details) {
    const excludedTypes = toStringArray(details.excluded_equipment_types);
    if (excludedTypes.length > 0) {
      lines.push(`未纳入口径设备类型: ${excludedTypes.join(', ')}`);
    }

    const avgCompleteness = details.avg_completeness_rate;
    if (typeof avgCompleteness === 'number') {
      lines.push(`平均完整率: ${avgCompleteness}%`);
    }

    if (typeof details.incomplete_bucket_count === 'number') {
      lines.push(`缺失时段条数: ${details.incomplete_bucket_count}`);
    }

    if (typeof details.clamp_threshold === 'number') {
      lines.push(`负值归零阈值: ${details.clamp_threshold}`);
    }

    if (typeof details.clamped_negative_total === 'number') {
      lines.push(`已归零负值合计: ${details.clamped_negative_total}`);
    }

    if (typeof details.severe_negative_total === 'number') {
      lines.push(`超阈值负值合计(已从SUM剔除): ${details.severe_negative_total}`);
    }

    if (typeof details.filtered_negative_total === 'number') {
      lines.push(`已过滤负值合计: ${details.filtered_negative_total}（结果为净化口径）`);
    }

    if (typeof details.policy === 'string' && details.policy.trim()) {
      lines.push(`处理规则: ${details.policy}`);
    }

    if (typeof details.min_negative_count === 'number') {
      lines.push(`黑名单告警最小负值条数: ${details.min_negative_count}`);
    }
  }

  const dependencyRows = details ? buildDependencyRows(details) : [];
  const availableRows = details ? buildAvailableRows(details) : [];
  const missingComponentRows = details ? buildMissingComponentRows(details) : [];
  const missingDiagnosticRows = details ? buildMissingDiagnosticRows(details) : [];
  const unmappedTagRows = details ? buildUnmappedTagRows(details) : [];
  const missingBucketRows = details ? buildMissingBucketRows(details) : [];
  const missingBucketDeviceRows = details ? buildMissingBucketDeviceRows(details) : [];
  const severeNegativeTypeRows = details ? buildSevereNegativeTypeRows(details) : [];
  const sensorBiasRows = details ? buildSensorBiasRows(details) : [];
  const hasMissingBucketRows = missingBucketRows.length > 0 || missingBucketDeviceRows.length > 0;
  const useDeviceBucketTable =
    missingBucketRows.length === 0
    || (showDeviceDetails && missingBucketDeviceRows.length > 0);

  if (
    !lines.length
    && !dependencyRows.length
    && !missingComponentRows.length
    && !availableRows.length
    && !missingDiagnosticRows.length
    && !unmappedTagRows.length
    && !hasMissingBucketRows
    && !severeNegativeTypeRows.length
    && !sensorBiasRows.length
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

      {hasMissingBucketRows && (
        <div>
          <Space
            align="center"
            style={{ width: '100%', justifyContent: 'space-between' }}
            wrap
          >
            <Text strong>缺失时段样例（默认按小时聚合）</Text>
            {missingBucketRows.length > 0 && missingBucketDeviceRows.length > 0 && (
              <Space size={6}>
                <Text type="secondary">查看设备明细</Text>
                <Switch
                  size="small"
                  checked={showDeviceDetails}
                  onChange={setShowDeviceDetails}
                />
              </Space>
            )}
          </Space>

          {!useDeviceBucketTable ? (
            <Table<MissingBucketRow>
              size="small"
              pagination={false}
              rowKey="key"
              style={{ marginTop: 6 }}
              scroll={{ x: 900 }}
              columns={[
                { title: 'metric_name', dataIndex: 'metric_name', key: 'metric_name', width: 180 },
                { title: 'bucket_time', dataIndex: 'bucket_time', key: 'bucket_time', width: 180 },
                { title: 'actual', dataIndex: 'actual_samples', key: 'actual_samples', width: 80 },
                { title: 'expected', dataIndex: 'expected_samples', key: 'expected_samples', width: 90 },
                {
                  title: 'completeness',
                  dataIndex: 'completeness_rate',
                  key: 'completeness_rate',
                  width: 110,
                  render: (value: number) => `${value.toFixed(1)}%`,
                },
              ]}
              dataSource={missingBucketRows}
            />
          ) : (
            <Table<MissingBucketDeviceRow>
              size="small"
              pagination={false}
              rowKey="key"
              style={{ marginTop: 6 }}
              scroll={{ x: 1560 }}
              columns={[
                { title: 'metric_name', dataIndex: 'metric_name', key: 'metric_name', width: 160 },
                { title: 'bucket_time', dataIndex: 'bucket_time', key: 'bucket_time', width: 180 },
                { title: 'building_id', dataIndex: 'building_id', key: 'building_id', width: 90 },
                { title: 'system_id', dataIndex: 'system_id', key: 'system_id', width: 90 },
                { title: 'equipment_type', dataIndex: 'equipment_type', key: 'equipment_type', width: 150 },
                { title: 'equipment_id', dataIndex: 'equipment_id', key: 'equipment_id', width: 120 },
                {
                  title: 'sub_equipment_id',
                  dataIndex: 'sub_equipment_id',
                  key: 'sub_equipment_id',
                  width: 140,
                },
                { title: 'actual', dataIndex: 'actual_samples', key: 'actual_samples', width: 80 },
                { title: 'expected', dataIndex: 'expected_samples', key: 'expected_samples', width: 90 },
                {
                  title: 'completeness',
                  dataIndex: 'completeness_rate',
                  key: 'completeness_rate',
                  width: 110,
                  render: (value: number) => `${value.toFixed(1)}%`,
                },
              ]}
              dataSource={missingBucketDeviceRows}
            />
          )}
        </div>
      )}

      {severeNegativeTypeRows.length > 0 && (
        <div>
          <Text strong>超阈值负值分布（Top）</Text>
          <Table<SevereNegativeTypeRow>
            size="small"
            pagination={false}
            rowKey="key"
            style={{ marginTop: 6 }}
            columns={[
              { title: 'equipment_type', dataIndex: 'equipment_type', key: 'equipment_type' },
              { title: 'count', dataIndex: 'count', key: 'count', width: 90 },
              { title: 'total', dataIndex: 'total', key: 'total', width: 120 },
            ]}
            dataSource={severeNegativeTypeRows}
          />
        </div>
      )}

      {sensorBiasRows.length > 0 && (
        <div>
          <Text strong>疑似传感器偏置点位</Text>
          <Table<SensorBiasRow>
            size="small"
            pagination={false}
            rowKey="key"
            style={{ marginTop: 6 }}
            columns={[{ title: '疑似传感器偏置点位', dataIndex: 'point', key: 'point' }]}
            dataSource={sensorBiasRows}
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
                    description={<IssueDetailBlock issue={issue} />}
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
