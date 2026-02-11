import { useState } from 'react';
import dayjs from 'dayjs';
import { Alert, Divider, Space, Tag, Typography } from 'antd';
import type { AlertProps } from 'antd';
import type { MetricCoverageItem, MetricCoverageOverview } from '../api/types';

const { Text, Link } = Typography;

interface Props {
  data?: MetricCoverageOverview;
  isLoading?: boolean;
  errorMessage?: string;
}

const MAX_ITEMS = 30;

type CoverageStatus = 'success' | 'partial' | 'no_data' | 'failed';

const STATUS_TAG_COLOR: Record<CoverageStatus, string> = {
  success: 'success',
  partial: 'warning',
  no_data: 'default',
  failed: 'error',
};

const STATUS_TAG_LABEL: Record<CoverageStatus, string> = {
  success: '成功',
  partial: '部分可算',
  no_data: '无数据',
  failed: '失败',
};

function formatRange(start: string, end: string): string {
  return `${dayjs(start).format('MM-DD HH:mm')} ~ ${dayjs(end).format('MM-DD HH:mm')}`;
}

function topEntries(record: Record<string, number>, limit = MAX_ITEMS): [string, number][] {
  return Object.entries(record)
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit);
}

function toMetricNames(items: MetricCoverageItem[], status: CoverageStatus): string[] {
  return items
    .filter((item) => item.status === status)
    .map((item) => item.metric_name)
    .slice(0, MAX_ITEMS);
}

function resolveAlertType(overview: MetricCoverageOverview): AlertProps['type'] {
  const { failed_count, no_data_count, partial_count } = overview.summary;
  if (failed_count > 0) {
    return 'error';
  }
  if (no_data_count > 0 || partial_count > 0) {
    return 'warning';
  }
  return 'success';
}

function renderTagList(names: string[], color: string) {
  if (names.length === 0) {
    return <Text type="secondary">无</Text>;
  }
  return (
    <Space wrap size={[6, 6]}>
      {names.map((name) => (
        <Tag key={name} color={color}>
          {name}
        </Tag>
      ))}
    </Space>
  );
}

export default function DataCoverageBanner({
  data,
  isLoading = false,
  errorMessage,
}: Props) {
  const [expanded, setExpanded] = useState(false);

  if (errorMessage) {
    return (
      <Alert
        type="warning"
        showIcon
        style={{ marginBottom: 12 }}
        message="数据覆盖概览获取失败"
        description={errorMessage}
      />
    );
  }

  if (isLoading) {
    return (
      <Alert
        type="info"
        showIcon
        style={{ marginBottom: 12 }}
        message="正在评估当前时间范围可计算指标..."
      />
    );
  }

  if (!data) {
    return null;
  }

  const { summary } = data;
  const availableEntries = topEntries(data.available_metric_counts);
  const metricInputEntries = topEntries(data.metric_input_counts ?? {});
  const dependencyEntries = topEntries(data.missing_dependency_counts);

  const partialMetricNames = toMetricNames(data.items, 'partial');
  const noDataMetricNames = toMetricNames(data.items, 'no_data');
  const failedMetricNames = toMetricNames(data.items, 'failed');
  const lowSampleMetricNames = data.items
    .filter(
      (item) =>
        item.status !== 'failed'
        && item.status !== 'no_data'
        && item.valid_records <= 1,
    )
    .map((item) => item.metric_name)
    .slice(0, MAX_ITEMS);

  return (
    <Alert
      type={resolveAlertType(data)}
      showIcon
      style={{ marginBottom: 12 }}
      message={(
        <Space wrap size={[8, 8]}>
          <Text strong>
            {`当前范围 (${formatRange(data.time_start, data.time_end)}) 可计算 ${summary.calculable_count}/${summary.total_metrics} 个指标 (${summary.calculable_rate}%)`}
          </Text>
          <Tag color={STATUS_TAG_COLOR.success}>
            {`${STATUS_TAG_LABEL.success} ${summary.success_count}`}
          </Tag>
          <Tag color={STATUS_TAG_COLOR.partial}>
            {`${STATUS_TAG_LABEL.partial} ${summary.partial_count}`}
          </Tag>
          <Tag color={STATUS_TAG_COLOR.no_data}>
            {`${STATUS_TAG_LABEL.no_data} ${summary.no_data_count}`}
          </Tag>
          <Tag color={STATUS_TAG_COLOR.failed}>
            {`${STATUS_TAG_LABEL.failed} ${summary.failed_count}`}
          </Tag>
          <Link onClick={() => setExpanded((v) => !v)}>
            {expanded ? '收起详情' : '展开详情'}
          </Link>
        </Space>
      )}
      description={
        expanded ? (
          <Space direction="vertical" size={10} style={{ width: '100%' }}>
            <Text type="secondary">
              {`重点关注: 部分可算 ${summary.partial_count}，无数据 ${summary.no_data_count}，失败 ${summary.failed_count}`}
            </Text>

            <Divider style={{ margin: '2px 0 0' }} />
            <Text strong>受影响指标（Top）</Text>
            <Text type="secondary">部分可算</Text>
            {renderTagList(partialMetricNames, STATUS_TAG_COLOR.partial)}
            <Text type="secondary">无数据</Text>
            {renderTagList(noDataMetricNames, STATUS_TAG_COLOR.no_data)}
            <Text type="secondary">失败</Text>
            {renderTagList(failedMetricNames, STATUS_TAG_COLOR.failed)}

            <Divider style={{ margin: '2px 0 0' }} />
            <Text strong>缺失依赖（Top）</Text>
            {dependencyEntries.length > 0 ? (
              <Space wrap size={[6, 6]}>
                {dependencyEntries.map(([name, count]) => (
                  <Tag key={name} color="orange">
                    {`${name}: ${count}`}
                  </Tag>
                ))}
              </Space>
            ) : (
              <Text type="secondary">无</Text>
            )}

            <Divider style={{ margin: '2px 0 0' }} />
            <Text strong>原子指标覆盖（Top）</Text>
            {availableEntries.length > 0 ? (
              <Space wrap size={[6, 6]}>
                {availableEntries.map(([name, count]) => (
                  <Tag key={name}>
                    {`${name}: ${count}`}
                  </Tag>
                ))}
              </Space>
            ) : (
              <Text type="secondary">无</Text>
            )}

            <Divider style={{ margin: '2px 0 0' }} />
            <Text strong>业务指标输入记录（Top）</Text>
            {metricInputEntries.length > 0 ? (
              <Space wrap size={[6, 6]}>
                {metricInputEntries.map(([name, count]) => (
                  <Tag key={name}>
                    {`${name}: ${count}`}
                  </Tag>
                ))}
              </Space>
            ) : (
              <Text type="secondary">无</Text>
            )}

            <Divider style={{ margin: '2px 0 0' }} />
            <Text strong>低样本风险指标（valid_records ≤ 1）</Text>
            {renderTagList(lowSampleMetricNames, 'gold')}
          </Space>
        ) : undefined
      }
    />
  );
}
