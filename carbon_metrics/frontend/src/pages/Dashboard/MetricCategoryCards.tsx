import { useMemo } from 'react';
import { Row, Col, Card, Typography, Space, Spin, Tag, Tooltip } from 'antd';
import { useNavigate } from 'react-router-dom';
import { useQueries } from '@tanstack/react-query';
import type { MetricResult, MetricStatus, MetricBatchResponse } from '../../api/types';
import { calculateMetricBatch } from '../../api/metricsApi';
import { VISIBLE_METRIC_CATEGORIES } from '../../constants/metricCategories';
import { getMetricFilterConfig } from '../../constants/metricFilterConfig';

const { Text, Link } = Typography;

interface Props {
  timeRange: [string, string];
}

interface MetricCardItem {
  value: number | null;
  unit: string;
  status: MetricStatus;
  issueHint?: string;
}

const STATUS_TAG: Record<MetricStatus, { label: string; color: string }> = {
  success: { label: '正常', color: 'success' },
  partial: { label: '部分', color: 'warning' },
  no_data: { label: '无数据', color: 'default' },
  failed: { label: '失败', color: 'error' },
};

function toItem(result?: MetricResult): MetricCardItem {
  if (!result) {
    return { value: null, unit: '', status: 'no_data' };
  }
  return {
    value: result.value,
    unit: result.unit,
    status: result.status,
    issueHint: result.quality_issues?.[0]?.description,
  };
}

interface MetricGroup {
  equipmentType: string;
  metricNames: string[];
}

/**
 * Group metrics by their fixedEquipmentType so each batch call uses the
 * correct equipment_type filter — matching the Detail page's canonical scope.
 * equipmentType '' means no fixedEquipmentType (system-level metrics).
 */
function groupMetricsByEquipmentType(metrics: string[]): MetricGroup[] {
  const map = new Map<string, string[]>();
  for (const name of metrics) {
    const config = getMetricFilterConfig(name);
    const key = config.fixedEquipmentType ?? '';
    const list = map.get(key);
    if (list) {
      list.push(name);
    } else {
      map.set(key, [name]);
    }
  }
  return [...map.entries()].map(([equipmentType, metricNames]) => ({
    equipmentType,
    metricNames,
  }));
}

export default function MetricCategoryCards({ timeRange }: Props) {
  const navigate = useNavigate();

  const allMetrics = useMemo(
    () => VISIBLE_METRIC_CATEGORIES.flatMap((cat) => cat.metrics),
    [],
  );

  // Stable grouping derived from static config — won't change between renders
  const groups = useMemo(() => groupMetricsByEquipmentType(allMetrics), [allMetrics]);

  // Fire one batch query per equipment_type group using useQueries (hooks-safe)
  const groupQueries = useQueries({
    queries: groups.map(({ equipmentType, metricNames }) => ({
      queryKey: [
        'metrics',
        'calculate_batch',
        metricNames,
        {
          time_start: timeRange[0],
          time_end: timeRange[1],
          equipment_type: equipmentType || undefined,
        },
      ],
      queryFn: () =>
        calculateMetricBatch(metricNames, {
          time_start: timeRange[0],
          time_end: timeRange[1],
          ...(equipmentType ? { equipment_type: equipmentType } : {}),
        }),
      enabled: !!timeRange[0] && !!timeRange[1],
      staleTime: 60 * 1000,
    })),
  });

  const isLoading = groupQueries.some((q) => q.isLoading);

  const resultMap = useMemo(() => {
    const map: Record<string, MetricCardItem> = {};
    for (const query of groupQueries) {
      const data = query.data as MetricBatchResponse | undefined;
      for (const item of data?.items ?? []) {
        map[item.metric_name] = toItem(item);
      }
    }
    for (const name of allMetrics) {
      if (!map[name]) {
        map[name] = toItem(undefined);
      }
    }
    return map;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [allMetrics, ...groupQueries.map((q) => q.data)]);

  return (
    <Row gutter={[16, 16]}>
      {VISIBLE_METRIC_CATEGORIES.map((cat) => (
        <Col xs={24} sm={12} lg={6} key={cat.key}>
          <Card title={cat.label} size="small" style={{ height: '100%' }}>
            <Space direction="vertical" size={6} style={{ width: '100%' }}>
              {cat.metrics.map((metricName) => {
                const item = resultMap[metricName];
                const tag = STATUS_TAG[item?.status ?? 'no_data'];
                const valueText =
                  item?.value !== null && item?.value !== undefined
                    ? `${item.value.toFixed(2)} ${item.unit}`
                    : '--';

                return (
                  <div
                    key={metricName}
                    style={{
                      display: 'flex',
                      justifyContent: 'space-between',
                      alignItems: 'center',
                      gap: 8,
                    }}
                  >
                    <Link
                      onClick={() =>
                        navigate(
                          `/metrics?category=${encodeURIComponent(cat.key)}&metric=${encodeURIComponent(metricName)}&time_start=${encodeURIComponent(timeRange[0])}&time_end=${encodeURIComponent(timeRange[1])}`,
                        )
                      }
                    >
                      <Text style={{ fontSize: 13 }}>{metricName}</Text>
                    </Link>

                    {isLoading ? (
                      <Spin size="small" />
                    ) : (
                      <Space size={4}>
                        <Tooltip title={item?.issueHint}>
                          <Tag color={tag.color} style={{ marginInlineEnd: 0 }}>
                            {tag.label}
                          </Tag>
                        </Tooltip>
                        <Text type="secondary" style={{ fontSize: 12, whiteSpace: 'nowrap' }}>
                          {valueText}
                        </Text>
                      </Space>
                    )}
                  </div>
                );
              })}
            </Space>
          </Card>
        </Col>
      ))}
    </Row>
  );
}
