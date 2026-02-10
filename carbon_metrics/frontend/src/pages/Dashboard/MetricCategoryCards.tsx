import { useMemo } from 'react';
import { Row, Col, Card, Typography, Space, Spin, Tag, Tooltip } from 'antd';
import { useNavigate } from 'react-router-dom';
import type { MetricResult, MetricStatus } from '../../api/types';
import { METRIC_CATEGORIES } from '../../constants/metricCategories';
import { useMetricBatchCalculate } from '../../hooks/useMetrics';

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

export default function MetricCategoryCards({ timeRange }: Props) {
  const navigate = useNavigate();

  const allMetrics = useMemo(
    () => METRIC_CATEGORIES.flatMap((cat) => cat.metrics),
    [],
  );

  const batchQuery = useMetricBatchCalculate(allMetrics, {
    time_start: timeRange[0],
    time_end: timeRange[1],
  });

  const resultMap = useMemo(() => {
    const map: Record<string, MetricCardItem> = {};
    const resultItems = batchQuery.data?.items ?? [];
    const byName = new Map(resultItems.map((item) => [item.metric_name, item]));

    allMetrics.forEach((name) => {
      map[name] = toItem(byName.get(name));
    });
    return map;
  }, [allMetrics, batchQuery.data?.items]);

  return (
    <Row gutter={[16, 16]}>
      {METRIC_CATEGORIES.map((cat) => (
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

                    {batchQuery.isLoading ? (
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
