import { Card, Space, Tag, Tooltip, Typography } from 'antd';
import type { MetricStatus, QualityIssue } from '../../api/types';
import QualityScoreTag from '../../components/QualityScoreTag';

const { Title, Text } = Typography;

const STATUS_CONFIG: Record<MetricStatus, { label: string; color: string }> = {
  success: { label: '成功', color: 'success' },
  partial: { label: '部分', color: 'warning' },
  failed: { label: '失败', color: 'error' },
  no_data: { label: '无数据', color: 'default' },
};

interface Props {
  metricName: string;
  value: number | null;
  unit: string;
  status: MetricStatus;
  qualityScore: number;
  qualityIssues?: QualityIssue[];
}

export default function MetricResultCard({
  metricName,
  value,
  unit,
  status,
  qualityScore,
  qualityIssues,
}: Props) {
  const statusCfg = STATUS_CONFIG[status];

  const intersectionIssue = qualityIssues?.find(
    (i) => i.type === 'minimum_calculable_principle',
  );
  const details = intersectionIssue?.details;
  const intersectionHours =
    typeof details?.intersection_hours === 'number' ? details.intersection_hours : undefined;
  const expectedHours =
    typeof details?.expected_hours === 'number' ? details.expected_hours : undefined;

  return (
    <Card>
      <Space direction="vertical" size="small" style={{ width: '100%' }}>
        <Text type="secondary">{metricName}</Text>
        <Space align="baseline">
          <Title level={2} style={{ margin: 0 }}>
            {value !== null ? value.toFixed(2) : '--'}
          </Title>
          <Text type="secondary" style={{ fontSize: 16 }}>
            {unit}
          </Text>
        </Space>
        <Space wrap>
          <Tag color={statusCfg.color}>{statusCfg.label}</Tag>
          <QualityScoreTag score={qualityScore} />
          {intersectionHours != null && expectedHours != null && (
            <Tooltip title={intersectionIssue?.description}>
              <Tag color="blue">
                基于 {intersectionHours}/{expectedHours} 小时交集
              </Tag>
            </Tooltip>
          )}
        </Space>
      </Space>
    </Card>
  );
}
