import { Card, Space, Tag, Typography } from 'antd';
import type { MetricStatus } from '../../api/types';
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
}

export default function MetricResultCard({
  metricName,
  value,
  unit,
  status,
  qualityScore,
}: Props) {
  const statusCfg = STATUS_CONFIG[status];

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
        <Space>
          <Tag color={statusCfg.color}>{statusCfg.label}</Tag>
          <QualityScoreTag score={qualityScore} />
        </Space>
      </Space>
    </Card>
  );
}
