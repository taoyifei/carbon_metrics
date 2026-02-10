import { useState } from 'react';
import { useParams } from 'react-router-dom';
import { Card, Table, Typography, Space } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import dayjs from 'dayjs';
import type { QualityTrend, Granularity } from '../../api/types';
import { fetchEquipmentTrend } from '../../api/qualityApi';
import { useQuery } from '@tanstack/react-query';
import { useGlobalTimeRange } from '../../hooks/useGlobalTimeRange';
import TimeRangeSelector from '../../components/TimeRangeSelector';
import FilterBar from '../../components/FilterBar';
import ErrorAlert from '../../components/ErrorAlert';
import LoadingCard from '../../components/LoadingCard';
import QualityScoreTag from '../../components/QualityScoreTag';

const { Title, Text } = Typography;

const columns: ColumnsType<QualityTrend> = [
  {
    title: '时间',
    dataIndex: 'bucket_time',
    render: (v: string) => dayjs(v).format('YYYY-MM-DD HH:mm'),
  },
  {
    title: '质量分',
    dataIndex: 'quality_score',
    render: (v: number) => <QualityScoreTag score={v} />,
  },
  {
    title: '完整率',
    dataIndex: 'completeness_rate',
    render: (v: number) => `${v.toFixed(1)}%`,
  },
  {
    title: '异常数',
    dataIndex: 'issue_count',
    render: (v: number) => (
      <Text style={v > 0 ? { color: '#ff4d4f' } : undefined}>{v}</Text>
    ),
  },
];

export default function EquipmentDetail() {
  const { equipmentId = '' } = useParams<{ equipmentId: string }>();
  const [timeRange, setTimeRange] = useGlobalTimeRange(30);
  const [granularity, setGranularity] = useState<Granularity>('day');

  const { data, isLoading, error } = useQuery({
    queryKey: [
      'quality', 'equipment', equipmentId, 'trend',
      { time_start: timeRange[0], time_end: timeRange[1], granularity },
    ],
    queryFn: () =>
      fetchEquipmentTrend(
        equipmentId,
        timeRange[0],
        timeRange[1],
        undefined,
        granularity,
      ),
    enabled: !!equipmentId,
  });

  return (
    <div>
      <Title level={4}>设备: {equipmentId}</Title>

      <Card style={{ marginBottom: 16 }}>
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          <TimeRangeSelector value={timeRange} onChange={setTimeRange} />
          <FilterBar
            granularity={granularity}
            onGranularityChange={setGranularity}
            showGranularity
          />
        </Space>
      </Card>

      {error && <ErrorAlert message={(error as Error).message} />}
      {isLoading && <LoadingCard />}

      {data && (
        <Card title="质量趋势">
          <Table<QualityTrend>
            columns={columns}
            dataSource={data.trend}
            rowKey={(r) => r.bucket_time}
            pagination={{ pageSize: 20, showTotal: (t) => `共 ${t} 条` }}
            size="middle"
          />
        </Card>
      )}
    </div>
  );
}
