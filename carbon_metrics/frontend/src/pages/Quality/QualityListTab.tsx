import { Table } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import dayjs from 'dayjs';
import type { QualityRecord, PaginatedResponse } from '../../api/types';
import { EQUIPMENT_TYPE_LABELS } from '../../constants/equipmentTypes';
import QualityBadge from '../../components/QualityBadge';
import QualityScoreTag from '../../components/QualityScoreTag';
import ErrorAlert from '../../components/ErrorAlert';

interface Props {
  data?: PaginatedResponse<QualityRecord>;
  isLoading: boolean;
  error: Error | null;
  page: number;
  pageSize: number;
  onPageChange: (page: number, pageSize: number) => void;
}

const columns: ColumnsType<QualityRecord> = [
  {
    title: '时间',
    dataIndex: 'bucket_time',
    width: 160,
    render: (v: string) => dayjs(v).format('YYYY-MM-DD HH:mm'),
  },
  {
    title: '设备类型',
    dataIndex: 'equipment_type',
    width: 120,
    render: (v: string) => EQUIPMENT_TYPE_LABELS[v] ?? v,
  },
  {
    title: '设备ID',
    dataIndex: 'equipment_id',
    width: 140,
    ellipsis: true,
  },
  {
    title: '指标',
    dataIndex: 'metric_name',
    width: 120,
  },
  {
    title: '质量分',
    dataIndex: 'quality_score',
    width: 90,
    sorter: true,
    render: (v: number) => <QualityScoreTag score={v} />,
  },
  {
    title: '等级',
    dataIndex: 'quality_level',
    width: 80,
    render: (v: QualityRecord['quality_level']) => <QualityBadge level={v} />,
  },
  {
    title: '完整率',
    dataIndex: 'completeness_rate',
    width: 90,
    render: (v: number) => `${v.toFixed(1)}%`,
  },
  {
    title: '缺口',
    dataIndex: 'gap_count',
    width: 70,
  },
  {
    title: '负值',
    dataIndex: 'negative_count',
    width: 70,
  },
  {
    title: '跳变',
    dataIndex: 'jump_count',
    width: 70,
  },
];

export default function QualityListTab({
  data,
  isLoading,
  error,
  page,
  pageSize,
  onPageChange,
}: Props) {
  if (error) return <ErrorAlert message={error.message} />;

  return (
    <Table<QualityRecord>
      columns={columns}
      dataSource={data?.items}
      loading={isLoading}
      rowKey={(r) =>
        `${r.bucket_time}-${r.building_id}-${r.system_id}-${r.equipment_type}-${r.equipment_id}-${r.sub_equipment_id}-${r.metric_name}`
      }
      pagination={{
        current: page,
        pageSize,
        total: data?.total ?? 0,
        showSizeChanger: true,
        showTotal: (total) => `共 ${total} 条`,
        onChange: onPageChange,
      }}
      scroll={{ x: 1100 }}
      size="middle"
    />
  );
}
