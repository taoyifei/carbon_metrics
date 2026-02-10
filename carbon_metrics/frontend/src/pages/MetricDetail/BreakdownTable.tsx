import { Table, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { MetricBreakdown } from '../../api/types';
import { EQUIPMENT_TYPE_LABELS } from '../../constants/equipmentTypes';

const { Title } = Typography;

interface Props {
  breakdown: MetricBreakdown[];
}

const columns: ColumnsType<MetricBreakdown> = [
  {
    title: '设备类型',
    dataIndex: 'equipment_type',
    render: (v: string) => EQUIPMENT_TYPE_LABELS[v] ?? v,
  },
  {
    title: '设备ID',
    dataIndex: 'equipment_id',
    render: (v: string | null) => v ?? '--',
  },
  {
    title: '值',
    dataIndex: 'value',
    render: (v: number) => v.toFixed(2),
  },
];

export default function BreakdownTable({ breakdown }: Props) {
  if (!breakdown.length) return null;

  return (
    <div style={{ marginTop: 16 }}>
      <Title level={5}>设备分解</Title>
      <Table<MetricBreakdown>
        columns={columns}
        dataSource={breakdown}
        rowKey={(r) => `${r.equipment_type}-${r.equipment_id}`}
        pagination={false}
        size="small"
      />
    </div>
  );
}
