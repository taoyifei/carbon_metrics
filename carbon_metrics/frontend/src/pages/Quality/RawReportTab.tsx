import { Table } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { RawQualityReportItem } from '../../api/types';
import { useRawQualityReport } from '../../hooks/useQuality';
import ErrorAlert from '../../components/ErrorAlert';

const columns: ColumnsType<RawQualityReportItem> = [
  {
    title: '表名',
    dataIndex: 'table',
    width: 140,
  },
  {
    title: '值列',
    dataIndex: 'value_column',
    width: 140,
  },
  {
    title: '总行数',
    dataIndex: 'total_rows',
    width: 100,
    sorter: (a, b) => a.total_rows - b.total_rows,
    render: (v: number) => v.toLocaleString(),
  },
  {
    title: '负值数',
    dataIndex: 'negative_values',
    width: 90,
    render: (v: number) => (
      <span style={v > 0 ? { color: '#ff4d4f', fontWeight: 500 } : undefined}>
        {v.toLocaleString()}
      </span>
    ),
  },
  {
    title: '缺口数',
    dataIndex: 'gap_count',
    width: 90,
    render: (v: number) => (
      <span style={v > 0 ? { color: '#faad14', fontWeight: 500 } : undefined}>
        {v.toLocaleString()}
      </span>
    ),
  },
  {
    title: '跳变数',
    dataIndex: 'jump_anomaly_count',
    width: 90,
    render: (v: number) => (
      <span style={v > 0 ? { color: '#faad14', fontWeight: 500 } : undefined}>
        {v.toLocaleString()}
      </span>
    ),
  },
  {
    title: '最大缺口(秒)',
    dataIndex: 'max_gap_seconds',
    width: 120,
    render: (v: number) => v.toLocaleString(),
  },
  {
    title: '间隔不规则率',
    dataIndex: 'interval_irregular_rate',
    width: 120,
    render: (v: number) => `${(v * 100).toFixed(1)}%`,
  },
  {
    title: '重复行',
    dataIndex: 'duplicate_rows',
    width: 90,
    render: (v: number) => v.toLocaleString(),
  },
];

export default function RawReportTab() {
  const { data, isLoading, error } = useRawQualityReport();

  if (error) return <ErrorAlert message={error.message} />;

  return (
    <Table<RawQualityReportItem>
      columns={columns}
      dataSource={data?.items}
      loading={isLoading}
      rowKey={(r) => `${r.table}-${r.value_column}`}
      pagination={false}
      scroll={{ x: 1000 }}
      size="middle"
    />
  );
}
