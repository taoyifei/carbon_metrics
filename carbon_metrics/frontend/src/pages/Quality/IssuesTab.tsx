import { Table, Select, Space } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import dayjs from 'dayjs';
import type {
  DataIssue,
  PaginatedResponse,
  IssueType,
  Severity,
} from '../../api/types';
import { EQUIPMENT_TYPE_LABELS } from '../../constants/equipmentTypes';
import IssueTypeTag from '../../components/IssueTypeTag';
import SeverityTag from '../../components/SeverityTag';
import ErrorAlert from '../../components/ErrorAlert';

interface Props {
  data?: PaginatedResponse<DataIssue>;
  isLoading: boolean;
  error: Error | null;
  page: number;
  pageSize: number;
  onPageChange: (page: number, pageSize: number) => void;
  issueType?: IssueType;
  onIssueTypeChange: (v: IssueType | undefined) => void;
  severity?: Severity;
  onSeverityChange: (v: Severity | undefined) => void;
}

const columns: ColumnsType<DataIssue> = [
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
    width: 110,
  },
  {
    title: '问题类型',
    dataIndex: 'issue_type',
    width: 100,
    render: (v: IssueType) => <IssueTypeTag type={v} />,
  },
  {
    title: '严重程度',
    dataIndex: 'severity',
    width: 90,
    render: (v: Severity) => <SeverityTag severity={v} />,
  },
  {
    title: '描述',
    dataIndex: 'description',
    ellipsis: true,
  },
  {
    title: '数量',
    dataIndex: 'count',
    width: 70,
  },
];

export default function IssuesTab({
  data,
  isLoading,
  error,
  page,
  pageSize,
  onPageChange,
  issueType,
  onIssueTypeChange,
  severity,
  onSeverityChange,
}: Props) {
  if (error) return <ErrorAlert message={error.message} />;

  return (
    <div>
      <Space style={{ marginBottom: 16 }}>
        <Select
          placeholder="问题类型"
          allowClear
          style={{ width: 130 }}
          value={issueType}
          onChange={onIssueTypeChange}
          options={[
            { value: 'gap', label: '时间缺口' },
            { value: 'negative', label: '负值' },
            { value: 'jump', label: '异常跳变' },
            { value: 'out_of_range', label: '超量程' },
          ]}
        />
        <Select
          placeholder="严重程度"
          allowClear
          style={{ width: 110 }}
          value={severity}
          onChange={onSeverityChange}
          options={[
            { value: 'high', label: '严重' },
            { value: 'medium', label: '中等' },
            { value: 'low', label: '轻微' },
          ]}
        />
      </Space>

      <Table<DataIssue>
        columns={columns}
        dataSource={data?.items}
        loading={isLoading}
        rowKey={(r) =>
          `${r.bucket_time}-${r.building_id}-${r.system_id}-${r.equipment_type}-${r.equipment_id}-${r.sub_equipment_id}-${r.issue_type}-${r.metric_name}`
        }
        pagination={{
          current: page,
          pageSize,
          total: data?.total ?? 0,
          showSizeChanger: true,
          showTotal: (total) => `共 ${total} 条`,
          onChange: onPageChange,
        }}
        scroll={{ x: 1000 }}
        size="middle"
      />
    </div>
  );
}
