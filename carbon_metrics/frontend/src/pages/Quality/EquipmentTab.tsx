import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Table } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import dayjs from 'dayjs';
import type { QualityRecord, QualityLevel, Granularity } from '../../api/types';
import { useQualityList } from '../../hooks/useQuality';
import { EQUIPMENT_TYPE_LABELS } from '../../constants/equipmentTypes';
import QualityBadge from '../../components/QualityBadge';
import QualityScoreTag from '../../components/QualityScoreTag';
import ErrorAlert from '../../components/ErrorAlert';

interface Props {
  timeRange: [string, string];
  equipmentType?: string;
  qualityLevel?: QualityLevel;
  granularity: Granularity;
}

export default function EquipmentTab({
  timeRange,
  equipmentType,
  qualityLevel,
  granularity,
}: Props) {
  const navigate = useNavigate();
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);

  const { data, isLoading, error } = useQualityList({
    time_start: timeRange[0],
    time_end: timeRange[1],
    equipment_type: equipmentType,
    quality_level: qualityLevel,
    granularity,
    page,
    page_size: pageSize,
  });

  useEffect(() => {
    setPage(1);
  }, [timeRange[0], timeRange[1], equipmentType, qualityLevel, granularity]);

  const columns: ColumnsType<QualityRecord> = [
    {
      title: '时间',
      dataIndex: 'bucket_time',
      width: 140,
      render: (v: string) => dayjs(v).format('YYYY-MM-DD'),
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
      width: 160,
      ellipsis: true,
    },
    {
      title: '指标',
      dataIndex: 'metric_name',
      width: 110,
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
      render: (v: QualityRecord['quality_level']) => (
        <QualityBadge level={v} />
      ),
    },
    {
      title: '完整率',
      dataIndex: 'completeness_rate',
      width: 90,
      render: (v: number) => `${v.toFixed(1)}%`,
    },
    {
      title: '操作',
      width: 80,
      render: (_: unknown, record: QualityRecord) =>
        record.equipment_id ? (
          <a
            onClick={() =>
              navigate(
                `/quality/equipment/${encodeURIComponent(record.equipment_id!)}?time_start=${encodeURIComponent(timeRange[0])}&time_end=${encodeURIComponent(timeRange[1])}`,
              )
            }
          >
            详情
          </a>
        ) : null,
    },
  ];

  return (
    <div>
      {error && <ErrorAlert message={error.message} />}
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
          onChange: (p, ps) => {
            setPage(p);
            setPageSize(ps);
          },
        }}
        scroll={{ x: 900 }}
        size="middle"
      />
    </div>
  );
}
