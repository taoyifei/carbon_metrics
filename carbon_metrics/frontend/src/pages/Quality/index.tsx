import { useEffect, useMemo, useState } from 'react';
import { Tabs, Card, Space } from 'antd';
import type {
  QualityLevel,
  Granularity,
  IssueType,
  Severity,
} from '../../api/types';
import {
  useQualitySummary,
  useQualityList,
  useQualityIssues,
} from '../../hooks/useQuality';
import { useGlobalTimeRange } from '../../hooks/useGlobalTimeRange';
import TimeRangeSelector from '../../components/TimeRangeSelector';
import FilterBar from '../../components/FilterBar';
import QualitySummaryTab from './QualitySummaryTab';
import QualityListTab from './QualityListTab';
import IssuesTab from './IssuesTab';
import RawReportTab from './RawReportTab';
import EquipmentTab from './EquipmentTab';

export default function QualityPage() {
  const [timeRange, setTimeRange] = useGlobalTimeRange(7);
  const [equipmentType, setEquipmentType] = useState<string>();
  const [qualityLevel, setQualityLevel] = useState<QualityLevel>();
  const [granularity, setGranularity] = useState<Granularity>('hour');
  const [activeTab, setActiveTab] = useState('summary');

  // List tab pagination
  const [listPage, setListPage] = useState(1);
  const [listPageSize, setListPageSize] = useState(20);

  // Issues tab state
  const [issuesPage, setIssuesPage] = useState(1);
  const [issuesPageSize, setIssuesPageSize] = useState(20);
  const [issueType, setIssueType] = useState<IssueType>();
  const [severity, setSeverity] = useState<Severity>();

  const baseFilters = useMemo(
    () => ({
      time_start: timeRange[0],
      time_end: timeRange[1],
      equipment_type: equipmentType,
      quality_level: qualityLevel,
      granularity,
    }),
    [equipmentType, granularity, qualityLevel, timeRange],
  );

  const summaryQuery = useQualitySummary(
    baseFilters,
    activeTab === 'summary',
  );

  const listQuery = useQualityList(
    { ...baseFilters, page: listPage, page_size: listPageSize },
    activeTab === 'list',
  );

  const issuesQuery = useQualityIssues(
    {
      ...baseFilters,
      issue_type: issueType,
      severity,
      page: issuesPage,
      page_size: issuesPageSize,
    },
    activeTab === 'issues',
  );

  const handleListPageChange = (page: number, pageSize: number) => {
    setListPage(page);
    setListPageSize(pageSize);
  };

  const handleIssuesPageChange = (page: number, pageSize: number) => {
    setIssuesPage(page);
    setIssuesPageSize(pageSize);
  };

  useEffect(() => {
    setListPage(1);
  }, [timeRange[0], timeRange[1], equipmentType, qualityLevel, granularity]);

  useEffect(() => {
    setIssuesPage(1);
  }, [
    timeRange[0],
    timeRange[1],
    equipmentType,
    qualityLevel,
    granularity,
    issueType,
    severity,
  ]);

  return (
    <div>
      <Card style={{ marginBottom: 16 }}>
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          <TimeRangeSelector value={timeRange} onChange={setTimeRange} />
          <FilterBar
            equipmentType={equipmentType}
            onEquipmentTypeChange={setEquipmentType}
            qualityLevel={qualityLevel}
            onQualityLevelChange={setQualityLevel}
            granularity={granularity}
            onGranularityChange={setGranularity}
            showGranularity
            showQualityLevel
          />
        </Space>
      </Card>

      <Card>
        <Tabs
          activeKey={activeTab}
          onChange={setActiveTab}
          items={[
            {
              key: 'summary',
              label: '质量汇总',
              children: (
                <QualitySummaryTab
                  data={summaryQuery.data}
                  isLoading={summaryQuery.isLoading}
                  error={summaryQuery.error}
                />
              ),
            },
            {
              key: 'list',
              label: '质量明细',
              children: (
                <QualityListTab
                  data={listQuery.data}
                  isLoading={listQuery.isLoading}
                  error={listQuery.error}
                  page={listPage}
                  pageSize={listPageSize}
                  onPageChange={handleListPageChange}
                />
              ),
            },
            {
              key: 'issues',
              label: '异常问题',
              children: (
                <IssuesTab
                  data={issuesQuery.data}
                  isLoading={issuesQuery.isLoading}
                  error={issuesQuery.error}
                  page={issuesPage}
                  pageSize={issuesPageSize}
                  onPageChange={handleIssuesPageChange}
                  issueType={issueType}
                  onIssueTypeChange={setIssueType}
                  severity={severity}
                  onSeverityChange={setSeverity}
                />
              ),
            },
            {
              key: 'raw',
              label: '原始报告',
              children: <RawReportTab />,
            },
            {
              key: 'equipment',
              label: '设备列表',
              children: (
                <EquipmentTab
                  timeRange={timeRange}
                  equipmentType={equipmentType}
                  qualityLevel={qualityLevel}
                  granularity={granularity}
                />
              ),
            },
          ]}
        />
      </Card>
    </div>
  );
}
