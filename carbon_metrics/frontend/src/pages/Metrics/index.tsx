import { useEffect, useMemo, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Alert, Button, Card, Empty, Menu, Radio, Space } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import { METRIC_CATEGORIES } from '../../constants/metricCategories';
import { getMetricFilterConfig } from '../../constants/metricFilterConfig';
import { useMetricCalculate, useMetricCoverage } from '../../hooks/useMetrics';
import { useGlobalTimeRange } from '../../hooks/useGlobalTimeRange';
import TimeRangeSelector from '../../components/TimeRangeSelector';
import FilterBar from '../../components/FilterBar';
import DataCoverageBanner from '../../components/DataCoverageBanner';
import ErrorAlert from '../../components/ErrorAlert';
import LoadingCard from '../../components/LoadingCard';
import MetricResultCard from '../MetricDetail/MetricResultCard';
import QualityIssuesPanel from '../MetricDetail/QualityIssuesPanel';
import TracePanel from '../MetricDetail/TracePanel';
import BreakdownTable from '../MetricDetail/BreakdownTable';

export default function MetricsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [timeRange, setTimeRange] = useGlobalTimeRange(30);

  const initCategory = searchParams.get('category') || METRIC_CATEGORIES[0]!.key;
  const initCat = METRIC_CATEGORIES.find((c) => c.key === initCategory) ?? METRIC_CATEGORIES[0]!;
  const initMetric = searchParams.get('metric') || initCat.metrics[0]!;

  const [selectedCategory, setSelectedCategory] = useState(initCategory);
  const [selectedMetric, setSelectedMetric] = useState(initMetric);
  const [buildingId, setBuildingId] = useState<string | undefined>();
  const [systemId, setSystemId] = useState<string | undefined>();
  const [equipmentType, setEquipmentType] = useState<string | undefined>();
  const [equipmentId, setEquipmentId] = useState<string | undefined>();

  const updateSearchParams = (
    category: string,
    metric: string,
    range: [string, string],
  ) => {
    setSearchParams(
      {
        category,
        metric,
        time_start: range[0],
        time_end: range[1],
      },
      { replace: true },
    );
  };

  const currentCategory = useMemo(
    () => METRIC_CATEGORIES.find((c) => c.key === selectedCategory) ?? METRIC_CATEGORIES[0]!,
    [selectedCategory],
  );

  const filterConfig = useMemo(
    () => getMetricFilterConfig(selectedMetric),
    [selectedMetric],
  );

  const currentFilters = useMemo(
    () => ({
      time_start: timeRange[0],
      time_end: timeRange[1],
      building_id: buildingId,
      system_id: systemId,
      equipment_type: filterConfig.showEquipmentType ? equipmentType : filterConfig.fixedEquipmentType,
      equipment_id: filterConfig.showEquipmentId ? equipmentId : undefined,
    }),
    [
      buildingId,
      equipmentId,
      equipmentType,
      filterConfig.fixedEquipmentType,
      filterConfig.showEquipmentId,
      filterConfig.showEquipmentType,
      systemId,
      timeRange,
    ],
  );
  const { data, isLoading, error, refetch } = useMetricCalculate(
    selectedMetric,
    currentFilters,
  );
  const metricCoverageQuery = useMetricCoverage(currentFilters);

  const handleCategoryChange = (key: string) => {
    setSelectedCategory(key);
    const cat = METRIC_CATEGORIES.find((c) => c.key === key) ?? METRIC_CATEGORIES[0]!;
    setSelectedMetric(cat.metrics[0]!);
    setEquipmentType(undefined);
    setEquipmentId(undefined);
    updateSearchParams(key, cat.metrics[0]!, timeRange);
  };

  const handleMetricChange = (metric: string) => {
    setSelectedMetric(metric);
    setEquipmentType(undefined);
    setEquipmentId(undefined);
    updateSearchParams(selectedCategory, metric, timeRange);
  };

  useEffect(() => {
    updateSearchParams(selectedCategory, selectedMetric, timeRange);
  }, [selectedCategory, selectedMetric, timeRange]);

  const categoryMenuItems = METRIC_CATEGORIES.map((cat) => ({
    key: cat.key,
    label: `${cat.label} (${cat.metrics.length})`,
  }));

  return (
    <div style={{ display: 'flex', gap: 16 }}>
      <Card style={{ width: 180, flexShrink: 0 }} bodyStyle={{ padding: 0 }}>
        <Menu
          mode="vertical"
          selectedKeys={[selectedCategory]}
          items={categoryMenuItems}
          onClick={({ key }) => handleCategoryChange(key)}
          style={{ border: 'none' }}
        />
      </Card>

      <div style={{ flex: 1, minWidth: 0 }}>
        <Card style={{ marginBottom: 16 }}>
          <Space direction="vertical" size="middle" style={{ width: '100%' }}>
            <TimeRangeSelector value={timeRange} onChange={setTimeRange} />
            <Space>
              <FilterBar
                buildingId={buildingId}
                onBuildingIdChange={setBuildingId}
                systemId={systemId}
                onSystemIdChange={setSystemId}
                equipmentType={equipmentType}
                onEquipmentTypeChange={setEquipmentType}
                equipmentId={equipmentId}
                onEquipmentIdChange={setEquipmentId}
                showBuildingId={filterConfig.showBuildingId}
                showSystemId={filterConfig.showSystemId}
                showEquipmentType={filterConfig.showEquipmentType}
                showEquipmentId={filterConfig.showEquipmentId}
                fixedEquipmentType={filterConfig.fixedEquipmentType}
              />
              <Button
                type="primary"
                icon={<ReloadOutlined />}
                onClick={() => refetch()}
                loading={isLoading}
              >
                重新计算
              </Button>
            </Space>
          </Space>
        </Card>

        <DataCoverageBanner
          data={metricCoverageQuery.data}
          isLoading={metricCoverageQuery.isLoading}
          errorMessage={metricCoverageQuery.error?.message}
        />

        <Card size="small" style={{ marginBottom: 16 }}>
          <Radio.Group
            value={selectedMetric}
            onChange={(e) => handleMetricChange(e.target.value)}
            optionType="button"
            buttonStyle="solid"
          >
            {currentCategory.metrics.map((m) => (
              <Radio.Button key={m} value={m}>
                {m}
              </Radio.Button>
            ))}
          </Radio.Group>
        </Card>

        {error && <ErrorAlert message={error.message} />}
        {isLoading && <LoadingCard />}
        {data ? (
          <>
            <MetricResultCard
              metricName={data.metric_name}
              value={data.value}
              unit={data.unit}
              status={data.status}
              qualityScore={data.quality_score}
            />
            {data.status === 'no_data' && data.quality_issues.length === 0 && (
              <Alert
                style={{ marginTop: 12 }}
                type="warning"
                showIcon
                message="当前条件下没有可用数据"
                description="请调整时间范围、设备筛选，或检查该指标依赖的数据是否已入库。"
              />
            )}
            <QualityIssuesPanel status={data.status} issues={data.quality_issues} />
            <TracePanel trace={data.trace} />
            <BreakdownTable breakdown={data.breakdown} />
          </>
        ) : (
          !isLoading &&
          !error && (
            <Card>
              <Empty description="暂无指标结果，请调整筛选后重试" />
            </Card>
          )
        )}
      </div>
    </div>
  );
}
