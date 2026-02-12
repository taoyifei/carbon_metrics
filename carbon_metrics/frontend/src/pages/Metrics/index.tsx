import { useEffect, useMemo, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Alert, Button, Card, Empty, Menu, Radio, Space } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import { METRIC_CATEGORIES } from '../../constants/metricCategories';
import { getMetricFilterConfig } from '../../constants/metricFilterConfig';
import {
  useMetricCalculate,
  useMetricCalculateBySubScopes,
  useMetricCoverage,
} from '../../hooks/useMetrics';
import { useGlobalTimeRange } from '../../hooks/useGlobalTimeRange';
import type { MetricResult } from '../../api/types';
import TimeRangeSelector from '../../components/TimeRangeSelector';
import FilterBar from '../../components/FilterBar';
import DataCoverageBanner from '../../components/DataCoverageBanner';
import ErrorAlert from '../../components/ErrorAlert';
import LoadingCard from '../../components/LoadingCard';
import MetricResultCard from '../MetricDetail/MetricResultCard';
import QualityIssuesPanel from '../MetricDetail/QualityIssuesPanel';
import TracePanel from '../MetricDetail/TracePanel';
import BreakdownTable from '../MetricDetail/BreakdownTable';

type SubEquipmentScope = 'all' | 'main' | 'backup' | 'null';

const SPLIT_SUB_EQUIPMENT_SCOPES = ['main', 'backup', '__NULL__'] as const;
const SPLIT_SCOPE_LABELS: Record<(typeof SPLIT_SUB_EQUIPMENT_SCOPES)[number], string> = {
  main: '主机(main)',
  backup: '备机(backup)',
  __NULL__: '未区分(null)',
};

function isValidSubScope(value: string | null): value is SubEquipmentScope {
  return value === 'all' || value === 'main' || value === 'backup' || value === 'null';
}

function toSubEquipmentFilter(scope: SubEquipmentScope): string | undefined {
  if (scope === 'main') return 'main';
  if (scope === 'backup') return 'backup';
  if (scope === 'null') return '__NULL__';
  return undefined;
}

export default function MetricsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [timeRange, setTimeRange] = useGlobalTimeRange(30);

  const initCategory = searchParams.get('category') || METRIC_CATEGORIES[0]!.key;
  const initCat = METRIC_CATEGORIES.find((c) => c.key === initCategory) ?? METRIC_CATEGORIES[0]!;
  const initMetric = searchParams.get('metric') || initCat.metrics[0]!;
  const initSubScopeParam = searchParams.get('sub_scope');
  const initSubScope: SubEquipmentScope = isValidSubScope(initSubScopeParam)
    ? initSubScopeParam
    : 'all';

  const [selectedCategory, setSelectedCategory] = useState(initCategory);
  const [selectedMetric, setSelectedMetric] = useState(initMetric);
  const [buildingId, setBuildingId] = useState<string | undefined>();
  const [systemId, setSystemId] = useState<string | undefined>();
  const [equipmentType, setEquipmentType] = useState<string | undefined>();
  const [equipmentId, setEquipmentId] = useState<string | undefined>();
  const [subEquipmentScope, setSubEquipmentScope] = useState<SubEquipmentScope>(initSubScope);
  const [coverageRequested, setCoverageRequested] = useState(false);

  const updateSearchParams = (
    category: string,
    metric: string,
    range: [string, string],
    subScope: SubEquipmentScope,
  ) => {
    setSearchParams(
      {
        category,
        metric,
        time_start: range[0],
        time_end: range[1],
        sub_scope: subScope,
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
  const isSplitMode = filterConfig.showSubEquipmentScope && subEquipmentScope === 'all';
  const subEquipmentIdFilter = filterConfig.showSubEquipmentScope
    ? toSubEquipmentFilter(subEquipmentScope)
    : undefined;

  const currentFilters = useMemo(
    () => ({
      time_start: timeRange[0],
      time_end: timeRange[1],
      building_id: buildingId,
      system_id: systemId,
      equipment_type: filterConfig.showEquipmentType ? equipmentType : filterConfig.fixedEquipmentType,
      equipment_id: filterConfig.showEquipmentId ? equipmentId : undefined,
      sub_equipment_id: subEquipmentIdFilter,
    }),
    [
      buildingId,
      equipmentId,
      equipmentType,
      filterConfig.fixedEquipmentType,
      filterConfig.showEquipmentId,
      filterConfig.showEquipmentType,
      subEquipmentIdFilter,
      systemId,
      timeRange,
    ],
  );

  const isLongRange = useMemo(
    () => dayjs(timeRange[1]).diff(dayjs(timeRange[0]), 'day', true) > 31,
    [timeRange],
  );

  useEffect(() => {
    setCoverageRequested(false);
  }, [JSON.stringify(currentFilters), isLongRange]);

  const coverageEnabled = !isLongRange || coverageRequested;

  const singleMetricQuery = useMetricCalculate(
    selectedMetric,
    currentFilters,
    !isSplitMode,
  );
  const splitMetricQueries = useMetricCalculateBySubScopes(
    selectedMetric,
    currentFilters,
    [...SPLIT_SUB_EQUIPMENT_SCOPES],
    isSplitMode,
  );
  const metricCoverageQuery = useMetricCoverage(currentFilters, coverageEnabled);

  const splitErrorRaw = isSplitMode
    ? splitMetricQueries.find((query) => query.error)?.error
    : null;
  const splitError = splitErrorRaw
    ? (splitErrorRaw instanceof Error
      ? splitErrorRaw
      : new Error(String(splitErrorRaw)))
    : null;
  const error = isSplitMode ? splitError : (singleMetricQuery.error ?? null);
  const isLoading = isSplitMode
    ? splitMetricQueries.some((query) => query.isLoading)
    : singleMetricQuery.isLoading;
  const data = isSplitMode ? null : singleMetricQuery.data;

  const handleRefetch = () => {
    if (isSplitMode) {
      splitMetricQueries.forEach((query) => {
        void query.refetch();
      });
      return;
    }
    void singleMetricQuery.refetch();
  };

  const handleCategoryChange = (key: string) => {
    setSelectedCategory(key);
    const cat = METRIC_CATEGORIES.find((c) => c.key === key) ?? METRIC_CATEGORIES[0]!;
    setSelectedMetric(cat.metrics[0]!);
    setEquipmentType(undefined);
    setEquipmentId(undefined);
    setSubEquipmentScope('all');
    updateSearchParams(key, cat.metrics[0]!, timeRange, 'all');
  };

  const handleMetricChange = (metric: string) => {
    setSelectedMetric(metric);
    setEquipmentType(undefined);
    setEquipmentId(undefined);
    setSubEquipmentScope('all');
    updateSearchParams(selectedCategory, metric, timeRange, 'all');
  };

  useEffect(() => {
    updateSearchParams(selectedCategory, selectedMetric, timeRange, subEquipmentScope);
  }, [selectedCategory, selectedMetric, subEquipmentScope, timeRange]);

  const categoryMenuItems = METRIC_CATEGORIES.map((cat) => ({
    key: cat.key,
    label: `${cat.label} (${cat.metrics.length})`,
  }));

  const renderMetricDetail = (result: MetricResult, metricNameOverride?: string) => (
    <>
      <MetricResultCard
        metricName={metricNameOverride ?? result.metric_name}
        value={result.value}
        unit={result.unit}
        status={result.status}
        qualityScore={result.quality_score}
      />
      {result.status === 'no_data' && result.quality_issues.length === 0 && (
        <Alert
          style={{ marginTop: 12 }}
          type="warning"
          showIcon
          message="当前条件下没有可用数据"
          description="请调整时间范围、设备筛选，或检查该指标依赖的数据是否已入库。"
        />
      )}
      <TracePanel trace={result.trace} />
      <QualityIssuesPanel status={result.status} issues={result.quality_issues} />
      <BreakdownTable breakdown={result.breakdown} />
    </>
  );

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
                subEquipmentScope={subEquipmentScope}
                onSubEquipmentScopeChange={setSubEquipmentScope}
                showBuildingId={filterConfig.showBuildingId}
                showSystemId={filterConfig.showSystemId}
                showEquipmentType={filterConfig.showEquipmentType}
                showEquipmentId={filterConfig.showEquipmentId}
                showSubEquipmentScope={filterConfig.showSubEquipmentScope}
                fixedEquipmentType={filterConfig.fixedEquipmentType}
              />
              <Button
                type="primary"
                icon={<ReloadOutlined />}
                onClick={handleRefetch}
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
          deferredLoad={isLongRange && !coverageRequested}
          onRequestLoad={() => setCoverageRequested(true)}
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

        {isSplitMode && !isLoading && !error && (
          <Space direction="vertical" size={16} style={{ width: '100%' }}>
            <Alert
              type="info"
              showIcon
              message="当前为主备分算模式（全部）"
              description="已分别计算 主机(main)、备机(backup)、未区分(null) 三组结果。"
            />
            {SPLIT_SUB_EQUIPMENT_SCOPES.map((scope, idx) => {
              const result = splitMetricQueries[idx]?.data as MetricResult | undefined;
              const label = SPLIT_SCOPE_LABELS[scope];
              if (!result) {
                return (
                  <Card key={scope}>
                    <Empty description={`${label} 暂无可用结果`} />
                  </Card>
                );
              }
              return (
                <Card key={scope} size="small" title={label}>
                  {renderMetricDetail(result, `${result.metric_name} - ${label}`)}
                </Card>
              );
            })}
          </Space>
        )}

        {!isSplitMode && data && renderMetricDetail(data)}

        {!isLoading && !error && !data && !isSplitMode && (
          <Card>
            <Empty description="暂无指标结果，请调整筛选后重试" />
          </Card>
        )}
      </div>
    </div>
  );
}
