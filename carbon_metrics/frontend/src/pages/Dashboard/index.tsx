import { useMemo } from 'react';
import { Card, Typography } from 'antd';
import { useQualitySummary } from '../../hooks/useQuality';
import { useMetricCoverage } from '../../hooks/useMetrics';
import { useGlobalTimeRange } from '../../hooks/useGlobalTimeRange';
import TimeRangeSelector from '../../components/TimeRangeSelector';
import DataCoverageBanner from '../../components/DataCoverageBanner';
import ErrorAlert from '../../components/ErrorAlert';
import LoadingCard from '../../components/LoadingCard';
import QualityOverviewCard from './QualityOverviewCard';
import MetricCategoryCards from './MetricCategoryCards';

const { Title } = Typography;

export default function Dashboard() {
  const [timeRange, setTimeRange] = useGlobalTimeRange(7);

  const filters = useMemo(
    () => ({
      time_start: timeRange[0],
      time_end: timeRange[1],
    }),
    [timeRange],
  );

  const metricCoverageQuery = useMetricCoverage(filters);
  const { data, isLoading, error } = useQualitySummary(filters);

  return (
    <div>
      <Card style={{ marginBottom: 16 }}>
        <TimeRangeSelector value={timeRange} onChange={setTimeRange} />
      </Card>

      <DataCoverageBanner
        data={metricCoverageQuery.data}
        isLoading={metricCoverageQuery.isLoading}
        errorMessage={metricCoverageQuery.error?.message}
      />

      <Title level={5} style={{ marginBottom: 12 }}>
        数据质量概览
      </Title>
      {error && <ErrorAlert message={error.message} />}
      {isLoading && <LoadingCard />}
      {data && <QualityOverviewCard data={data} />}

      <Title level={5} style={{ marginTop: 24, marginBottom: 12 }}>
        指标分类
      </Title>
      <MetricCategoryCards timeRange={timeRange} />
    </div>
  );
}
