import { Row, Col, Card, Statistic } from 'antd';
import {
  CheckCircleOutlined,
  WarningOutlined,
  CloseCircleOutlined,
} from '@ant-design/icons';
import type { QualitySummary } from '../../api/types';
import ErrorAlert from '../../components/ErrorAlert';
import LoadingCard from '../../components/LoadingCard';

interface Props {
  data?: QualitySummary;
  isLoading: boolean;
  error: Error | null;
}

export default function QualitySummaryTab({ data, isLoading, error }: Props) {
  if (isLoading) return <LoadingCard />;
  if (error) return <ErrorAlert message={error.message} />;
  if (!data) return null;

  const totalIssues = data.total_gaps + data.total_negatives + data.total_jumps;

  return (
    <div>
      <Row gutter={[16, 16]}>
        <Col span={6}>
          <Card>
            <Statistic title="总记录数" value={data.total_records} />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="平均质量分"
              value={data.avg_quality_score}
              precision={1}
              suffix="/ 100"
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="平均完整率"
              value={data.avg_completeness_rate}
              precision={1}
              suffix="%"
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="总异常数"
              value={totalIssues}
              valueStyle={
                totalIssues > 0 ? { color: '#ff4d4f' } : undefined
              }
            />
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
        <Col span={8}>
          <Card>
            <Statistic
              title="良好"
              value={data.good_count}
              prefix={<CheckCircleOutlined />}
              valueStyle={{ color: '#52c41a' }}
            />
          </Card>
        </Col>
        <Col span={8}>
          <Card>
            <Statistic
              title="警告"
              value={data.warning_count}
              prefix={<WarningOutlined />}
              valueStyle={{ color: '#faad14' }}
            />
          </Card>
        </Col>
        <Col span={8}>
          <Card>
            <Statistic
              title="差"
              value={data.poor_count}
              prefix={<CloseCircleOutlined />}
              valueStyle={{ color: '#ff4d4f' }}
            />
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
        <Col span={8}>
          <Card>
            <Statistic title="时间缺口" value={data.total_gaps} />
          </Card>
        </Col>
        <Col span={8}>
          <Card>
            <Statistic title="负值异常" value={data.total_negatives} />
          </Card>
        </Col>
        <Col span={8}>
          <Card>
            <Statistic title="跳变异常" value={data.total_jumps} />
          </Card>
        </Col>
      </Row>
    </div>
  );
}
