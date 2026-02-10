import { Row, Col, Card, Statistic } from 'antd';
import {
  CheckCircleOutlined,
  WarningOutlined,
  ExclamationCircleOutlined,
} from '@ant-design/icons';
import type { QualitySummary } from '../../api/types';

interface Props {
  data: QualitySummary;
}

export default function QualityOverviewCard({ data }: Props) {
  const totalIssues = data.total_gaps + data.total_negatives + data.total_jumps;

  return (
    <Row gutter={[16, 16]}>
      <Col xs={12} sm={6}>
        <Card>
          <Statistic
            title="平均质量分"
            value={data.avg_quality_score}
            precision={1}
            suffix="/ 100"
          />
        </Card>
      </Col>
      <Col xs={12} sm={6}>
        <Card>
          <Statistic
            title="良好"
            value={data.good_count}
            prefix={<CheckCircleOutlined />}
            valueStyle={{ color: '#52c41a' }}
          />
        </Card>
      </Col>
      <Col xs={12} sm={6}>
        <Card>
          <Statistic
            title="警告"
            value={data.warning_count}
            prefix={<WarningOutlined />}
            valueStyle={{ color: '#faad14' }}
          />
        </Card>
      </Col>
      <Col xs={12} sm={6}>
        <Card>
          <Statistic
            title="异常总数"
            value={totalIssues}
            prefix={<ExclamationCircleOutlined />}
            valueStyle={totalIssues > 0 ? { color: '#ff4d4f' } : undefined}
          />
        </Card>
      </Col>
    </Row>
  );
}
