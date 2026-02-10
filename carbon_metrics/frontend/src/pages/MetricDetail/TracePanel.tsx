import { Collapse, Descriptions, Typography } from 'antd';
import type { MetricTrace } from '../../api/types';

const { Text, Paragraph } = Typography;

interface Props {
  trace: MetricTrace | null;
}

export default function TracePanel({ trace }: Props) {
  if (!trace) return null;

  const { data_source } = trace;

  return (
    <Collapse
      style={{ marginTop: 16 }}
      items={[
        {
          key: 'trace',
          label: '计算追溯',
          children: (
            <div>
              <Descriptions column={1} size="small" bordered>
                <Descriptions.Item label="计算公式">
                  {trace.formula}
                </Descriptions.Item>
                <Descriptions.Item label="带值公式">
                  <Text strong>{trace.formula_with_values}</Text>
                </Descriptions.Item>
              </Descriptions>

              <Descriptions
                column={2}
                size="small"
                bordered
                style={{ marginTop: 12 }}
                title="数据源"
              >
                <Descriptions.Item label="数据表">
                  {data_source.table}
                </Descriptions.Item>
                <Descriptions.Item label="聚合字段">
                  {data_source.field}
                </Descriptions.Item>
                <Descriptions.Item label="筛选条件" span={2}>
                  <Text code>{data_source.condition}</Text>
                </Descriptions.Item>
                <Descriptions.Item label="总记录数">
                  {data_source.total_records.toLocaleString()}
                </Descriptions.Item>
                <Descriptions.Item label="有效记录数">
                  {data_source.valid_records.toLocaleString()}
                </Descriptions.Item>
              </Descriptions>

              <div style={{ marginTop: 12 }}>
                <Text type="secondary">执行 SQL</Text>
                <Paragraph
                  code
                  copyable
                  style={{
                    marginTop: 4,
                    whiteSpace: 'pre-wrap',
                    fontSize: 12,
                    maxHeight: 200,
                    overflow: 'auto',
                  }}
                >
                  {trace.sql}
                </Paragraph>
              </div>
            </div>
          ),
        },
      ]}
    />
  );
}
