import { Collapse, Descriptions, Space, Tag, Typography } from 'antd';
import type { MetricTrace } from '../../api/types';

const { Text, Paragraph } = Typography;

interface Props {
  trace: MetricTrace | null;
}

type ScopeState = 'included' | 'excluded' | 'unrestricted' | 'filtered';

interface ScopeCell {
  state: ScopeState;
  note: string;
}

interface SubEquipmentScope {
  main: ScopeCell;
  backup: ScopeCell;
  unlabeled: ScopeCell;
}

const scopePreset: Record<ScopeState, { color: string; text: string }> = {
  included: { color: 'success', text: '计入' },
  excluded: { color: 'error', text: '排除' },
  unrestricted: { color: 'processing', text: '未限制' },
  filtered: { color: 'warning', text: '按筛选' },
};

function inferSubEquipmentScope(condition: string): SubEquipmentScope {
  const normalized = (condition || '').toLowerCase().replace(/\s+/g, ' ').trim();
  const unrestricted: SubEquipmentScope = {
    main: { state: 'unrestricted', note: '未限制' },
    backup: { state: 'unrestricted', note: '未限制' },
    unlabeled: { state: 'unrestricted', note: '未限制' },
  };

  if (!normalized.includes('sub_equipment_id')) {
    return unrestricted;
  }

  if (/sub_equipment_id\s*=\s*%s/i.test(normalized)) {
    return {
      main: { state: 'filtered', note: '按筛选值' },
      backup: { state: 'filtered', note: '按筛选值' },
      unlabeled: { state: 'filtered', note: '按筛选值' },
    };
  }

  const inMatch = normalized.match(/sub_equipment_id\s+in\s*\(([^)]*)\)/i);
  if (inMatch) {
    const inClause = inMatch[1] ?? '';
    const includeMain = /'main'/.test(inClause);
    const includeBackup = /'backup'/.test(inClause);
    const includeNull = /\bnull\b/.test(inClause) || /''/.test(inClause);

    return {
      main: { state: includeMain ? 'included' : 'excluded', note: 'IN 口径' },
      backup: { state: includeBackup ? 'included' : 'excluded', note: 'IN 口径' },
      unlabeled: { state: includeNull ? 'included' : 'excluded', note: 'IN 口径' },
    };
  }

  if (/sub_equipment_id\s+is\s+null/i.test(normalized)) {
    return {
      main: { state: 'excluded', note: '仅 null' },
      backup: { state: 'excluded', note: '仅 null' },
      unlabeled: { state: 'included', note: '仅 null' },
    };
  }

  const eqMatch = normalized.match(/sub_equipment_id\s*=\s*'([^']*)'/i);
  if (eqMatch) {
    const value = eqMatch[1] ?? '';
    return {
      main: { state: value === 'main' ? 'included' : 'excluded', note: '= 口径' },
      backup: { state: value === 'backup' ? 'included' : 'excluded', note: '= 口径' },
      unlabeled: {
        state: value === '' ? 'included' : 'excluded',
        note: value === '' ? '= 空字符串' : '= 口径',
      },
    };
  }

  return {
    main: { state: 'filtered', note: '请结合SQL' },
    backup: { state: 'filtered', note: '请结合SQL' },
    unlabeled: { state: 'filtered', note: '请结合SQL' },
  };
}

function renderScopeTag(cell: ScopeCell) {
  const preset = scopePreset[cell.state];
  return (
    <Space size={6}>
      <Tag color={preset.color}>{preset.text}</Tag>
      <Text type="secondary">{cell.note}</Text>
    </Space>
  );
}

export default function TracePanel({ trace }: Props) {
  if (!trace) return null;

  const { data_source } = trace;
  const subEquipmentScope = inferSubEquipmentScope(data_source.condition);

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

              <Descriptions
                column={3}
                size="small"
                bordered
                style={{ marginTop: 12 }}
                title="主备口径"
              >
                <Descriptions.Item label="主机(main)">
                  {renderScopeTag(subEquipmentScope.main)}
                </Descriptions.Item>
                <Descriptions.Item label="备机(backup)">
                  {renderScopeTag(subEquipmentScope.backup)}
                </Descriptions.Item>
                <Descriptions.Item label="未区分(null)">
                  {renderScopeTag(subEquipmentScope.unlabeled)}
                </Descriptions.Item>
              </Descriptions>
              <Text type="secondary" style={{ display: 'block', marginTop: 8 }}>
                注：未区分(null) 表示点位未拆分主机/备机，属于单通道采集。
              </Text>

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
