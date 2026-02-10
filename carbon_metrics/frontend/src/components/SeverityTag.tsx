import { Tag } from 'antd';
import type { Severity } from '../api/types';
import { SEVERITY_CONFIG } from '../constants/qualityLevels';

interface Props {
  severity: Severity;
}

export default function SeverityTag({ severity }: Props) {
  const config = SEVERITY_CONFIG[severity];
  if (!config) return <Tag>{severity}</Tag>;
  return <Tag color={config.color}>{config.label}</Tag>;
}
