import { Tag } from 'antd';
import type { QualityLevel } from '../api/types';
import { QUALITY_LEVEL_CONFIG } from '../constants/qualityLevels';

interface Props {
  level: QualityLevel;
}

export default function QualityBadge({ level }: Props) {
  const config = QUALITY_LEVEL_CONFIG[level];
  return <Tag color={config.color}>{config.label}</Tag>;
}
