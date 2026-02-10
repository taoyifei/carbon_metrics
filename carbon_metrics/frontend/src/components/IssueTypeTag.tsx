import { Tag } from 'antd';
import type { IssueType } from '../api/types';
import { ISSUE_TYPE_CONFIG } from '../constants/qualityLevels';

interface Props {
  type: IssueType;
}

export default function IssueTypeTag({ type }: Props) {
  const config = ISSUE_TYPE_CONFIG[type];
  if (!config) return <Tag>{type}</Tag>;
  return <Tag color={config.color}>{config.label}</Tag>;
}
