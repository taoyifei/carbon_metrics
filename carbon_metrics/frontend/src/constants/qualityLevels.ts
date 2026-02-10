export const QUALITY_LEVEL_CONFIG = {
  good: { label: '良好', color: '#52c41a' },
  warning: { label: '警告', color: '#faad14' },
  poor: { label: '差', color: '#ff4d4f' },
} as const;

export const ISSUE_TYPE_CONFIG = {
  gap: { label: '时间缺口', color: '#1890ff' },
  negative: { label: '负值', color: '#ff4d4f' },
  jump: { label: '异常跳变', color: '#faad14' },
  out_of_range: { label: '超量程', color: '#722ed1' },
} as const;

export const SEVERITY_CONFIG = {
  high: { label: '严重', color: '#ff4d4f' },
  medium: { label: '中等', color: '#faad14' },
  low: { label: '轻微', color: '#52c41a' },
} as const;
