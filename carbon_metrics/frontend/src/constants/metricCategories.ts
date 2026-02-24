export interface MetricCategory {
  key: string;
  label: string;
  metrics: string[];
}

export const METRIC_CATEGORIES: MetricCategory[] = [
  {
    key: 'energy',
    label: '能耗结构',
    metrics: ['系统总电量', '冷机能耗占比', '水泵能耗占比', '风机能耗占比'],
  },
  {
    key: 'temperature',
    label: '温度与温差',
    metrics: [
      '冷冻水供水温度',
      '冷冻水回水温度',
      '冷却水供水温度',
      '冷却水回水温度',
      '冷冻水温差',
    ],
  },
  {
    key: 'flow',
    label: '流量与制冷量',
    metrics: ['冷冻水流量', '冷却水流量', '制冷量'],
  },
  {
    key: 'chiller',
    label: '冷机效率',
    metrics: ['冷机平均负载率', '冷机最大负载率', '冷机负载波动系数', '冷机COP', '制冷系统COP'],
  },
  {
    key: 'pump',
    label: '水泵效率',
    metrics: ['冷冻泵工作频率', '冷却泵工作频率', '冷冻泵能耗密度', '冷却泵能耗密度'],
  },
  {
    key: 'tower',
    label: '冷却塔效率',
    metrics: ['冷却水温差', '冷却塔风机功率', '冷却塔效率'],
  },
  {
    key: 'stability',
    label: '运行稳定性',
    metrics: ['冷机运行时长占比', '风机运行时长占比'],
  },
  {
    key: 'maintenance',
    label: '预测性维护',
    metrics: ['过载风险指数'],
  },
];

// Categories temporarily hidden from frontend navigation and dashboard cards.
export const FRONTEND_HIDDEN_CATEGORY_KEYS = new Set<string>(['stability', 'maintenance']);

export const VISIBLE_METRIC_CATEGORIES: MetricCategory[] = METRIC_CATEGORIES.filter(
  (category) => !FRONTEND_HIDDEN_CATEGORY_KEYS.has(category.key),
);
