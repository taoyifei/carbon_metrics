/**
 * 每个指标的筛选条件配置
 * 控制 FilterBar 中哪些筛选项可见/可用
 */
export interface MetricFilterConfig {
  showBuildingId: boolean;
  showSystemId: boolean;
  showEquipmentType: boolean;
  showEquipmentId: boolean;
  /** 固定设备类型（不可更改），如 'chiller' */
  fixedEquipmentType?: string;
}

const FULL_FILTER: MetricFilterConfig = {
  showBuildingId: true,
  showSystemId: true,
  showEquipmentType: true,
  showEquipmentId: true,
};

const SYSTEM_ONLY: MetricFilterConfig = {
  showBuildingId: true,
  showSystemId: true,
  showEquipmentType: false,
  showEquipmentId: false,
};

function fixedType(type: string): MetricFilterConfig {
  return {
    showBuildingId: true,
    showSystemId: true,
    showEquipmentType: false,
    showEquipmentId: true,
    fixedEquipmentType: type,
  };
}

/**
 * 指标名 -> 筛选配置映射
 */
export const METRIC_FILTER_CONFIG: Record<string, MetricFilterConfig> = {
  // 能耗结构
  '系统总电量': FULL_FILTER,
  '冷机能耗占比': SYSTEM_ONLY,
  '水泵能耗占比': SYSTEM_ONLY,
  '风机能耗占比': SYSTEM_ONLY,

  // 温度与温差（系统级数据）
  '冷冻水供水温度': SYSTEM_ONLY,
  '冷冻水回水温度': SYSTEM_ONLY,
  '冷却水供水温度': SYSTEM_ONLY,
  '冷却水回水温度': SYSTEM_ONLY,
  '冷冻水温差': SYSTEM_ONLY,

  // 流量与制冷量（系统级数据）
  '冷冻水流量': SYSTEM_ONLY,
  '冷却水流量': SYSTEM_ONLY,
  '制冷量': SYSTEM_ONLY,

  // 冷机效率
  '冷机平均负载率': fixedType('chiller'),
  '冷机最大负载率': fixedType('chiller'),
  '冷机负载波动系数': fixedType('chiller'),

  // 水泵效率
  '冷冻泵工作频率': fixedType('chilled_pump'),
  '冷却泵工作频率': fixedType('cooling_pump'),

  // 冷却塔效率
  '冷却水温差': SYSTEM_ONLY,
  // 不再固定到 tower_fan，避免误筛掉 cooling_tower/cooling_tower_closed 的功率数据
  '冷却塔风机功率': FULL_FILTER,

  // 运行稳定性
  '冷机运行时长占比': fixedType('chiller'),
  // 不固定设备类型，允许 tower_fan / cooling_tower / cooling_tower_closed 口径
  '风机运行时长占比': FULL_FILTER,

  // 预测性维护
  '过载风险指数': fixedType('chiller'),
};

/** 获取指标的筛选配置，未配置的默认全部显示 */
export function getMetricFilterConfig(metricName: string): MetricFilterConfig {
  return METRIC_FILTER_CONFIG[metricName] ?? FULL_FILTER;
}
