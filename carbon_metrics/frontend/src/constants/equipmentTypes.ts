export const EQUIPMENT_TYPE_LABELS: Record<string, string> = {
  chiller: '冷机',
  chilled_pump: '冷冻泵',
  cooling_pump: '冷却泵',
  closed_tower_pump: '闭式塔泵',
  user_side_pump: '用户侧循环泵',
  source_side_pump: '水源侧循环泵',
  heat_recovery_primary_pump: '余热回收一次泵',
  heat_recovery_secondary_pump: '余热回收二次泵',
  fire_pump: '消防泵',
  unknown_pump: '未知泵',
  cooling_tower: '开式冷却塔',
  cooling_tower_closed: '闭式冷却塔',
  tower_fan: '冷塔风机',
};

export const EQUIPMENT_TYPE_OPTIONS = [
  { value: '', label: '全部' },
  ...Object.entries(EQUIPMENT_TYPE_LABELS).map(
    ([value, label]) => ({ value, label }),
  ),
];
