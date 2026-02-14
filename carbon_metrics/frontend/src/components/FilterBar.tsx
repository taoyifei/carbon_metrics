import { useEffect, useMemo } from 'react';
import { Select, Space } from 'antd';
import { EQUIPMENT_TYPE_OPTIONS } from '../constants/equipmentTypes';
import { useEquipmentIds, useSubEquipmentScopes } from '../hooks/useMetrics';
import type { QualityLevel, Granularity } from '../api/types';

/** 机楼 → 系统映射（硬编码，数据量小） */
const BUILDING_OPTIONS = [
  { value: '', label: '全部机楼' },
  { value: 'G11', label: 'G11' },
  { value: 'G12', label: 'G12' },
];

const SYSTEM_MAP: Record<string, { value: string; label: string }[]> = {
  G11: [
    { value: 'G11-1', label: 'G11-1' },
    { value: 'G11-2', label: 'G11-2' },
    { value: 'G11-3', label: 'G11-3' },
  ],
  G12: [
    { value: 'G12-1', label: 'G12-1' },
    { value: 'G12-2', label: 'G12-2' },
    { value: 'G12-3', label: 'G12-3' },
  ],
};

interface Props {
  buildingId?: string;
  onBuildingIdChange?: (v: string | undefined) => void;
  systemId?: string;
  onSystemIdChange?: (v: string | undefined) => void;
  equipmentType?: string;
  onEquipmentTypeChange?: (v: string | undefined) => void;
  equipmentId?: string;
  onEquipmentIdChange?: (v: string | undefined) => void;
  subEquipmentScope?: 'all' | 'main' | 'backup' | 'null';
  onSubEquipmentScopeChange?: (v: 'all' | 'main' | 'backup' | 'null') => void;
  showBuildingId?: boolean;
  showSystemId?: boolean;
  showEquipmentType?: boolean;
  showEquipmentId?: boolean;
  showSubEquipmentScope?: boolean;
  /** 固定设备类型（只读显示） */
  fixedEquipmentType?: string;
  qualityLevel?: QualityLevel;
  onQualityLevelChange?: (v: QualityLevel | undefined) => void;
  granularity?: Granularity;
  onGranularityChange?: (v: Granularity) => void;
  showGranularity?: boolean;
  showQualityLevel?: boolean;
}

export default function FilterBar({
  buildingId,
  onBuildingIdChange,
  systemId,
  onSystemIdChange,
  equipmentType,
  onEquipmentTypeChange,
  equipmentId,
  onEquipmentIdChange,
  subEquipmentScope,
  onSubEquipmentScopeChange,
  showBuildingId = false,
  showSystemId = false,
  showEquipmentType = true,
  showEquipmentId = false,
  showSubEquipmentScope = false,
  fixedEquipmentType,
  qualityLevel,
  onQualityLevelChange,
  granularity,
  onGranularityChange,
  showGranularity = false,
  showQualityLevel = false,
}: Props) {
  const effectiveEqType = fixedEquipmentType ?? equipmentType;
  const { data: eqData } = useEquipmentIds(effectiveEqType, showEquipmentId);
  const shouldLoadSubScopes =
    showSubEquipmentScope && (!!buildingId || !!systemId || !!effectiveEqType || !!equipmentId);
  const { data: subScopeData } = useSubEquipmentScopes(
    {
      building_id: buildingId,
      system_id: systemId,
      equipment_type: effectiveEqType,
      equipment_id: equipmentId,
    },
    shouldLoadSubScopes,
  );
  const equipmentIdOptions = [
    { value: '', label: '全部设备' },
    ...(eqData?.items?.map((item) => ({
      value: item.equipment_id,
      label: item.equipment_id,
    })) ?? []),
  ];

  const systemOptions = buildingId
    ? [{ value: '', label: '全部系统' }, ...(SYSTEM_MAP[buildingId] ?? [])]
    : [
        { value: '', label: '全部系统' },
        ...Object.values(SYSTEM_MAP).flat(),
      ];

  const availableSubScopes = useMemo(() => {
    if (!shouldLoadSubScopes) {
      return ['main', 'backup', 'null'] as const;
    }
    return subScopeData?.available_scopes ?? (['main', 'backup', 'null'] as const);
  }, [shouldLoadSubScopes, subScopeData?.available_scopes]);

  const subScopeOptions = useMemo(() => {
    const allScopeOptions: Array<{ value: 'main' | 'backup' | 'null'; label: string }> = [
      { value: 'main', label: '主机(main)' },
      { value: 'backup', label: '备机(backup)' },
      { value: 'null', label: '未区分(null)' },
    ];
    const visibleScopeOptions = allScopeOptions.filter((item) =>
      availableSubScopes.includes(item.value),
    );

    if (visibleScopeOptions.length <= 1) {
      return visibleScopeOptions;
    }
    return [
      { value: 'all' as const, label: '全部(主/备/null)' },
      ...visibleScopeOptions,
    ];
  }, [availableSubScopes]);

  const selectedSubScopeValue = useMemo(() => {
    if (subScopeOptions.length === 0) {
      return undefined;
    }
    const current = subEquipmentScope ?? 'all';
    const allowed = new Set(subScopeOptions.map((item) => item.value));
    if (allowed.has(current)) {
      return current;
    }
    return subScopeOptions[0]!.value;
  }, [subEquipmentScope, subScopeOptions]);

  useEffect(() => {
    if (!showSubEquipmentScope) {
      return;
    }
    if (subScopeOptions.length === 0) {
      if (subEquipmentScope && subEquipmentScope !== 'all') {
        onSubEquipmentScopeChange?.('all');
      }
      return;
    }
    const current = subEquipmentScope ?? 'all';
    const allowed = new Set(subScopeOptions.map((item) => item.value));
    if (!allowed.has(current)) {
      onSubEquipmentScopeChange?.(subScopeOptions[0]!.value);
    }
  }, [
    onSubEquipmentScopeChange,
    showSubEquipmentScope,
    subEquipmentScope,
    subScopeOptions,
  ]);

  return (
    <Space wrap>
      {showBuildingId && (
        <Select
          placeholder="机楼"
          style={{ width: 120 }}
          value={buildingId ?? ''}
          onChange={(v: string) => {
            onBuildingIdChange?.(v === '' ? undefined : v);
            onSystemIdChange?.(undefined);
            onEquipmentTypeChange?.(undefined);
            onEquipmentIdChange?.(undefined);
          }}
          options={BUILDING_OPTIONS}
        />
      )}
      {showSystemId && (
        <Select
          placeholder="系统"
          style={{ width: 120 }}
          value={systemId ?? ''}
          onChange={(v: string) => {
            onSystemIdChange?.(v === '' ? undefined : v);
            onEquipmentIdChange?.(undefined);
          }}
          options={systemOptions}
        />
      )}
      {showEquipmentType && !fixedEquipmentType && (
        <Select
          placeholder="设备类型"
          style={{ width: 160 }}
          value={equipmentType ?? ''}
          onChange={(v: string) => {
            onEquipmentTypeChange?.(v === '' ? undefined : v);
            onEquipmentIdChange?.(undefined);
          }}
          options={EQUIPMENT_TYPE_OPTIONS}
        />
      )}
      {fixedEquipmentType && (
        <Select
          style={{ width: 160 }}
          value={fixedEquipmentType}
          disabled
          options={EQUIPMENT_TYPE_OPTIONS}
        />
      )}
      {showEquipmentId && (
        <Select
          placeholder="设备ID"
          style={{ width: 200 }}
          showSearch
          value={equipmentId ?? ''}
          onChange={(v: string) =>
            onEquipmentIdChange?.(v === '' ? undefined : v)
          }
          options={equipmentIdOptions}
        />
      )}
      {showSubEquipmentScope && subScopeOptions.length > 0 && (
        <Select
          placeholder="主备口径"
          style={{ width: 180 }}
          value={selectedSubScopeValue}
          onChange={(v: 'all' | 'main' | 'backup' | 'null') =>
            onSubEquipmentScopeChange?.(v)
          }
          options={subScopeOptions}
        />
      )}
      {showQualityLevel && (
        <Select
          placeholder="质量等级"
          allowClear
          style={{ width: 120 }}
          value={qualityLevel}
          onChange={onQualityLevelChange}
          options={[
            { value: 'good', label: '良好' },
            { value: 'warning', label: '警告' },
            { value: 'poor', label: '差' },
          ]}
        />
      )}
      {showGranularity && (
        <Select
          style={{ width: 120 }}
          value={granularity ?? 'hour'}
          onChange={onGranularityChange}
          options={[
            { value: 'hour', label: '按小时' },
            { value: 'day', label: '按天' },
          ]}
        />
      )}
    </Space>
  );
}
