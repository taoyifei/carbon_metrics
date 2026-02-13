-- ============================================================
-- 幽灵设备清理脚本 (2026-02-11)
-- 用途：清理 equipment_registry 中由随机 UUID 产生的幽灵记录
-- ============================================================

-- Step 1: 统计幽灵设备数量
SELECT '=== Step 1: 幽灵设备统计 ===' AS step;
SELECT COUNT(*) AS phantom_count
FROM equipment_registry
WHERE equipment_id REGEXP '_[0-9a-f]{6}$'
  AND equipment_id NOT REGEXP '_h[0-9a-f]{6}$';

-- Step 2: 安全验证 — 确认幽灵设备在 agg_hour 中无实际数据
-- 期望返回 0 行，若有结果则需人工核查后再继续
SELECT '=== Step 2: 安全验证（应返回0行）===' AS step;
SELECT er.equipment_id, er.equipment_type, er.system_id
FROM equipment_registry er
WHERE er.equipment_id REGEXP '_[0-9a-f]{6}$'
  AND er.equipment_id NOT REGEXP '_h[0-9a-f]{6}$'
  AND EXISTS (
      SELECT 1 FROM agg_hour ah
      WHERE ah.equipment_id = er.equipment_id
        AND ah.equipment_type = er.equipment_type
  );

-- ============================================================
-- Step 3: 软删除（确认 Step 2 返回 0 行后再执行）
-- ============================================================
SELECT '=== Step 3: 软删除幽灵设备 ===' AS step;
UPDATE equipment_registry
SET is_active = 0,
    remarks = CONCAT(COALESCE(remarks, ''), ' [phantom-cleanup-2026-02-11]'),
    updated_at = CURRENT_TIMESTAMP
WHERE equipment_id REGEXP '_[0-9a-f]{6}$'
  AND equipment_id NOT REGEXP '_h[0-9a-f]{6}$';

-- Step 4: 验证 — 剩余活跃设备应约 116 台
SELECT '=== Step 4: 验证活跃设备 ===' AS step;
SELECT COUNT(*) AS active_count FROM equipment_registry WHERE is_active = 1;

SELECT equipment_type, COUNT(*) AS cnt
FROM equipment_registry
WHERE is_active = 1
GROUP BY equipment_type
ORDER BY cnt DESC;

-- ============================================================
-- Step 5: 硬删除（观察几天确认无误后再执行）
-- ============================================================
-- DELETE FROM equipment_registry
-- WHERE is_active = 0
--   AND remarks LIKE '%phantom-cleanup-2026-02-11%';
