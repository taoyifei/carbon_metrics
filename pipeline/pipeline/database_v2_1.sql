-- ============================================================
-- 制冷系统数据库 V2.1 - 增量更新脚本
-- ============================================================
-- 版本: 2.1.0
-- 日期: 2025-02-04
-- 说明: 
--   1. 添加指标定义表 (metric_definition)
--   2. 修改 metric_result 表结构
--   3. 更新 equipment_type 注释
--   4. 确保水泵类型区分 (chilled_pump/cooling_pump)
-- 执行: mysql -u root -p cooling_system_v2 < database_update_v2.1.sql
-- ============================================================

USE cooling_system_v2;

-- ============================================================
-- 1. 新增指标定义表
-- 用途: 存储所有计算指标的元数据定义
-- ============================================================

DROP TABLE IF EXISTS metric_definition;
CREATE TABLE metric_definition (
    id INT AUTO_INCREMENT PRIMARY KEY,
    
    -- 指标标识
    metric_code VARCHAR(64) NOT NULL COMMENT '指标代码（英文唯一标识）',
    metric_name VARCHAR(128) NOT NULL COMMENT '指标名称（中文）',
    
    -- 分类
    category_code VARCHAR(32) NOT NULL COMMENT '分类代码',
    category_name VARCHAR(64) NOT NULL COMMENT '分类名称',
    
    -- 计算定义
    formula TEXT NOT NULL COMMENT '计算公式（人类可读）',
    formula_expr TEXT DEFAULT NULL COMMENT '计算表达式（可执行）',
    
    -- 数据来源
    required_metrics JSON NOT NULL COMMENT '依赖的原始指标列表',
    required_params JSON DEFAULT NULL COMMENT '依赖的额定参数列表',
    
    -- 适用范围
    applicable_levels JSON NOT NULL COMMENT '适用层级 [1-6]',
    applicable_equipment_types JSON DEFAULT NULL COMMENT '适用设备类型',
    
    -- 聚合规则
    time_granularity JSON NOT NULL COMMENT '支持的时间粒度 ["hour","day","week","month"]',
    agg_method ENUM('avg', 'sum', 'max', 'min', 'last', 'ratio', 'std', 'custom') 
        NOT NULL COMMENT '聚合方式',
    
    -- 显示配置
    unit VARCHAR(32) DEFAULT NULL COMMENT '单位',
    decimal_places TINYINT DEFAULT 2 COMMENT '小数位数',
    display_format VARCHAR(64) DEFAULT NULL COMMENT '显示格式',
    
    -- 阈值配置
    warning_threshold DOUBLE DEFAULT NULL COMMENT '警告阈值',
    critical_threshold DOUBLE DEFAULT NULL COMMENT '严重阈值',
    baseline_value DOUBLE DEFAULT NULL COMMENT '基准值（用于偏离率计算）',
    
    -- 元数据
    description TEXT DEFAULT NULL COMMENT '指标说明',
    calc_notes TEXT DEFAULT NULL COMMENT '计算注意事项',
    is_active TINYINT(1) DEFAULT 1 COMMENT '是否启用',
    sort_order INT DEFAULT 100 COMMENT '排序',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_code (metric_code),
    INDEX idx_category (category_code, is_active),
    INDEX idx_active (is_active, sort_order)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指标定义表';


-- ============================================================
-- 2. 修改 metric_result 表
-- 添加 sub_equipment_id、统计类型、基准值等字段
-- 注意: 使用简单SQL，忽略已存在的列/索引错误
-- ============================================================

-- 添加新列（如果报错说明已存在，可忽略）
-- sub_equipment_id
SET @col_exists = (SELECT COUNT(*) FROM information_schema.columns 
    WHERE table_schema = DATABASE() AND table_name = 'metric_result' AND column_name = 'sub_equipment_id');
SET @sql = IF(@col_exists = 0, 
    'ALTER TABLE metric_result ADD COLUMN sub_equipment_id VARCHAR(64) DEFAULT NULL COMMENT ''子设备编号'' AFTER equipment_id', 
    'SELECT ''sub_equipment_id already exists''');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- metric_code
SET @col_exists = (SELECT COUNT(*) FROM information_schema.columns 
    WHERE table_schema = DATABASE() AND table_name = 'metric_result' AND column_name = 'metric_code');
SET @sql = IF(@col_exists = 0, 
    'ALTER TABLE metric_result ADD COLUMN metric_code VARCHAR(64) DEFAULT NULL COMMENT ''关联 metric_definition.metric_code'' AFTER metric_category', 
    'SELECT ''metric_code already exists''');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- stat_type
SET @col_exists = (SELECT COUNT(*) FROM information_schema.columns 
    WHERE table_schema = DATABASE() AND table_name = 'metric_result' AND column_name = 'stat_type');
SET @sql = IF(@col_exists = 0, 
    'ALTER TABLE metric_result ADD COLUMN stat_type ENUM(''value'', ''avg'', ''max'', ''min'', ''sum'', ''std'', ''ratio'', ''delta'') DEFAULT ''value'' COMMENT ''统计类型'' AFTER value', 
    'SELECT ''stat_type already exists''');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- baseline_value
SET @col_exists = (SELECT COUNT(*) FROM information_schema.columns 
    WHERE table_schema = DATABASE() AND table_name = 'metric_result' AND column_name = 'baseline_value');
SET @sql = IF(@col_exists = 0, 
    'ALTER TABLE metric_result ADD COLUMN baseline_value DOUBLE DEFAULT NULL COMMENT ''基准值'' AFTER stat_type', 
    'SELECT ''baseline_value already exists''');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- deviation_pct
SET @col_exists = (SELECT COUNT(*) FROM information_schema.columns 
    WHERE table_schema = DATABASE() AND table_name = 'metric_result' AND column_name = 'deviation_pct');
SET @sql = IF(@col_exists = 0, 
    'ALTER TABLE metric_result ADD COLUMN deviation_pct DOUBLE DEFAULT NULL COMMENT ''偏离率(%)'' AFTER baseline_value', 
    'SELECT ''deviation_pct already exists''');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 添加索引 idx_metric_code（如果不存在）
SET @idx_exists = (SELECT COUNT(*) FROM information_schema.statistics 
    WHERE table_schema = DATABASE() AND table_name = 'metric_result' AND index_name = 'idx_metric_code');
SET @sql = IF(@idx_exists = 0, 
    'ALTER TABLE metric_result ADD INDEX idx_metric_code (metric_code, bucket_time)', 
    'SELECT ''idx_metric_code already exists''');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 更新唯一键（检查是否需要更新）
SET @uk_cols = (SELECT GROUP_CONCAT(column_name ORDER BY seq_in_index) 
                FROM information_schema.statistics 
                WHERE table_schema = DATABASE() AND table_name = 'metric_result' AND index_name = 'uk_metric');

SET @needs_update = IF(@uk_cols IS NULL OR @uk_cols NOT LIKE '%sub_equipment_id%', 1, 0);

-- 只有当唯一键不包含 sub_equipment_id 时才更新
SET @sql = IF(@needs_update = 1,
    'ALTER TABLE metric_result DROP INDEX uk_metric, ADD UNIQUE KEY uk_metric (bucket_time, bucket_type, metric_name, building_id, system_id, equipment_type, equipment_id, sub_equipment_id)',
    'SELECT ''uk_metric already updated''');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


-- ============================================================
-- 3. 初始化指标定义数据
-- 基于《数据中心制冷系统指标梳理V4》
-- 使用 INSERT IGNORE 避免重复插入错误
-- ============================================================

INSERT IGNORE INTO metric_definition 
    (metric_code, metric_name, category_code, category_name, formula, required_metrics, 
     applicable_levels, time_granularity, agg_method, unit, description, sort_order)
VALUES

-- ==================== 一、系统级能效指标 ====================
('pue', 'PUE', 'efficiency', '系统级能效指标',
 '总用电量 / IT用电量',
 '["total_energy", "it_energy"]',
 '[1,2,3]', '["hour","day","week","month"]', 'ratio', NULL,
 'Power Usage Effectiveness，数据中心能效比', 101),

('hvac_efficiency', '空调系统能效比', 'efficiency', '系统级能效指标',
 'IT负荷制冷量 / 空调系统用电量',
 '["cooling_capacity", "hvac_energy"]',
 '[1,2,3]', '["hour","day","week","month"]', 'ratio', NULL,
 '空调系统能效比 = 冷冻水流量×温差×4.186 / 空调系统用电量', 102),

('system_cop', '系统综合能效比', 'efficiency', '系统级能效指标',
 '总制冷量 / 系统总输入功率',
 '["total_cooling_capacity", "total_power"]',
 '[1,2,3]', '["hour","day","week","month"]', 'ratio', NULL,
 '系统综合COP', 103),

('pue_std', '系统能效标准差', 'efficiency', '系统级能效指标',
 '各系统PUE的标准差',
 '["pue"]',
 '[1,2]', '["day","week","month"]', 'std', NULL,
 '各系统PUE一致性评估', 104),

-- ==================== 二、能耗结构指标 ====================
('chiller_energy_ratio', '冷机能耗占比', 'energy_structure', '能耗结构指标',
 '冷机电量 / 系统总电量',
 '["chiller_energy", "total_energy"]',
 '[1,2,3]', '["hour","day","week","month"]', 'ratio', '%',
 '冷机在制冷系统中的能耗占比', 201),

('pump_energy_ratio', '水泵能耗占比', 'energy_structure', '能耗结构指标',
 '(冷冻泵+冷却泵)电量 / 系统总电量',
 '["chilled_pump_energy", "cooling_pump_energy", "total_energy"]',
 '[1,2,3]', '["hour","day","week","month"]', 'ratio', '%',
 '水泵（冷冻泵+冷却泵）在制冷系统中的能耗占比', 202),

('tower_energy_ratio', '风机能耗占比', 'energy_structure', '能耗结构指标',
 '冷塔风机电量 / 系统总电量',
 '["tower_energy", "total_energy"]',
 '[1,2,3]', '["hour","day","week","month"]', 'ratio', '%',
 '冷却塔风机在制冷系统中的能耗占比', 203),

('total_energy', '总能耗', 'energy_structure', '能耗结构指标',
 '冷机+冷冻泵+冷却泵+冷塔电量',
 '["chiller_energy", "chilled_pump_energy", "cooling_pump_energy", "tower_energy"]',
 '[1,2,3]', '["hour","day","week","month"]', 'sum', 'kWh',
 '制冷系统四类设备总电量', 204),

-- ==================== 三、冷机运行效率指标 ====================
('chiller_avg_load', '冷机平均负载率', 'chiller_efficiency', '冷机运行效率指标',
 '平均负载率',
 '["load_rate"]',
 '[3,4,5]', '["hour","day","week","month"]', 'avg', '%',
 '冷机平均负载率', 301),

('chiller_max_load', '冷机最大负载率', 'chiller_efficiency', '冷机运行效率指标',
 '最大负载率',
 '["load_rate"]',
 '[3,4,5]', '["hour","day","week","month"]', 'max', '%',
 '冷机最大负载率', 302),

('chiller_load_cv', '冷机负载波动系数', 'chiller_efficiency', '冷机运行效率指标',
 '标准差 / 平均值',
 '["load_rate"]',
 '[3,4,5]', '["day","week","month"]', 'custom', NULL,
 '冷机负载稳定性指标，值越小越稳定', 303),

('chiller_cop', '冷机COP', 'chiller_efficiency', '冷机运行效率指标',
 '制冷量 / 冷机输入功率',
 '["chilled_flow", "chilled_supply_temp", "chilled_return_temp", "chiller_power"]',
 '[3,4,5]', '["hour","day","week","month"]', 'ratio', NULL,
 'COP = 冷冻水流量×温差×4.186 / 冷机功率', 304),

('chiller_efficiency_ratio', '冷机能效比', 'chiller_efficiency', '冷机运行效率指标',
 '实际COP / 额定COP',
 '["chiller_cop", "rated_cop"]',
 '[5]', '["hour","day","week","month"]', 'ratio', '%',
 '实际运行效率与额定效率的比值', 305),

('chiller_start_count', '冷机启停次数', 'chiller_efficiency', '冷机运行效率指标',
 '启停次数统计',
 '["run_status"]',
 '[4,5]', '["day","week","month"]', 'sum', '次',
 '冷机启停次数，频繁启停会增加能耗和磨损', 306),

-- ==================== 四、水泵效率指标 ====================
('chilled_pump_frequency', '冷冻泵工作频率', 'pump_efficiency', '水泵效率指标',
 '实际频率',
 '["frequency"]',
 '[5]', '["hour","day"]', 'avg', 'Hz',
 '冷冻泵变频器输出频率', 401),

('cooling_pump_frequency', '冷却泵工作频率', 'pump_efficiency', '水泵效率指标',
 '实际频率',
 '["frequency"]',
 '[5]', '["hour","day"]', 'avg', 'Hz',
 '冷却泵变频器输出频率', 402),

('chilled_pump_energy_density', '冷冻泵能耗密度', 'pump_efficiency', '水泵效率指标',
 '冷冻泵耗电量 / 冷冻水流量',
 '["chilled_pump_energy", "chilled_flow"]',
 '[3,4]', '["hour","day","week","month"]', 'ratio', 'kWh/m³',
 '单位流量的能耗', 403),

('cooling_pump_energy_density', '冷却泵能耗密度', 'pump_efficiency', '水泵效率指标',
 '冷却泵耗电量 / 冷却水流量',
 '["cooling_pump_energy", "cooling_flow"]',
 '[3,4]', '["hour","day","week","month"]', 'ratio', 'kWh/m³',
 '单位流量的能耗', 404),

('pump_power_utilization', '水泵功率利用率', 'pump_efficiency', '水泵效率指标',
 '实际功率 / 额定功率',
 '["power", "rated_power_kw"]',
 '[5]', '["hour","day"]', 'ratio', '%',
 '水泵功率利用率', 405),

('pump_efficiency', '水泵效率', 'pump_efficiency', '水泵效率指标',
 '扬程×流量 / 输入功率',
 '["head_m", "flow", "power"]',
 '[5]', '["hour","day"]', 'ratio', '%',
 '水泵机械效率', 406),

-- ==================== 五、冷却塔效率指标 ====================
('cooling_water_delta_t', '冷却水温差', 'tower_efficiency', '冷却塔效率指标',
 '回水温度 - 供水温度',
 '["cooling_return_temp", "cooling_supply_temp"]',
 '[3,4]', '["hour","day"]', 'avg', '℃',
 '冷却塔进出水温差', 501),

('tower_fan_power', '冷却塔风机功率', 'tower_efficiency', '冷却塔效率指标',
 '实际功率',
 '["power"]',
 '[5,6]', '["hour","day"]', 'avg', 'kW',
 '冷却塔风机实际功率', 502),

('tower_efficiency', '冷却塔效率', 'tower_efficiency', '冷却塔效率指标',
 '(回水温度 - 供水温度) / 冷却塔耗电量',
 '["cooling_return_temp", "cooling_supply_temp", "tower_energy"]',
 '[3,4,5]', '["hour","day"]', 'ratio', '℃/kWh',
 '单位能耗的冷却效果', 503),

('tower_fan_runtime', '风机累计运行时长', 'tower_efficiency', '冷却塔效率指标',
 '总运行时间',
 '["runtime"]',
 '[5,6]', '["day","week","month"]', 'sum', 'h',
 '风机累计运行小时数', 504),

('tower_approach', '冷却塔逼近度', 'tower_efficiency', '冷却塔效率指标',
 '回水温度 - 湿球温度',
 '["cooling_return_temp", "wet_bulb_temp"]',
 '[3,4,5]', '["hour","day"]', 'avg', '℃',
 '冷却塔出水温度与湿球温度的差值，值越小效率越高', 505),

-- ==================== 六、温度与温差指标 ====================
('chilled_supply_temp', '冷冻水供水温度', 'temperature', '温度与温差指标',
 '实际供水温度',
 '["chilled_supply_temp"]',
 '[3,4]', '["hour","day"]', 'avg', '℃',
 '冷冻水供水温度', 601),

('chilled_return_temp', '冷冻水回水温度', 'temperature', '温度与温差指标',
 '实际回水温度',
 '["chilled_return_temp"]',
 '[3,4]', '["hour","day"]', 'avg', '℃',
 '冷冻水回水温度', 602),

('chilled_water_delta_t', '冷冻水温差', 'temperature', '温度与温差指标',
 '回水 - 供水',
 '["chilled_return_temp", "chilled_supply_temp"]',
 '[3,4]', '["hour","day"]', 'avg', '℃',
 '冷冻水供回水温差', 603),

('cooling_supply_temp', '冷却水供水温度', 'temperature', '温度与温差指标',
 '实际供水温度',
 '["cooling_supply_temp"]',
 '[3,4]', '["hour","day"]', 'avg', '℃',
 '冷却水供水温度', 604),

('cooling_return_temp', '冷却水回水温度', 'temperature', '温度与温差指标',
 '实际回水温度',
 '["cooling_return_temp"]',
 '[3,4]', '["hour","day"]', 'avg', '℃',
 '冷却水回水温度', 605),

('temp_deviation_ratio', '温差偏离率', 'temperature', '温度与温差指标',
 '(实际-标准) / 标准',
 '["chilled_water_delta_t"]',
 '[3,4]', '["hour","day"]', 'ratio', '%',
 '实际温差与设计温差的偏离程度', 606),

-- ==================== 七、流量效率指标 ====================
('chilled_flow', '冷冻水流量', 'flow', '流量效率指标',
 '实际流量',
 '["chilled_flow"]',
 '[3,4]', '["hour","day"]', 'avg', 'm³/h',
 '冷冻水流量', 701),

('cooling_flow', '冷却水流量', 'flow', '流量效率指标',
 '实际流量',
 '["cooling_flow"]',
 '[3,4]', '["hour","day"]', 'avg', 'm³/h',
 '冷却水流量', 702),

('flow_utilization', '流量利用率', 'flow', '流量效率指标',
 '实际流量 / 额定流量',
 '["flow", "rated_flow"]',
 '[3,4]', '["hour","day"]', 'ratio', '%',
 '流量利用率', 703),

('cooling_capacity', '制冷量', 'flow', '流量效率指标',
 '流量 × 温差 × 4.186',
 '["chilled_flow", "chilled_water_delta_t"]',
 '[3,4]', '["hour","day"]', 'avg', 'kW',
 '制冷量 = 流量(m³/h) × 温差(℃) × 4.186 / 3.6', 704),

-- ==================== 八、运行稳定性指标 ====================
('chiller_runtime_ratio', '冷机运行时长占比', 'stability', '运行稳定性指标',
 '运行时长 / 评估周期',
 '["runtime"]',
 '[4,5]', '["day","week","month"]', 'ratio', '%',
 '冷机运行时长占比', 801),

('chilled_pump_runtime_ratio', '冷冻泵运行时长占比', 'stability', '运行稳定性指标',
 '运行时长 / 评估周期',
 '["runtime"]',
 '[4,5]', '["day","week","month"]', 'ratio', '%',
 '冷冻泵运行时长占比', 802),

('cooling_pump_runtime_ratio', '冷却泵运行时长占比', 'stability', '运行稳定性指标',
 '运行时长 / 评估周期',
 '["runtime"]',
 '[4,5]', '["day","week","month"]', 'ratio', '%',
 '冷却泵运行时长占比', 803),

('tower_fan_runtime_ratio', '风机运行时长占比', 'stability', '运行稳定性指标',
 '运行时长 / 评估周期',
 '["runtime"]',
 '[5,6]', '["day","week","month"]', 'ratio', '%',
 '冷却塔风机运行时长占比', 804),

-- ==================== 九、预测性维护指标 ====================
('equipment_aging', '设备老化系数', 'maintenance', '预测性维护指标',
 '累计运行时长 / 设计寿命',
 '["runtime", "design_lifetime"]',
 '[5]', '["month"]', 'ratio', NULL,
 '设备老化程度评估', 901),

('chiller_degradation', '冷机性能退化率', 'maintenance', '预测性维护指标',
 '(当前COP - 初始COP) / 初始COP',
 '["chiller_cop", "initial_cop"]',
 '[5]', '["month"]', 'ratio', '%',
 '冷机性能退化评估', 902),

('pump_vfd_risk', '水泵变频失效风险', 'maintenance', '预测性维护指标',
 '频率固定不变的时长 / 总运行时长',
 '["frequency", "runtime"]',
 '[5]', '["week","month"]', 'ratio', '%',
 '变频器可能故障的风险评估', 903),

('chiller_overload_risk', '过载风险指数', 'maintenance', '预测性维护指标',
 '(负载率-80%) / 80%',
 '["load_rate"]',
 '[5]', '["hour","day"]', 'avg', NULL,
 '冷机过载运行风险评估，>0表示存在过载风险', 904),

-- ==================== 十、节能潜力评估指标 ====================
('vfd_saving_potential', '变频节能潜力', 'saving', '节能潜力评估指标',
 '(定频功耗-变频功耗) × 时间',
 '["power", "frequency"]',
 '[4,5]', '["month"]', 'sum', 'kWh',
 '变频改造节能潜力评估', 1001),

('delta_t_optimization', '温差优化潜力', 'saving', '节能潜力评估指标',
 '流量减少 × 泵功率 × 优化率',
 '["chilled_water_delta_t", "flow", "pump_power"]',
 '[3,4]', '["month"]', 'sum', 'kWh',
 '通过优化温差降低流量的节能潜力', 1002),

('load_optimization', '负载优化潜力', 'saving', '节能潜力评估指标',
 'COP提升差值 × 能耗',
 '["chiller_cop", "chiller_energy"]',
 '[3,4]', '["month"]', 'sum', 'kWh',
 '通过优化冷机负载提升COP的节能潜力', 1003),

('fan_optimization', '风机优化潜力', 'saving', '节能潜力评估指标',
 '功率降低 × 时间',
 '["tower_fan_power", "runtime"]',
 '[4,5]', '["month"]', 'sum', 'kWh',
 '冷却塔风机优化运行的节能潜力', 1004);


-- ============================================================
-- 4. 更新 source_config 表
-- 确保水泵区分冷冻泵和冷却泵
-- ============================================================

-- 更新 pump 相关配置，明确区分冷冻泵和冷却泵
UPDATE source_config 
SET target_equipment_type = 'chilled_pump',
    description = '冷冻泵电量'
WHERE source_name = 'pump_energy' AND target_equipment_type = 'pump';

UPDATE source_config 
SET target_equipment_type = 'chilled_pump',
    description = '冷冻泵功率'
WHERE source_name = 'pump_power' AND target_equipment_type = 'pump';

-- 添加冷却泵配置（如果不存在）
INSERT IGNORE INTO source_config 
    (source_name, directory_pattern, filename_pattern, schema_type, 
     target_equipment_type, target_metric_name, time_column, value_column, key_column, description)
VALUES
('cooling_pump_energy', '水泵功率和电量', '.*冷却泵.*电量.*', 'device', 'cooling_pump', 'energy', 'record_time', 'record_value', 'device_path', '冷却泵电量'),
('cooling_pump_power', '水泵功率和电量', '.*冷却泵.*功率.*', 'device', 'cooling_pump', 'power', 'record_time', 'record_value', 'device_path', '冷却泵功率'),
('chilled_pump_energy', '水泵功率和电量', '.*冷冻泵.*电量.*', 'device', 'chilled_pump', 'energy', 'record_time', 'record_value', 'device_path', '冷冻泵电量'),
('chilled_pump_power', '水泵功率和电量', '.*冷冻泵.*功率.*', 'device', 'chilled_pump', 'power', 'record_time', 'record_value', 'device_path', '冷冻泵功率');


-- ============================================================
-- 5. 更新 meter_config 表
-- 添加所有泵类型的配置
-- ============================================================

INSERT INTO meter_config (equipment_type, metric_name, meter_max, max_delta_per_hour)
VALUES
('chilled_pump', 'power', 200, NULL),
('cooling_pump', 'power', 200, NULL),
('closed_tower_pump', 'power', 200, NULL),
('closed_tower_pump', 'energy', 99999999, 200),
('user_side_pump', 'power', 200, NULL),
('user_side_pump', 'energy', 99999999, 200),
('source_side_pump', 'power', 200, NULL),
('source_side_pump', 'energy', 99999999, 200),
('heat_recovery_primary_pump', 'power', 200, NULL),
('heat_recovery_primary_pump', 'energy', 99999999, 200),
('heat_recovery_secondary_pump', 'power', 200, NULL),
('heat_recovery_secondary_pump', 'energy', 99999999, 200),
('fire_pump', 'power', 200, NULL),
('fire_pump', 'energy', 99999999, 200),
('unknown_pump', 'power', 200, NULL),
('unknown_pump', 'energy', 99999999, 200)
ON DUPLICATE KEY UPDATE meter_max = VALUES(meter_max);


-- ============================================================
-- 6. 记录 Schema 版本（避免重复插入）
-- ============================================================

INSERT IGNORE INTO schema_version (table_name, version, change_type, change_desc, applied_by) VALUES
('metric_definition', 'v2.1.0', 'create', '新增指标定义表，存储48个计算指标的元数据', 'update_script'),
('metric_result', 'v2.1.0', 'add_column', '添加 sub_equipment_id, metric_code, stat_type, baseline_value, deviation_pct 字段', 'update_script'),
('source_config', 'v2.1.0', 'modify_column', '区分冷冻泵/冷却泵配置', 'update_script');


-- ============================================================
-- 完成提示
-- ============================================================
SELECT '=====================================================' AS '';
SELECT 'cooling_system_v2 数据库更新完成! (v2.1.0)' AS Message;
SELECT '=====================================================' AS '';
SELECT CONCAT('metric_definition 指标数量: ', COUNT(*)) AS Message FROM metric_definition;
SELECT '=====================================================' AS '';