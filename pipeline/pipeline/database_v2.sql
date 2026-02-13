-- ============================================================
-- 制冷系统数据库 V2.0 - 完整建库脚本
-- ============================================================
-- 版本: 2.0.0
-- 日期: 2025-02-04
-- 说明: 
--   本脚本创建完整的数据库结构，支持从Excel导入数据并计算指标
--   数据来源: 冷机/水泵/冷塔的功率电量、温度、流量、运行状态等
--   缺失数据: IT用电量（影响PUE计算）、部分冷却泵功率
-- 执行: mysql -u root -p < database_v2.sql
-- ============================================================

-- 创建数据库
CREATE DATABASE IF NOT EXISTS cooling_system_v2 
    DEFAULT CHARACTER SET utf8mb4 
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE cooling_system_v2;

-- ============================================================
-- 1. 元数据管理表
-- ============================================================

-- Schema版本管理
DROP TABLE IF EXISTS schema_version;
CREATE TABLE schema_version (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(64) NOT NULL COMMENT '表名',
    version VARCHAR(16) NOT NULL COMMENT '版本号',
    change_type ENUM('create', 'add_column', 'modify_column', 'add_index', 'drop') NOT NULL,
    change_desc TEXT COMMENT '变更说明',
    applied_by VARCHAR(64) COMMENT '执行者',
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_table (table_name),
    INDEX idx_version (version)
) ENGINE=InnoDB COMMENT='数据库版本管理';

-- ============================================================
-- 2. 数据源配置表
-- ============================================================

DROP TABLE IF EXISTS source_config;
CREATE TABLE source_config (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source_name VARCHAR(64) NOT NULL COMMENT '数据源名称',
    directory_pattern VARCHAR(256) NOT NULL COMMENT '目录匹配模式',
    filename_pattern VARCHAR(256) DEFAULT NULL COMMENT '文件名匹配正则',
    schema_type ENUM('params', 'tag', 'device') NOT NULL COMMENT '数据格式类型',
    target_equipment_type VARCHAR(32) NOT NULL COMMENT '目标设备类型',
    target_metric_name VARCHAR(64) DEFAULT NULL COMMENT '目标指标名',
    time_column VARCHAR(64) DEFAULT 'record_time' COMMENT '时间列名',
    value_column VARCHAR(64) DEFAULT 'record_value' COMMENT '值列名',
    key_column VARCHAR(64) DEFAULT NULL COMMENT '关键字列名',
    priority INT DEFAULT 100 COMMENT '处理优先级',
    is_active TINYINT(1) DEFAULT 1 COMMENT '是否启用',
    description TEXT COMMENT '说明',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_source (source_name),
    INDEX idx_schema (schema_type, is_active)
) ENGINE=InnoDB COMMENT='数据源配置表';

-- ============================================================
-- 3. 设备主数据表
-- ============================================================

DROP TABLE IF EXISTS equipment_registry;
CREATE TABLE equipment_registry (
    id INT AUTO_INCREMENT PRIMARY KEY,
    
    -- 层级定位 (L2-L5)
    building_id VARCHAR(16) NOT NULL COMMENT '机楼ID: G11, G12',
    system_id VARCHAR(16) NOT NULL COMMENT '系统ID: G11-1, G11-2, G12-1等',
    equipment_type ENUM(
        'chiller',                          -- 冷机
        'chilled_pump',                     -- 冷冻泵
        'cooling_pump',                     -- 冷却泵
        'closed_tower_pump',                -- 闭式塔泵（含冷冻/冷却）
        'user_side_pump',                   -- 用户侧循环泵
        'source_side_pump',                 -- 水源侧循环泵
        'heat_recovery_primary_pump',       -- 余热回收一次泵
        'heat_recovery_secondary_pump',     -- 余热回收二次泵
        'fire_pump',                        -- 消防泵
        'unknown_pump',                     -- 未知泵（待人工分类）
        'cooling_tower',                    -- 开式冷却塔
        'cooling_tower_closed',             -- 闭式冷却塔
        'tower_fan'                         -- 冷塔风机
    ) NOT NULL COMMENT '设备类型',
    equipment_id VARCHAR(64) NOT NULL COMMENT '设备编号: chiller_01, pump_01等',
    
    -- 基础信息
    equipment_name VARCHAR(128) COMMENT '设备名称',
    device_code VARCHAR(64) COMMENT '原始设备编号',
    brand VARCHAR(64) COMMENT '品牌',
    model VARCHAR(128) COMMENT '型号',
    serial_number VARCHAR(64) COMMENT '序列号',
    location VARCHAR(128) COMMENT '位置',
    room VARCHAR(64) COMMENT '房间号',
    
    -- 额定参数（通用）
    rated_power_kw DECIMAL(10,2) COMMENT '额定功率(kW)',
    rated_voltage VARCHAR(32) COMMENT '额定电压',
    production_date DATE COMMENT '生产日期',
    
    -- 扩展参数（JSON存储各类型特有参数）
    -- 水泵: head_m, flow_rate_m3h, motor_speed_rpm, pump_speed_rpm, max_current_a
    -- 冷机: cooling_capacity_kw, rated_cop, refrigerant_charge_kg, evaporator_outlet_temp_c
    -- 冷塔: fan_count, cooling_capacity_kcal_h, water_treatment_capacity_m3h, fill_spec_mm
    extended_params JSON COMMENT '扩展参数(JSON)',
    
    -- 关联
    parent_equipment_id VARCHAR(64) COMMENT '父设备ID（冷塔风机关联冷塔）',
    
    -- 管理字段
    remarks TEXT COMMENT '备注',
    source_file VARCHAR(256) COMMENT '数据来源文件',
    is_active TINYINT(1) DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_equipment (system_id, equipment_type, equipment_id),
    INDEX idx_building (building_id),
    INDEX idx_type (equipment_type, is_active)
) ENGINE=InnoDB COMMENT='设备主数据表';

-- ============================================================
-- 4. 仪表配置表（用于数据校验）
-- ============================================================

DROP TABLE IF EXISTS meter_config;
CREATE TABLE meter_config (
    id INT AUTO_INCREMENT PRIMARY KEY,
    equipment_type VARCHAR(32) NOT NULL,
    metric_name VARCHAR(64) NOT NULL,
    meter_max DECIMAL(15,4) COMMENT '仪表最大值',
    max_delta_per_hour DECIMAL(15,4) COMMENT '每小时最大变化量',
    unit VARCHAR(16) COMMENT '单位',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_meter (equipment_type, metric_name)
) ENGINE=InnoDB COMMENT='仪表配置表';

-- ============================================================
-- 5. 数据导入批次表
-- ============================================================

DROP TABLE IF EXISTS ingest_batch;
CREATE TABLE ingest_batch (
    id INT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL COMMENT '批次ID',
    source_config_id INT COMMENT '关联source_config',
    source_directory VARCHAR(512) COMMENT '源目录',
    source_files JSON COMMENT '源文件列表',
    total_files INT DEFAULT 0,
    total_rows INT DEFAULT 0,
    success_rows INT DEFAULT 0,
    error_rows INT DEFAULT 0,
    status ENUM('running', 'success', 'partial', 'failed') DEFAULT 'running',
    error_message TEXT,
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_batch (batch_id),
    INDEX idx_status (status, created_at)
) ENGINE=InnoDB COMMENT='数据导入批次表';

-- ============================================================
-- 6. 原始数据表（第一层：原始导入）
-- ============================================================

DROP TABLE IF EXISTS raw_measurement;
CREATE TABLE raw_measurement (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL COMMENT '批次ID',
    source_config_id INT COMMENT '数据源配置ID',
    source_file VARCHAR(256) COMMENT '源文件名',
    
    -- 数据类型区分
    source_type ENUM('tag', 'device') NOT NULL COMMENT '数据源类型',
    
    -- Tag类数据字段
    tag_name VARCHAR(256) COMMENT 'Tag点位名称',
    
    -- Device类数据字段
    device_path VARCHAR(512) COMMENT '设备路径',
    location_path VARCHAR(512) COMMENT '位置路径',
    original_metric_name VARCHAR(128) COMMENT '原始指标名(如:正向有功电度)',
    
    -- 数据值
    ts DATETIME NOT NULL COMMENT '时间戳',
    value DOUBLE COMMENT '数值',
    unit VARCHAR(32) COMMENT '单位',
    
    -- 扩展
    extra_json JSON COMMENT '额外信息',
    
    INDEX idx_batch (batch_id),
    INDEX idx_ts (ts),
    INDEX idx_tag (source_type, tag_name(100)),
    INDEX idx_device (source_type, device_path(100))
) ENGINE=InnoDB COMMENT='原始测量数据表';

-- ============================================================
-- 7. 点位映射表
-- ============================================================

DROP TABLE IF EXISTS point_mapping;
CREATE TABLE point_mapping (
    id INT AUTO_INCREMENT PRIMARY KEY,
    
    -- 原始标识
    source_type ENUM('tag', 'device') NOT NULL,
    tag_name VARCHAR(256) COMMENT 'Tag点位名',
    device_path VARCHAR(512) COMMENT '设备路径',
    original_metric_name VARCHAR(128) COMMENT '原始指标名',
    
    -- 映射后的层级结构
    building_id VARCHAR(16) COMMENT '机楼',
    system_id VARCHAR(16) COMMENT '系统',
    equipment_type VARCHAR(32) COMMENT '设备类型',
    equipment_id VARCHAR(64) COMMENT '设备编号',
    sub_equipment_id VARCHAR(64) COMMENT '子设备编号（如风机）',
    
    -- 标准化指标
    metric_name VARCHAR(64) COMMENT '标准指标名',
    metric_category ENUM('instant', 'cumulative', 'status') DEFAULT 'instant' COMMENT '指标类别',
    agg_method ENUM('avg', 'sum', 'max', 'min', 'last', 'delta', 'first') DEFAULT 'avg' COMMENT '聚合方式',
    unit VARCHAR(32) COMMENT '单位',
    
    -- 映射质量
    confidence ENUM('high', 'medium', 'low') DEFAULT 'medium' COMMENT '映射置信度',
    is_active TINYINT(1) DEFAULT 1,
    remarks TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_tag (source_type, tag_name(100), device_path(100), original_metric_name(50)),
    INDEX idx_mapping (building_id, system_id, equipment_type, metric_name)
) ENGINE=InnoDB COMMENT='点位映射表';

-- ============================================================
-- 8. 标准化数据表（第二层：映射后）
-- ============================================================

DROP TABLE IF EXISTS canonical_measurement;
CREATE TABLE canonical_measurement (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    
    -- 时间
    ts DATETIME NOT NULL COMMENT '时间戳',
    
    -- 层级定位
    building_id VARCHAR(16) NOT NULL,
    system_id VARCHAR(16) NOT NULL,
    equipment_type VARCHAR(32) NOT NULL,
    equipment_id VARCHAR(64) COMMENT '设备编号（可为空表示系统级）',
    sub_equipment_id VARCHAR(64) COMMENT '子设备编号',
    
    -- 标准化数据
    point_key VARCHAR(256) COMMENT '点位唯一键',
    metric_name VARCHAR(64) NOT NULL COMMENT '标准指标名',
    value DOUBLE NOT NULL COMMENT '数值',
    unit VARCHAR(32) COMMENT '单位',
    
    -- 质量标记
    quality_flags VARCHAR(64) DEFAULT '' COMMENT '质量标记',
    
    -- 溯源
    raw_id BIGINT COMMENT '关联raw_measurement.id',
    batch_id VARCHAR(64) COMMENT '批次ID',
    mapping_version VARCHAR(16) DEFAULT 'v2.0.0',
    
    INDEX idx_ts (ts),
    INDEX idx_point (building_id, system_id, equipment_type, equipment_id, metric_name),
    INDEX idx_metric (metric_name, ts)
) ENGINE=InnoDB COMMENT='标准化测量数据表';

-- ============================================================
-- 9. 小时级聚合表（第三层）
-- ============================================================

DROP TABLE IF EXISTS agg_hour;
CREATE TABLE agg_hour (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    
    -- 时间桶
    bucket_time DATETIME NOT NULL COMMENT '小时开始时间',
    
    -- 层级定位
    building_id VARCHAR(16) NOT NULL,
    system_id VARCHAR(16) NOT NULL,
    equipment_type VARCHAR(32) NOT NULL,
    equipment_id VARCHAR(64) COMMENT '设备编号',
    sub_equipment_id VARCHAR(64) COMMENT '子设备编号',
    
    -- 指标
    metric_name VARCHAR(64) NOT NULL,
    
    -- 聚合值
    agg_avg DOUBLE COMMENT '平均值',
    agg_min DOUBLE COMMENT '最小值',
    agg_max DOUBLE COMMENT '最大值',
    agg_sum DOUBLE COMMENT '求和',
    agg_delta DOUBLE COMMENT '增量（末值-首值，用于累计量）',
    agg_first DOUBLE COMMENT '首值',
    agg_last DOUBLE COMMENT '末值',
    sample_count INT DEFAULT 0 COMMENT '样本数',
    
    -- 质量
    quality_flags VARCHAR(64) DEFAULT '',
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_agg (bucket_time, building_id, system_id, equipment_type, 
                       equipment_id, sub_equipment_id, metric_name),
    INDEX idx_query (system_id, equipment_type, metric_name, bucket_time)
) ENGINE=InnoDB COMMENT='小时级聚合表';

-- ============================================================
-- 10. 日级聚合表（第四层）
-- ============================================================

DROP TABLE IF EXISTS agg_day;
CREATE TABLE agg_day (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    
    -- 时间桶
    bucket_time DATE NOT NULL COMMENT '日期',
    
    -- 层级定位
    building_id VARCHAR(16) NOT NULL,
    system_id VARCHAR(16) NOT NULL,
    equipment_type VARCHAR(32) NOT NULL,
    equipment_id VARCHAR(64) COMMENT '设备编号',
    sub_equipment_id VARCHAR(64) COMMENT '子设备编号',
    
    -- 指标
    metric_name VARCHAR(64) NOT NULL,
    
    -- 聚合值
    agg_avg DOUBLE COMMENT '平均值',
    agg_min DOUBLE COMMENT '最小值',
    agg_max DOUBLE COMMENT '最大值',
    agg_sum DOUBLE COMMENT '求和',
    agg_delta DOUBLE COMMENT '日增量',
    agg_first DOUBLE COMMENT '首值',
    agg_last DOUBLE COMMENT '末值',
    sample_count INT DEFAULT 0 COMMENT '样本数',
    
    -- 质量
    quality_flags VARCHAR(64) DEFAULT '',
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_agg (bucket_time, building_id, system_id, equipment_type, 
                       equipment_id, sub_equipment_id, metric_name),
    INDEX idx_query (system_id, equipment_type, metric_name, bucket_time)
) ENGINE=InnoDB COMMENT='日级聚合表';

-- ============================================================
-- 11. 指标计算结果表
-- ============================================================

DROP TABLE IF EXISTS metric_result;
CREATE TABLE metric_result (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    
    -- 时间维度
    bucket_time DATETIME NOT NULL COMMENT '时间桶',
    bucket_type ENUM('hour', 'day', 'week', 'month') NOT NULL DEFAULT 'hour',
    
    -- 层级定位
    building_id VARCHAR(16) COMMENT '机楼',
    system_id VARCHAR(16) COMMENT '系统',
    equipment_type VARCHAR(32) COMMENT '设备类型',
    equipment_id VARCHAR(64) COMMENT '设备编号',
    sub_equipment_id VARCHAR(64) COMMENT '子设备编号',
    
    -- 指标信息
    metric_category VARCHAR(32) COMMENT '指标分类',
    metric_name VARCHAR(64) NOT NULL COMMENT '指标名称',
    metric_code VARCHAR(64) COMMENT '关联metric_definition',
    
    -- 计算结果
    value DOUBLE COMMENT '指标值',
    stat_type ENUM('value', 'avg', 'max', 'min', 'sum', 'std', 'ratio', 'delta') 
        DEFAULT 'value' COMMENT '统计类型',
    baseline_value DOUBLE COMMENT '基准值（额定值）',
    deviation_pct DOUBLE COMMENT '偏离率(%)',
    unit VARCHAR(32) COMMENT '单位',
    
    -- 计算元数据
    formula VARCHAR(256) COMMENT '计算公式',
    data_sources JSON COMMENT '数据来源',
    sql_trace_id VARCHAR(64) COMMENT 'SQL追踪ID',
    
    -- 层级回退信息
    granularity_requested TINYINT COMMENT '请求的层级(1-6)',
    granularity_actual TINYINT COMMENT '实际计算层级',
    fallback_reason VARCHAR(128) COMMENT '回退原因',
    
    -- 质量
    quality_flags VARCHAR(64) DEFAULT '',
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_metric (bucket_time, bucket_type, metric_name, building_id, 
                          system_id, equipment_type, equipment_id, sub_equipment_id),
    INDEX idx_code (metric_code, bucket_time),
    INDEX idx_query (system_id, metric_name, bucket_type, bucket_time)
) ENGINE=InnoDB COMMENT='指标计算结果表';

-- ============================================================
-- 12. 初始化数据源配置
-- 根据实际Excel文件结构配置
-- ============================================================

INSERT INTO source_config (source_name, directory_pattern, filename_pattern, schema_type, 
    target_equipment_type, target_metric_name, description, priority) VALUES

-- 参数表
('pump_params', '设备参数', '水泵.*参数\\.xlsx$', 'params', 'pump', NULL, '水泵额定参数', 10),
('chiller_params', '设备参数', '冷机.*参数\\.xlsx$', 'params', 'chiller', NULL, '冷机额定参数', 10),
('tower_params', '设备参数', '冷却塔.*参数\\.xlsx$', 'params', 'cooling_tower', NULL, '冷却塔额定参数', 10),

-- 冷机功率电量 (Device类)
('chiller_energy', '冷机功率和电量', '.*电量.*\\.xlsx$', 'device', 'chiller', 'energy', '冷机电量', 20),
('chiller_power', '冷机功率和电量', '.*功率.*\\.xlsx$', 'device', 'chiller', 'power', '冷机功率', 20),

-- 水泵功率电量 (Device类) - 区分冷冻泵和冷却泵
('chilled_pump_energy', '水泵功率和电量', '.*冷冻泵.*电量.*\\.xlsx$', 'device', 'chilled_pump', 'energy', '冷冻泵电量', 20),
('chilled_pump_power', '水泵功率和电量', '.*冷冻泵.*功率.*\\.xlsx$', 'device', 'chilled_pump', 'power', '冷冻泵功率', 20),
('cooling_pump_energy', '水泵功率和电量', '.*冷却泵.*电量.*\\.xlsx$', 'device', 'cooling_pump', 'energy', '冷却泵电量', 20),
('cooling_pump_power', '水泵功率和电量', '.*冷却泵.*功率.*\\.xlsx$', 'device', 'cooling_pump', 'power', '冷却泵功率（数据不全）', 20),

-- 冷塔功率电量 (Device类)
('tower_energy', '冷塔功率和电量', '.*电量.*\\.xlsx$', 'device', 'cooling_tower', 'energy', '冷却塔电量', 20),
('tower_power', '冷塔功率和电量', '.*功率.*\\.xlsx$', 'device', 'cooling_tower', 'power', '冷却塔功率', 20),

-- 温度数据 (Tag类)
('chilled_water_temp', '冷冻水供回水温度', '.*', 'tag', 'system', 'chilled_water_temp', '冷冻水供回水温度', 30),
('cooling_water_temp', '冷却水进出水温度', '.*', 'tag', 'system', 'cooling_water_temp', '冷却水进出水温度', 30),

-- 流量数据 (Tag类)
('water_flow', '冷冻水、冷却水流量', '.*', 'tag', 'system', 'flow', '冷冻水冷却水流量', 30),

-- 运行状态 (Tag类)
('chiller_run_status', '冷机运行状态', '.*', 'tag', 'chiller', 'run_status', '冷机运行状态', 30),
('pump_run_status', '水泵运行状态', '.*', 'tag', 'pump', 'run_status', '水泵运行状态', 30),
('tower_run_status', '冷却塔运行状态', '.*', 'tag', 'cooling_tower', 'run_status', '冷却塔运行状态', 30),

-- 水泵频率 (Tag类)
('pump_frequency', '水泵变频频率', '.*', 'tag', 'pump', 'frequency', '水泵变频频率', 30),

-- 冷机负载率 (Tag类)
('chiller_load_ratio', '冷机负载率', '.*', 'tag', 'chiller', 'load_rate', '冷机负载率', 30);

-- ============================================================
-- 13. 初始化仪表配置
-- ============================================================

INSERT INTO meter_config (equipment_type, metric_name, meter_max, max_delta_per_hour, unit) VALUES
-- 冷机
('chiller', 'power', 2000, NULL, 'kW'),
('chiller', 'energy', 99999999, 2000, 'kWh'),
('chiller', 'load_rate', 100, NULL, '%'),
-- 冷冻泵
('chilled_pump', 'power', 200, NULL, 'kW'),
('chilled_pump', 'energy', 99999999, 200, 'kWh'),
('chilled_pump', 'frequency', 60, NULL, 'Hz'),
-- 冷却泵
('cooling_pump', 'power', 200, NULL, 'kW'),
('cooling_pump', 'energy', 99999999, 200, 'kWh'),
('cooling_pump', 'frequency', 60, NULL, 'Hz'),
-- 冷却塔
('cooling_tower', 'power', 500, NULL, 'kW'),
('cooling_tower', 'energy', 99999999, 500, 'kWh'),
-- 温度
('system', 'chilled_supply_temp', 30, 5, '℃'),
('system', 'chilled_return_temp', 30, 5, '℃'),
('system', 'cooling_supply_temp', 50, 5, '℃'),
('system', 'cooling_return_temp', 50, 5, '℃'),
-- 流量
('system', 'chilled_flow', 10000, 2000, 'm³/h'),
('system', 'cooling_flow', 10000, 2000, 'm³/h');

-- ============================================================
-- 14. 记录Schema版本
-- ============================================================

INSERT INTO schema_version (table_name, version, change_type, change_desc, applied_by) VALUES
('cooling_system_v2', 'v2.0.0', 'create', '初始化数据库，创建完整表结构', 'init_script'),
('source_config', 'v2.0.0', 'create', '数据源配置表', 'init_script'),
('equipment_registry', 'v2.0.0', 'create', '设备主数据表', 'init_script'),
('raw_measurement', 'v2.0.0', 'create', '原始数据表', 'init_script'),
('point_mapping', 'v2.0.0', 'create', '点位映射表', 'init_script'),
('canonical_measurement', 'v2.0.0', 'create', '标准化数据表', 'init_script'),
('agg_hour', 'v2.0.0', 'create', '小时级聚合表', 'init_script'),
('agg_day', 'v2.0.0', 'create', '日级聚合表', 'init_script'),
('metric_result', 'v2.0.0', 'create', '指标结果表', 'init_script');

-- ============================================================
-- 完成提示
-- ============================================================

SELECT '=====================================================' AS '';
SELECT 'cooling_system_v2 数据库创建完成! (v2.0.0)' AS Message;
SELECT '=====================================================' AS '';
SELECT '下一步: 执行 database_v2_1.sql 添加指标定义表' AS Message;
SELECT '=====================================================' AS '';
