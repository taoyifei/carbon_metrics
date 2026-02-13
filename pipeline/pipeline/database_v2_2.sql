-- ============================================================
-- database_v2_2.sql
-- 数据质量增强：新增 agg_hour_quality 表
-- 创建时间: 2026-02-05
-- ============================================================

USE cooling_system_v2;

-- ============================================================
-- 1. 小时聚合质量明细表
-- ============================================================

DROP TABLE IF EXISTS agg_hour_quality;
CREATE TABLE agg_hour_quality (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    -- 关联聚合记录（复合键关联，不用外键）
    bucket_time DATETIME NOT NULL COMMENT '小时开始时间',
    building_id VARCHAR(16) NOT NULL,
    system_id VARCHAR(16) NOT NULL,
    equipment_type VARCHAR(32) NOT NULL,
    equipment_id VARCHAR(64) COMMENT '设备编号',
    sub_equipment_id VARCHAR(64) COMMENT '子设备编号',
    metric_name VARCHAR(64) NOT NULL,

    -- 质量统计
    expected_samples INT DEFAULT 12 COMMENT '预期样本数（5分钟间隔=12条/小时）',
    actual_samples INT COMMENT '实际样本数',
    completeness_rate DECIMAL(5,2) COMMENT '完整率 %',

    -- 问题计数
    gap_count INT DEFAULT 0 COMMENT '时间缺口数（间隔>10分钟）',
    max_gap_seconds INT DEFAULT 0 COMMENT '最大缺口秒数',
    negative_count INT DEFAULT 0 COMMENT '负值数量',
    jump_count INT DEFAULT 0 COMMENT '异常跳变数量',
    out_of_range_count INT DEFAULT 0 COMMENT '超量程数量',

    -- 质量评分
    quality_score DECIMAL(5,2) DEFAULT 100 COMMENT '质量评分 0-100',
    quality_level ENUM('good', 'warning', 'poor') DEFAULT 'good' COMMENT '质量等级',

    -- 问题详情 JSON
    issues_json JSON COMMENT '问题详情列表',

    -- 时间戳
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- 索引
    UNIQUE KEY uk_quality (bucket_time, building_id, system_id, equipment_type,
                           equipment_id, sub_equipment_id, metric_name),
    INDEX idx_quality_level (quality_level),
    INDEX idx_completeness (completeness_rate),
    INDEX idx_equipment (equipment_type, equipment_id)
) ENGINE=InnoDB COMMENT='小时聚合质量明细表';

-- ============================================================
-- 2. 日聚合质量明细表
-- ============================================================

DROP TABLE IF EXISTS agg_day_quality;
CREATE TABLE agg_day_quality (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    -- 关联聚合记录
    bucket_time DATE NOT NULL COMMENT '日期',
    building_id VARCHAR(16) NOT NULL,
    system_id VARCHAR(16) NOT NULL,
    equipment_type VARCHAR(32) NOT NULL,
    equipment_id VARCHAR(64) COMMENT '设备编号',
    sub_equipment_id VARCHAR(64) COMMENT '子设备编号',
    metric_name VARCHAR(64) NOT NULL,

    -- 质量统计
    expected_hours INT DEFAULT 24 COMMENT '预期小时数',
    actual_hours INT COMMENT '实际有数据的小时数',
    completeness_rate DECIMAL(5,2) COMMENT '完整率 %',

    -- 问题汇总
    total_gap_hours DECIMAL(5,2) DEFAULT 0 COMMENT '总缺口小时数',
    total_negative_count INT DEFAULT 0 COMMENT '当日负值总数',
    total_jump_count INT DEFAULT 0 COMMENT '当日跳变总数',

    -- 质量评分
    quality_score DECIMAL(5,2) DEFAULT 100 COMMENT '质量评分 0-100',
    quality_level ENUM('good', 'warning', 'poor') DEFAULT 'good' COMMENT '质量等级',

    -- 问题详情 JSON
    issues_json JSON COMMENT '问题详情列表',

    -- 时间戳
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- 索引
    UNIQUE KEY uk_quality (bucket_time, building_id, system_id, equipment_type,
                           equipment_id, sub_equipment_id, metric_name),
    INDEX idx_quality_level (quality_level),
    INDEX idx_completeness (completeness_rate)
) ENGINE=InnoDB COMMENT='日聚合质量明细表';

-- ============================================================
-- 3. 更新 schema_version 表
-- ============================================================

INSERT INTO schema_version (table_name, version, change_type, change_desc, applied_by)
VALUES
    ('agg_hour_quality', 'v2.2.0', 'create', '小时聚合质量明细表', 'database_v2_2.sql'),
    ('agg_day_quality', 'v2.2.0', 'create', '日聚合质量明细表', 'database_v2_2.sql');

SELECT '数据库 v2.2 质量表创建完成' AS status;
