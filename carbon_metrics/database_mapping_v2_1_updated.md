# 数据库映射文档 V2.1

本文档记录了 Excel 原始文件/目录与 **cooling_system_v2** 数据库的映射关系。

**版本**: 2.3.0
**更新日期**: 2026-02-10
**适用数据库**: cooling_system_v2

**V2.3.0 更新内容**:
- Pipeline 入口从 `pipeline_v2_1_fixed.py` 迁移到 `run_pipeline.py`（模块化重构）
- ETL 流程新增 quality 阶段（`--quality` 参数）
- 数据统计更新：raw_measurement 8,205,735 条（含 tag 6,282,202 + device 1,923,533）
- Tag 解析规则更新：新增点分层级格式说明（Section 3.2.1）
- 表数量从 14 张更新为 16 张（含 agg_hour_quality / agg_day_quality）
- load_rate 数据已补全至完整时间范围（原截止 2025-10-31）

**V2.2.0 更新内容**:
- 新增 `agg_hour_quality` 小时聚合质量明细表
- 新增 `agg_day_quality` 日聚合质量明细表
- 新增数据质量验证章节（Delta Check 验证结果）

**V2.1.1 更新内容**:
- 新增 G12-3 冷塔总用电解析规则（系统级数据）
- 新增数据可用性与缺失说明章节
- 新增指标可计算性分析
- 明确数据库初始化顺序（先 database_v2.sql，再 database_v2_1.sql）

**V2.1 更新内容**:
- 新增 `metric_definition` 指标定义表（48个计算指标）
- 修改 `metric_result` 表结构（添加 sub_equipment_id 等字段）
- 明确 `equipment_type` 枚举值（包括 cooling_tower_closed）
- 区分冷冻泵(chilled_pump)和冷却泵(cooling_pump)配置

---

## 目录

1. [数据库初始化](#0-数据库初始化)
2. [数据可用性与缺失](#0-1-数据可用性与缺失)
3. [新旧映射对照](#1-新旧映射对照)
4. [目录与数据表映射（V2）](#2-目录与数据表映射v2)
5. [6级层级解析规则](#3-6级层级解析规则)
6. [字段映射详解](#4-字段映射详解)
7. [标准枚举值](#5-标准枚举值)
8. [数据流转路径](#6-数据流转路径)
9. [计算指标定义](#7-计算指标定义)
10. [数据质量表（V2.2新增）](#10-数据质量表v22新增)
11. [数据质量验证](#11-数据质量验证)

---

## 0. 数据库初始化

### 执行顺序

```bash
# 1. 创建数据库和所有基础表
mysql -u root -p < database_v2.sql

# 2. 添加指标定义表和增量更新
mysql -u root -p cooling_system_v2 < database_v2_1.sql

# 3. 添加数据质量表（V2.2新增）
mysql -u root -p cooling_system_v2 < database_v2_2.sql

# 4. 运行数据导入pipeline
python run_pipeline.py --init --ingest --map --canonical --agg --quality --no-progress
```

### 文件说明

| 文件 | 说明 |
|-----|------|
| `database_v2.sql` | 完整建库脚本，创建12张核心表 |
| `database_v2_1.sql` | 增量脚本，添加 metric_definition 表 |
| `database_v2_2.sql` | 增量脚本，添加 agg_hour_quality、agg_day_quality 表 |
| `run_pipeline.py` | Pipeline 入口脚本（替代旧 pipeline_v2_1_fixed.py） |
| `pipeline/` | ETL 模块化目录（ingest/mapping/canonical/agg/quality） |

---

## 0.1 数据可用性与缺失

### 可用数据

| 数据类型 | 覆盖系统 | 数据粒度 | 状态 |
|---------|---------|---------|------|
| 冷机电量 | G11-1/2/3, G12-1/2/3 | 单冷机 | ✅ 完整 |
| 冷机功率 | G11-1/2/3, G12-1/2/3 | 单冷机 | ✅ 完整 |
| 冷冻泵电量 | G11-1/2/3, G12-1/2/3 | 单泵 | ✅ 完整 |
| 冷却泵电量 | G11-1/2/3, G12-1/2/3 | 单泵 | ✅ 完整 |
| 冷冻泵功率 | G12-1 部分 | 单泵 | ⚠️ 不完整 |
| 冷却泵功率 | — | — | ❌ 缺失 |
| 冷塔电量 | G11-1/2/3, G12-1/2/3 | 风机级 | ✅ 完整 |
| 冷塔功率 | G12-3 | 系统级 | ⚠️ 仅G12-3 |
| 冷冻水温度 | 全部系统 | 系统级 | ✅ (Tag数据) |
| 冷却水温度 | 全部系统 | 系统级 | ✅ (Tag数据) |
| 冷冻/冷却水流量 | 全部系统 | 系统级 | ✅ (Tag数据) |
| 冷机负载率 | 全部系统 | 单冷机 | ✅ (Tag数据) |
| 水泵频率 | 全部系统 | 单泵 | ✅ (Tag数据) |
| 运行状态 | 全部系统 | 设备级 | ✅ (Tag数据) |

### 缺失数据

| 数据类型 | 影响指标 | 处理建议 |
|---------|---------|---------|
| **IT用电量** | PUE、系统能效标准差、月度PUE趋势 | 等待数据提供，暂时无法计算 |
| **湿球温度** | 冷却塔逼近度 | 可从气象API获取，或暂时搁置 |
| **设备设计寿命** | 设备老化系数 | 需手动配置到 equipment_registry.extended_params |
| **初始COP** | 冷机性能退化率 | 需记录设备初始值 |

### 指标可计算性统计

| 状态 | 数量 | 占比 | 说明 |
|-----|------|-----|------|
| ✅ 完全可计算 | 24 | 50% | 能耗占比、温度、流量、运行时长等 |
| ⚠️ 部分可计算 | 16 | 33% | 需要额定参数或需建立模型 |
| ❌ 无法计算 | 8 | 17% | 缺IT用电量、湿球温度等 |

---

## 1. 新旧映射对照

### 1.1 表结构变化

| 数据类别 | 旧库表名 | 新库表名 | 说明 |
|----------|----------|----------|------|
| **参数表** | | | |
| 水泵参数 | `pump_rated_params` | `equipment_registry` | 统一到设备主数据表 |
| 冷却塔参数 | `cooling_tower_rated_params` | `equipment_registry` | 统一到设备主数据表 |
| 冷机参数 | `chiller_rated_params` | `equipment_registry` | 统一到设备主数据表 |
| **Tag类时序** | | | |
| 水泵频率 | `pump_frequency_history` | `raw_measurement` → `canonical_measurement` | 统一到分层表 |
| 水泵运行状态 | `pump_run_status_history` | `raw_measurement` → `canonical_measurement` | 统一到分层表 |
| 冷却塔运行状态 | `cooling_tower_run_status_history` | `raw_measurement` → `canonical_measurement` | 统一到分层表 |
| 冷机运行状态 | `chiller_run_status_history` | `raw_measurement` → `canonical_measurement` | 统一到分层表 |
| 冷机负载率 | `chiller_load_ratio_history` | `raw_measurement` → `canonical_measurement` | 统一到分层表 |
| 冷却水温度 | `cooling_water_temp_history` | `raw_measurement` → `canonical_measurement` | 统一到分层表 |
| 冷冻水温度 | `chilled_water_temp_history` | `raw_measurement` → `canonical_measurement` | 统一到分层表 |
| 流量数据 | `water_flow_history` | `raw_measurement` → `canonical_measurement` | 统一到分层表 |
| **Device类时序** | | | |
| 冷机电量 | `chiller_energy_history` | `raw_measurement` → `canonical_measurement` | 统一到分层表 |
| 冷机功率 | `chiller_power_history` | `raw_measurement` → `canonical_measurement` | 统一到分层表 |
| 水泵电量 | `pump_energy_history` | `raw_measurement` → `canonical_measurement` | 统一到分层表 |
| 水泵功率 | `pump_power_history` | `raw_measurement` → `canonical_measurement` | 统一到分层表 |
| 冷却塔电量 | `cooling_tower_energy_history` | `raw_measurement` → `canonical_measurement` | 统一到分层表 |
| 冷却塔功率 | `cooling_tower_power_history` | `raw_measurement` → `canonical_measurement` | 统一到分层表 |

### 1.2 核心变化说明

```
旧库（17张表）                        新库（统一流转）
─────────────────────────────────────────────────────────────────
pump_frequency_history      ─┐
pump_run_status_history      │
chiller_run_status_history   │
chiller_load_ratio_history   │
cooling_tower_run_status     ├──→ raw_measurement (source_type='tag')
cooling_water_temp_history   │           │
chilled_water_temp_history   │           ▼
water_flow_history          ─┘    canonical_measurement
                                         │
chiller_energy_history      ─┐           ▼
chiller_power_history        │      agg_hour
pump_energy_history          ├──→ raw_measurement (source_type='device')
pump_power_history           │           │
cooling_tower_energy         │           ▼
cooling_tower_power         ─┘    canonical_measurement
                                         │
pump_rated_params           ─┐           ▼
chiller_rated_params         ├──→ equipment_registry
cooling_tower_rated_params  ─┘
```

---

## 2. 目录与数据表映射（V2）

### 2.1 数据源配置（source_config 表）

以下规则已预置在 `source_config` 表中，ETL 程序根据此配置自动匹配处理：

| source_name | 来源目录 | 文件名特征 | schema_type | target_equipment_type | target_metric_name |
|-------------|----------|------------|-------------|----------------------|-------------------|
| **参数表** | | | | | |
| `pump_params` | 设备参数 | `水泵.*参数\.xlsx$` | params | pump | - |
| `tower_params` | 设备参数 | `冷却塔.*参数\.xlsx$` | params | cooling_tower | - |
| `chiller_params` | 设备参数 | `冷机.*参数\.xlsx$` | params | chiller | - |
| **Tag类** | | | | | |
| `pump_frequency` | 水泵变频频率 | * | tag | pump | frequency |
| `pump_run_status` | 水泵运行状态 | * | tag | pump | run_status |
| `tower_run_status` | 冷却塔运行状态 | * | tag | cooling_tower | run_status |
| `chiller_run_status` | 冷机运行状态 | * | tag | chiller | run_status |
| `chiller_load_ratio` | 冷机负载率 | * | tag | chiller | load_rate |
| `cooling_water_temp` | 冷却水进出水温度 | * | tag | system | cooling_water_temp |
| `chilled_water_temp` | 冷冻水供回水温度 | * | tag | system | chilled_water_temp |
| `water_flow` | 冷冻水、冷却水流量 | * | tag | system | flow |
| **Device类** | | | | | |
| `chiller_energy` | 冷机功率和电量 | `.*电量.*` | device | chiller | energy |
| `chiller_power` | 冷机功率和电量 | `.*功率.*` | device | chiller | power |
| `pump_energy` | 水泵功率和电量 | `.*电量.*` | device | pump | energy |
| `pump_power` | 水泵功率和电量 | `.*功率.*` | device | pump | power |
| `tower_energy` | 冷塔功率和电量 | `.*电量.*` | device | cooling_tower | energy |
| `tower_power` | 冷塔功率和电量 | `.*功率.*` | device | cooling_tower | power |

### 2.2 数据流转目标表

| 数据类别 | 原始表 | 标准化表 | 聚合表 | 说明 |
|----------|--------|----------|--------|------|
| 参数表 | - | `equipment_registry` | - | 直接写入设备主数据 |
| Tag类时序 | `raw_measurement` | `canonical_measurement` | `agg_hour`/`agg_day` | 分层处理 |
| Device类时序 | `raw_measurement` | `canonical_measurement` | `agg_hour`/`agg_day` | 分层处理 |

### 2.3 原始字段映射

#### Tag 类数据（source_type = 'tag'）

| 原始 Excel 字段 | raw_measurement 字段 | 说明 |
|----------------|---------------------|------|
| - | `source_type` | 固定为 'tag' |
| Sheet名/点名 | `tag_name` | 原始点位标识 |
| 采集时间 | `ts` | 时间戳 |
| 采集值 | `value` | 数值 |
| 单位 | `unit` | 单位 |
| 文件名 | `source_file` | 来源文件 |

#### Device 类数据（source_type = 'device'）

| 原始 Excel 字段 | raw_measurement 字段 | 说明 |
|----------------|---------------------|------|
| - | `source_type` | 固定为 'device' |
| 设备路径 | `device_path` | 原始设备路径 |
| 位置路径 | `location_path` | 位置路径 |
| 指标名称 | `original_metric_name` | 原始指标名（如"正向有功电度"） |
| 时间 | `ts` | 时间戳 |
| 数值 | `value` | 数值 |
| 文件名 | `source_file` | 来源文件 |

---

## 3. 6级层级解析规则

### 3.1 层级定义

| 层级 | 字段 | 示例值 | 说明 |
|------|------|--------|------|
| L1 | - | - | 制冷系统总量（全站汇总） |
| L2 | `building_id` | G11, G12 | 机楼 |
| L3 | `system_id` | G11-1, G11-2, G12-1, G12-2, G12-3 | 系统 |
| L4 | `equipment_type` | chiller, chilled_pump, cooling_pump, cooling_tower | 设备类型 |
| L5 | `equipment_id` | chiller_01, pump_01, tower_01 | 单台设备 |
| L6 | `sub_equipment_id` | fan_01, fan_02 | 子设备（冷塔风机） |

### 3.2 Tag 类点位解析规则

> **V2.3 说明**: 当前 `mapping.py` 实际解析的 Tag 格式为**点分层级格式**（见 3.2.1），
> 下方规则1-3 为旧版格式参考，保留供历史数据对照。

#### 3.2.1 点分层级格式（当前实际使用）

Tag 点名采用 `楼号.系统号.类别.子类别.具体点名` 的点分层级结构。

**温度类**:
```
11楼.G111.冷冻水.冷冻水温度.冷冻水供水主管2温度1
  → building_id=G11, system_id=G11-1, metric_name=chilled_supply_temp

11楼.G111.冷冻水.冷冻水温度.冷冻水回水主管2温度1
  → metric_name=chilled_return_temp

11楼.G111.冷却水.冷却水温度.冷却水上塔环网温度1
  → metric_name=cooling_return_temp  (上塔=热水进塔=回水)

11楼.G111.冷却水.冷却水温度.冷却水下塔环网温度1
  → metric_name=cooling_supply_temp  (下塔=冷水出塔=供水)
```

**流量类**:
```
11楼.G112.冷冻水.流量.冷冻水回水主管1流量1
  → building_id=G11, system_id=G11-2, metric_name=chilled_flow
```

**频率类（设备级）**:
```
11楼.G111.泵.1号冷冻泵频率.1号冷冻水泵_频率反馈
  → equipment_type=chilled_pump, equipment_id=pump_01, metric_name=frequency
```

**负载率类（设备级）**:
```
11楼.G111.其它.冷机电流百分比.1号冷机电流百分比
  → equipment_type=chiller, equipment_id=chiller_01, metric_name=load_rate
```

**运行时长类（设备级）**:
```
11楼.G111.运行时间.主要设备累计运行时长.1号冷却水泵 累计运行时间
  → equipment_type=cooling_pump, equipment_id=pump_01, metric_name=runtime

11楼.G111.运行时间.主要设备累计运行时长.冷却塔1_1号风机 累计运行时间
  → equipment_type=cooling_tower, equipment_id=tower_01, sub_equipment_id=fan_01, metric_name=runtime
```

#### 规则1: 设备级点位（旧版格式参考）

**模式**: `G{楼号}-{系统号}{设备类型}{设备号}#?{指标后缀}`

```
示例: G11-1冷机1#负载率
      G11-1冷冻泵2#频率
      G12-3冷却泵1#运行

解析结果:
├── building_id: G11 / G12
├── system_id: G11-1 / G12-3
├── equipment_type: chiller / chilled_pump / cooling_pump
├── equipment_id: chiller_01 / pump_02 / pump_01
└── metric_name: load_rate / frequency / run_status
```

**正则表达式**:

```text
^G(\d+)-(\d+)(冷机|冷冻泵|冷却泵|冷却塔)(\d+)#?(.*)$
```

**Python 写法**:

```python
import re
pattern = r'^G(\d+)-(\d+)(冷机|冷冻泵|冷却泵|冷却塔)(\d+)#?(.*)$'
match = re.match(pattern, tag_name)
```

**设备类型映射**:
| 中文 | equipment_type |
|------|---------------|
| 冷机 | chiller |
| 冷冻泵 | chilled_pump |
| 冷却泵 | cooling_pump |
| 冷却塔 | cooling_tower |

#### 规则2: 系统级点位（温度/流量）（旧版格式参考）

**模式**: `{楼号}-{系统号}{水类型}{指标类型}`

```
示例: 11-1冷冻水供水
      11-2冷却水回水
      12-1冷冻水流量

解析结果:
├── building_id: G11 / G12
├── system_id: G11-1 / G12-1
├── equipment_type: system
├── equipment_id: NULL
└── metric_name: chilled_supply_temp / cooling_return_temp / chilled_flow
```

**正则表达式**:

```text
^(\d+)-(\d+)(冷冻水|冷却水)(供水|回水|流量).*$
```

**Python 写法**:

```python
import re
pattern = r'^(\d+)-(\d+)(冷冻水|冷却水)(供水|回水|流量).*$'
match = re.match(pattern, tag_name)
```

**指标映射**:
| 原始 | metric_name |
|------|-------------|
| 冷冻水+供水 | chilled_supply_temp |
| 冷冻水+回水 | chilled_return_temp |
| 冷却水+上塔/供水 | cooling_return_temp (热水进塔=回水) |
| 冷却水+下塔/回水 | cooling_supply_temp (冷水出塔=供水) |
| 冷冻水+流量 | chilled_flow |
| 冷却水+流量 | cooling_flow |

#### 规则3: 系统运行状态（旧版格式参考）

**模式**: `G{楼号}{设备类型}运行状态`

```
示例: G11冷机运行状态
      G12冷却塔运行状态

解析结果:
├── building_id: G11 / G12
├── system_id: G11-1（默认）
├── equipment_type: chiller / cooling_tower
├── equipment_id: NULL（系统级）
└── metric_name: run_status
```

### 3.3 Device 类点位解析规则

> **重要**: Device 类数据的 `device_path` 格式不统一，部分数据无法通过 device_path 解析。
> 因此采用**文件名优先**的解析策略：先尝试解析 device_path，失败则回退到文件名解析。

#### 规则0: 文件名解析（主要方案）

当 device_path 解析失败时，从文件名提取层级信息。

**模式0: G12-3冷塔总用电** - `G{楼号}-{系统号}冷塔总用电(主|备){指标}...`

> **说明**: G12-3 的冷塔数据是系统级汇总，没有细分到单个冷塔。这是一个特殊情况。

```
示例: G12-3冷塔总用电主功率数据查询报表...xlsx
      G12-3冷塔总用电备电量数据查询报表...xlsx

解析结果:
├── building_id: G12
├── system_id: G12-3
├── equipment_type: cooling_tower
├── equipment_id: tower_total (系统级汇总)
├── sub_equipment_id: main / backup
└── metric_name: energy / power
```

**正则表达式**:

```python
pattern = r'^G(\d+)-(\d+)冷塔总用电(主|备)'
```

**模式1: 闭式冷塔** - `G{楼号}-{系统号}闭式冷塔{塔号}#(主|备){指标}...`

```
示例: G11-2闭式冷塔1#主电量数据查询报表...xlsx
      G11-2闭式冷塔1#备功率数据查询报表...xlsx

解析结果:
├── building_id: G11
├── system_id: G11-2
├── equipment_type: cooling_tower_closed
├── equipment_id: tower_01
├── sub_equipment_id: main / backup
└── metric_name: energy / power
```

**模式2: 开式冷塔** - `G{楼号}-{系统号}开式冷塔{塔号}#{指标}...`

```
示例: G11-2开式冷塔1#电量数据查询报表...xlsx

解析结果:
├── building_id: G11
├── system_id: G11-2
├── equipment_type: cooling_tower
├── equipment_id: tower_01
└── metric_name: energy / power
```

**模式3: 普通冷塔(开式)带风机** - `G{楼号}-{系统号}冷塔{塔号}#风机{起始}-{结束}{指标}...`

> **说明**: 同一冷塔可能有多组风机数据（如风机1-4、风机5-8），数据库保留原始粒度，不做合并。

```
示例: G11-1冷塔1#风机1-4电量数据查询报表...xlsx
      G11-1冷塔1#风机5-8电量数据查询报表...xlsx

解析结果:
├── building_id: G11
├── system_id: G11-1
├── equipment_type: cooling_tower
├── equipment_id: tower_01
├── sub_equipment_id: fan_01-04 / fan_05-08 (保留原始风机范围)
└── metric_name: energy / power
```

**模式3.5: 普通冷塔(开式)无风机范围** - `G{楼号}-{系统号}冷塔{塔号}#{指标}...`

```
示例: G12-1冷塔1#电量数据查询报表...xlsx

解析结果:
├── building_id: G12
├── system_id: G12-1
├── equipment_type: cooling_tower
├── equipment_id: tower_01
├── sub_equipment_id: NULL (无风机范围)
└── metric_name: energy / power
```

**模式4: 冷机带主/备** - `G{楼号}-{系统号}冷机{机号}#(主|备){指标}...`

```
示例: G11-1冷机4#主电量数据查询报表...xlsx
      G11-1冷机4#备功率数据查询报表...xlsx

解析结果:
├── building_id: G11
├── system_id: G11-1
├── equipment_type: chiller
├── equipment_id: chiller_04
├── sub_equipment_id: main / backup
└── metric_name: energy / power
```

**模式5: 普通冷机/水泵** - `G{楼号}-{系统号}(冷机|冷冻泵|冷却泵){设备号}#{指标}...`

```
示例: G11-1冷机1#电量数据查询报表...xlsx
      G11-1冷冻泵1#电量数据查询报表...xlsx
      G11-1冷却泵1#电量数据查询报表...xlsx

解析结果:
├── building_id: G11
├── system_id: G11-1
├── equipment_type: chiller / chilled_pump / cooling_pump
├── equipment_id: chiller_01 / pump_01
└── metric_name: energy / power
```

#### 规则1: 水泵 (device_path)

**模式**: `(能源系统){泵类型}{泵号}G{楼号}_{系统号}...`

```
示例: (能源系统)冷却水泵3G2_1LQB_3
      (能源系统)冷冻水泵1G1_1LDB_1

解析结果:
├── building_id: G12 / G11
├── system_id: G12-1 / G11-1
├── equipment_type: cooling_pump / chilled_pump
├── equipment_id: pump_03 / pump_01
└── metric_name: energy / power（根据文件名判断）
```

**正则表达式**:

```text
(冷却水泵|冷冻水泵)(\d+)G(\d+)_(\d+)
```

#### 规则2: 冷却塔风机 (device_path)

**模式**: `(能源系统)...冷却塔G{楼号}_{系统号}..._{塔号}_{风机号}`

```
示例: (能源系统)南一.1开式冷却塔G1_2APTw_2_1

解析结果:
├── building_id: G11
├── system_id: G11-2
├── equipment_type: tower_fan
├── equipment_id: tower_02
├── sub_equipment_id: fan_01
└── metric_name: energy / power
```

**正则表达式**:

```text
冷却塔G(\d+)_(\d+).*_(\d+)_(\d+)$
```

#### 规则3: 冷机 (device_path)

**正则表达式**:

```text
冷机(\d+)G(\d+)_(\d+)
```

### 3.4 点位映射表示例

以下是 `point_mapping` 表的示例数据：

```sql
-- Tag 类映射
INSERT INTO point_mapping 
    (source_type, tag_name, building_id, system_id, equipment_type, 
     equipment_id, metric_name, metric_category, agg_method, unit, confidence)
VALUES
('tag', 'G11-1冷机1#负载率', 'G11', 'G11-1', 'chiller', 'chiller_01', 'load_rate', 'instant', 'avg', '%', 'high'),
('tag', 'G11-1冷冻泵1#频率', 'G11', 'G11-1', 'chilled_pump', 'pump_01', 'frequency', 'instant', 'avg', 'Hz', 'high'),
('tag', '11-1冷冻水供水', 'G11', 'G11-1', 'system', NULL, 'chilled_supply_temp', 'instant', 'avg', '℃', 'high'),
('tag', '11-1冷却水流量', 'G11', 'G11-1', 'system', NULL, 'cooling_flow', 'instant', 'avg', 'm³/h', 'high');

-- Device 类映射
INSERT INTO point_mapping 
    (source_type, device_path, original_metric_name, building_id, system_id, 
     equipment_type, equipment_id, sub_equipment_id, metric_name, metric_category, agg_method, confidence)
VALUES
('device', '(能源系统)冷却水泵3G2_1LQB_3', '正向有功电度', 'G12', 'G12-1', 'cooling_pump', 'pump_03', NULL, 'energy', 'cumulative', 'delta', 'high'),
('device', '(能源系统)南一.1开式冷却塔G1_2APTw_2_1', '正向有功电度', 'G11', 'G11-2', 'tower_fan', 'tower_02', 'fan_01', 'energy', 'cumulative', 'delta', 'high');
```

---

## 4. 字段映射详解

### 4.1 参数表 → equipment_registry

#### 冷机参数映射

| 原字段 (chiller_rated_params) | 新字段 (equipment_registry) | 转换规则 |
|------------------------------|----------------------------|----------|
| `building` | `building_id` | 直接映射 |
| - | `system_id` | 默认 `{building}-1`，需人工补充 |
| - | `equipment_type` | 固定 'chiller' |
| `device_code` | `equipment_id` | 转换为 `chiller_{序号}` |
| `device_name` | `equipment_name` | 直接映射 |
| `device_code` | `device_code` | 保留原值 |
| `brand` | `brand` | 直接映射 |
| `model` | `model` | 直接映射 |
| `serial_number` | `serial_number` | 直接映射 |
| `location` | `location` | 直接映射 |
| `power_kw` | `rated_power_kw` | 转换为 DECIMAL |
| `voltage` | `rated_voltage` | 直接映射 |
| `production_date` | `production_date` | 转换为 DATE |
| `cooling_capacity_kw` | `extended_params.cooling_capacity_kw` | JSON |
| `refrigerant_charge_kg` | `extended_params.refrigerant_charge_kg` | JSON |
| `evaporator_outlet_temp_c` | `extended_params.evaporator_outlet_temp_c` | JSON |
| `condenser_inlet_outlet_temp_c` | `extended_params.condenser_inlet_outlet_temp_c` | JSON |
| `remarks` | `remarks` | 直接映射 |
| `source_file` | `source_file` | 直接映射 |

**extended_params JSON 结构**:
```json
{
    "cooling_capacity_kw": 1000,
    "rated_cop": 5.5,
    "refrigerant_type": "R134a",
    "refrigerant_charge_kg": 150,
    "evaporator_outlet_temp_c": 7,
    "condenser_inlet_temp_c": 32,
    "condenser_outlet_temp_c": 37
}
```

#### 水泵参数映射

| 原字段 (pump_rated_params) | 新字段 (equipment_registry) | 转换规则 |
|---------------------------|----------------------------|----------|
| `building` | `building_id` | 直接映射 |
| - | `system_id` | 默认 `{building}-1` |
| `device_name` | `equipment_type` | 根据名称判断 chilled_pump/cooling_pump |
| `device_code` | `equipment_id` | 转换为 `pump_{序号}` |
| `device_name` | `equipment_name` | 直接映射 |
| `room` | `room` | 直接映射 |
| `power_kw` | `rated_power_kw` | 转换为 DECIMAL |
| `head_m` | `extended_params.head_m` | JSON |
| `flow_rate_m3h` | `extended_params.flow_rate_m3h` | JSON |
| `motor_speed_rpm` | `extended_params.motor_speed_rpm` | JSON |
| `pump_speed_rpm` | `extended_params.pump_speed_rpm` | JSON |
| `max_current` | `extended_params.max_current_a` | JSON |

**extended_params JSON 结构**:
```json
{
    "head_m": 25,
    "flow_rate_m3h": 500,
    "motor_speed_rpm": 1450,
    "pump_speed_rpm": 1450,
    "max_current_a": 50
}
```

#### 冷却塔参数映射

| 原字段 (cooling_tower_rated_params) | 新字段 (equipment_registry) | 转换规则 |
|------------------------------------|----------------------------|----------|
| `building` | `building_id` | 直接映射 |
| `chiller_room` | `system_id` | 解析得到系统号 |
| - | `equipment_type` | 固定 'cooling_tower' |
| `tower_code` | `equipment_id` | 转换为 `tower_{序号}` |
| `tower_code` | `device_code` | 保留原值 |
| `model` | `model` | 直接映射 |
| `chiller_room` | `location` | 直接映射 |
| `fan_count` | `extended_params.fan_count` | JSON |
| `cooling_capacity_kcal_h` | `extended_params.cooling_capacity_kcal_h` | JSON |
| `water_treatment_capacity_m3h` | `extended_params.water_treatment_capacity_m3h` | JSON |
| `fill_spec_mm` | `extended_params.fill_spec_mm` | JSON |
| `category` | `extended_params.category` | JSON |

**extended_params JSON 结构**:
```json
{
    "fan_count": 4,
    "cooling_capacity_kcal_h": 500000,
    "water_treatment_capacity_m3h": 800,
    "fill_spec_mm": "1200x500",
    "category": "开式"
}
```

---

## 5. 标准枚举值

### 5.1 equipment_type（设备类型）

| 值 | 说明 | 来源 |
|----|------|------|
| `chiller` | 冷机 | 冷机参数/冷机时序数据 |
| `chilled_pump` | 冷冻泵 | 水泵参数（名称含"冷冻"）/冷冻泵时序数据 |
| `cooling_pump` | 冷却泵 | 水泵参数（名称含"冷却"）/冷却泵时序数据 |
| `closed_tower_pump` | 闭式塔泵 | 闭式冷却塔系统专用泵（含冷冻/冷却，统一类型） |
| `user_side_pump` | 用户侧循环泵 | 用户侧循环泵 |
| `source_side_pump` | 水源侧循环泵 | 水源侧循环泵 |
| `heat_recovery_primary_pump` | 余热回收一次泵 | 余热回收系统一次泵 |
| `heat_recovery_secondary_pump` | 余热回收二次泵 | 余热回收系统二次泵 |
| `fire_pump` | 消防泵 | 消防泵房设备 |
| `unknown_pump` | 未知泵 | 无法识别类型的泵，**需人工分类** |
| `cooling_tower` | 开式冷却塔 | 普通冷塔/开式冷塔（文件名含"开式冷塔"或"冷塔"） |
| `cooling_tower_closed` | 闭式冷却塔 | 闭式冷塔（文件名含"闭式冷塔"，有主/备之分） |
| `tower_fan` | 冷塔风机 | 冷却塔子设备（通过 device_path 解析得到） |
| `system` | 系统级 | 温度/流量等系统级点位（无具体设备） |

> **泵类型识别规则**（按优先级排序）：
> 1. 名称含"闭式塔"或"闭式冷" → `closed_tower_pump`
> 2. 名称含"用户侧" → `user_side_pump`
> 3. 名称含"水源侧" → `source_side_pump`
> 4. 名称含"余热回收一次"或"余热一次" → `heat_recovery_primary_pump`
> 5. 名称含"余热回收二次"或"余热二次" → `heat_recovery_secondary_pump`
> 6. 名称含"消防" → `fire_pump`
> 7. 名称含"冷冻" → `chilled_pump`
> 8. 名称含"冷却" → `cooling_pump`
> 9. 无法识别 → `unknown_pump`（待人工分类）

### 5.1.1 sub_equipment_id（子设备/主备标识）

| 值 | 说明 | 适用设备 |
|----|------|----------|
| `main` | 主设备 | 冷机、闭式冷塔 |
| `backup` | 备用设备 | 冷机、闭式冷塔 |
| `fan_01-04` | 风机范围 | 普通冷塔（开式） |
| `fan_01` | 单个风机 | 冷塔风机 |

### 5.2 metric_name（标准指标名）

| 值 | 说明 | 单位 | metric_category | agg_method |
|----|------|------|-----------------|------------|
| `power` | 瞬时功率 | kW | instant | avg |
| `energy` | 累计电量 | kWh | cumulative | delta |
| `chilled_supply_temp` | 冷冻水供水温度 | ℃ | instant | avg |
| `chilled_return_temp` | 冷冻水回水温度 | ℃ | instant | avg |
| `cooling_supply_temp` | 冷却水供水温度 | ℃ | instant | avg |
| `cooling_return_temp` | 冷却水回水温度 | ℃ | instant | avg |
| `chilled_flow` | 冷冻水流量 | m³/h | instant | avg |
| `cooling_flow` | 冷却水流量 | m³/h | instant | avg |
| `frequency` | 工作频率 | Hz | instant | avg |
| `load_rate` | 负载率 | % | instant | avg |
| `run_status` | 运行状态 | - | status | last |
| `runtime` | 累计运行时长 | h | cumulative | delta |

### 5.3 metric_category（指标类别）

| 值 | 说明 | 聚合特点 |
|----|------|----------|
| `instant` | 瞬时值 | 取平均/最大/最小 |
| `cumulative` | 累计值 | 计算增量(delta) |
| `status` | 状态值 | 取末值(last) |

### 5.4 quality_flags（质量标记）

| Flag | 说明 | 触发条件 |
|------|------|----------|
| `gap` | 时间缺口 | 相邻点间隔 > 预期间隔 × 3 |
| `negative` | 非法负值 | 值 < 0 且该指标不允许负值 |
| `out_of_range` | 超出合理范围 | 超过物理合理值 |
| `jump` | 异常跳变 | 相邻点变化率 > 阈值 |
| `meter_rollover` | 表计回绕 | 累计值突降但在合理范围 |
| `meter_replaced` | 表计更换 | 累计值大幅突降 |
| `precision_drift` | 精度漂移 | -0.01 < delta < 0 |
| `interpolated` | 插值数据 | 该值为插值生成 |
| `stale` | 数据过期 | 数据时间过旧 |

---

## 6. 数据流转路径

### 6.1 完整 ETL 流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 0: 数据源识别                                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│  输入: Excel 文件目录                                                       │
│  处理: 匹配 source_config 表中的规则                                        │
│  输出: 确定 schema_type (tag/device/params)                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 1: 写入 raw_measurement                                               │
├─────────────────────────────────────────────────────────────────────────────┤
│  Tag类:                                                                     │
│    tag_name ← 点名                                                         │
│    ts ← collect_time                                                       │
│    value ← collect_value                                                   │
│    source_type ← 'tag'                                                     │
│                                                                             │
│  Device类:                                                                  │
│    device_path ← 设备路径                                                   │
│    original_metric_name ← 指标名称                                          │
│    ts ← record_time                                                        │
│    value ← record_value                                                    │
│    source_type ← 'device'                                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 2: 点位映射 (point_mapping)                                           │
├─────────────────────────────────────────────────────────────────────────────┤
│  查询 point_mapping 表，获取:                                               │
│    ├── building_id                                                         │
│    ├── system_id                                                           │
│    ├── equipment_type                                                      │
│    ├── equipment_id                                                        │
│    ├── sub_equipment_id                                                    │
│    ├── metric_name (标准化)                                                │
│    ├── metric_category                                                     │
│    └── agg_method                                                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 3: 写入 canonical_measurement                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│  完整 6 级层级 + 标准化指标名 + 数据质量标记                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 4: 计算 agg_hour                                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│  按 metric_category 选择聚合方式:                                           │
│    instant → AVG, MIN, MAX                                                 │
│    cumulative → DELTA (需要 meter_config 配置)                             │
│    status → LAST                                                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 5: 计算 agg_day                                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│  从 agg_hour 汇总到日级                                                     │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 6: 计算数据质量 (--quality)                                            │
├─────────────────────────────────────────────────────────────────────────────┤
│  agg_hour_quality: 每小时完整率/缺口/跳变/质量评分                            │
│  agg_day_quality:  每日完整率/缺口汇总/质量评分                               │
│  质量等级: good(≥90) / warning(60-89) / poor(<60)                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 7: 计算 metric_result (可选, --metrics)                               │
├─────────────────────────────────────────────────────────────────────────────┤
│  复杂指标计算: COP, 综合能效, 能耗占比等                                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 查询示例

#### 查询某系统某小时的所有指标

```sql
SELECT 
    bucket_time,
    equipment_type,
    equipment_id,
    metric_name,
    agg_avg,
    agg_delta,
    sample_count
FROM agg_hour
WHERE system_id = 'G11-1'
  AND bucket_time = '2025-07-15 10:00:00'
ORDER BY equipment_type, equipment_id, metric_name;
```

#### 查询某设备的历史功率曲线

```sql
SELECT 
    ts,
    value AS power_kw,
    quality_flags
FROM canonical_measurement
WHERE system_id = 'G11-1'
  AND equipment_type = 'chiller'
  AND equipment_id = 'chiller_01'
  AND metric_name = 'power'
  AND ts BETWEEN '2025-07-15' AND '2025-07-16'
ORDER BY ts;
```

#### 查询某机楼的日电量汇总

```sql
SELECT 
    bucket_time,
    equipment_type,
    SUM(agg_delta) AS total_energy_kwh
FROM agg_day
WHERE building_id = 'G11'
  AND metric_name = 'energy'
  AND bucket_time BETWEEN '2025-07-01' AND '2025-07-31'
GROUP BY bucket_time, equipment_type
ORDER BY bucket_time, equipment_type;
```

---

## 附录: 快速参考

### A. 旧表 → 新字段速查

| 旧表 | 旧字段 | 新表 | 新字段 |
|------|--------|------|--------|
| `*_history` (Tag类) | `tag_name` | `raw_measurement` | `tag_name` |
| `*_history` (Tag类) | `collect_time` | `raw_measurement` | `ts` |
| `*_history` (Tag类) | `collect_value` | `raw_measurement` | `value` |
| `*_history` (Device类) | `device_path` | `raw_measurement` | `device_path` |
| `*_history` (Device类) | `record_time` | `raw_measurement` | `ts` |
| `*_history` (Device类) | `record_value` | `raw_measurement` | `value` |
| `*_history` (Device类) | `metric_name` | `raw_measurement` | `original_metric_name` |
| `*_rated_params` | 各字段 | `equipment_registry` | 通用字段 + `extended_params` JSON |

### B. 常用 SQL 模板

```sql
-- 检查点位映射覆盖率
SELECT 
    confidence,
    COUNT(*) AS cnt,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) AS pct
FROM point_mapping
WHERE is_active = 1
GROUP BY confidence;

-- 检查未映射的点位
SELECT DISTINCT 
    source_type,
    COALESCE(tag_name, device_path) AS point_key
FROM raw_measurement r
WHERE NOT EXISTS (
    SELECT 1 FROM point_mapping m
    WHERE (r.source_type = 'tag' AND r.tag_name = m.tag_name)
       OR (r.source_type = 'device' AND r.device_path = m.device_path)
);

-- 检查数据量分布
SELECT 
    DATE(ts) AS dt,
    source_type,
    COUNT(*) AS cnt
FROM raw_measurement
GROUP BY DATE(ts), source_type
ORDER BY dt, source_type;
```

---

## 7. 计算指标定义

### 7.1 指标定义表 (metric_definition)

V2.1 新增 `metric_definition` 表，用于存储所有计算指标的元数据定义。

**表结构核心字段**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `metric_code` | VARCHAR(64) | 指标代码（英文唯一标识） |
| `metric_name` | VARCHAR(128) | 指标名称（中文） |
| `category_code` | VARCHAR(32) | 分类代码 |
| `category_name` | VARCHAR(64) | 分类名称 |
| `formula` | TEXT | 计算公式（人类可读） |
| `required_metrics` | JSON | 依赖的原始指标列表 |
| `applicable_levels` | JSON | 适用层级 [1-6] |
| `time_granularity` | JSON | 支持的时间粒度 |
| `agg_method` | ENUM | 聚合方式 |
| `unit` | VARCHAR(32) | 单位 |

### 7.2 指标分类汇总

基于《数据中心制冷系统指标梳理V4》，共定义 **48个计算指标**，分为 10 大类：

| 分类代码 | 分类名称 | 指标数量 | 主要指标 |
|----------|----------|----------|----------|
| `efficiency` | 系统级能效指标 | 4 | PUE、空调系统能效比、系统综合能效比、系统能效标准差 |
| `energy_structure` | 能耗结构指标 | 4 | 冷机/水泵/风机能耗占比、总能耗 |
| `chiller_efficiency` | 冷机运行效率指标 | 6 | COP、能效比、负载率、启停次数 |
| `pump_efficiency` | 水泵效率指标 | 6 | 工作频率、能耗密度、功率利用率、水泵效率 |
| `tower_efficiency` | 冷却塔效率指标 | 5 | 冷却水温差、风机功率、冷却塔效率、逼近度 |
| `temperature` | 温度与温差指标 | 6 | 各温度、温差、偏离率 |
| `flow` | 流量效率指标 | 4 | 流量、利用率、制冷量 |
| `stability` | 运行稳定性指标 | 4 | 各设备运行时长占比 |
| `maintenance` | 预测性维护指标 | 4 | 老化系数、性能退化率、风险指数 |
| `saving` | 节能潜力评估指标 | 4 | 各类优化潜力 |

### 7.3 指标详细定义

#### 7.3.1 系统级能效指标 (efficiency)

| 指标代码 | 指标名称 | 计算公式 | 依赖指标 | 适用层级 | 单位 |
|----------|----------|----------|----------|----------|------|
| `pue` | PUE | 总用电量 / IT用电量 | total_energy, it_energy | L1-L3 | - |
| `hvac_efficiency` | 空调系统能效比 | IT负荷制冷量 / 空调系统用电量 | cooling_capacity, hvac_energy | L1-L3 | - |
| `system_cop` | 系统综合能效比 | 总制冷量 / 系统总输入功率 | total_cooling_capacity, total_power | L1-L3 | - |
| `pue_std` | 系统能效标准差 | 各系统PUE的标准差 | pue | L1-L2 | - |

#### 7.3.2 能耗结构指标 (energy_structure)

| 指标代码 | 指标名称 | 计算公式 | 依赖指标 | 适用层级 | 单位 |
|----------|----------|----------|----------|----------|------|
| `chiller_energy_ratio` | 冷机能耗占比 | 冷机电量 / 系统总电量 | chiller_energy, total_energy | L1-L3 | % |
| `pump_energy_ratio` | 水泵能耗占比 | (冷冻泵+冷却泵)电量 / 系统总电量 | chilled_pump_energy, cooling_pump_energy, total_energy | L1-L3 | % |
| `tower_energy_ratio` | 风机能耗占比 | 冷塔风机电量 / 系统总电量 | tower_energy, total_energy | L1-L3 | % |
| `total_energy` | 总能耗 | 冷机+冷冻泵+冷却泵+冷塔电量 | chiller_energy, chilled_pump_energy, cooling_pump_energy, tower_energy | L1-L3 | kWh |

#### 7.3.3 冷机运行效率指标 (chiller_efficiency)

| 指标代码 | 指标名称 | 计算公式 | 依赖指标 | 适用层级 | 单位 |
|----------|----------|----------|----------|----------|------|
| `chiller_avg_load` | 冷机平均负载率 | 平均负载率 | load_rate | L3-L5 | % |
| `chiller_max_load` | 冷机最大负载率 | 最大负载率 | load_rate | L3-L5 | % |
| `chiller_load_cv` | 冷机负载波动系数 | 标准差 / 平均值 | load_rate | L3-L5 | - |
| `chiller_cop` | 冷机COP | 制冷量 / 冷机输入功率 | chilled_flow, temp, chiller_power | L3-L5 | - |
| `chiller_efficiency_ratio` | 冷机能效比 | 实际COP / 额定COP | chiller_cop, rated_cop | L5 | % |
| `chiller_start_count` | 冷机启停次数 | 启停次数统计 | run_status | L4-L5 | 次 |

#### 7.3.4 水泵效率指标 (pump_efficiency)

| 指标代码 | 指标名称 | 计算公式 | 依赖指标 | 适用层级 | 单位 |
|----------|----------|----------|----------|----------|------|
| `chilled_pump_frequency` | 冷冻泵工作频率 | 实际频率 | frequency | L5 | Hz |
| `cooling_pump_frequency` | 冷却泵工作频率 | 实际频率 | frequency | L5 | Hz |
| `chilled_pump_energy_density` | 冷冻泵能耗密度 | 冷冻泵耗电量 / 冷冻水流量 | chilled_pump_energy, chilled_flow | L3-L4 | kWh/m³ |
| `cooling_pump_energy_density` | 冷却泵能耗密度 | 冷却泵耗电量 / 冷却水流量 | cooling_pump_energy, cooling_flow | L3-L4 | kWh/m³ |
| `pump_power_utilization` | 水泵功率利用率 | 实际功率 / 额定功率 | power, rated_power_kw | L5 | % |
| `pump_efficiency` | 水泵效率 | 扬程×流量 / 输入功率 | head_m, flow, power | L5 | % |

#### 7.3.5 冷却塔效率指标 (tower_efficiency)

| 指标代码 | 指标名称 | 计算公式 | 依赖指标 | 适用层级 | 单位 |
|----------|----------|----------|----------|----------|------|
| `cooling_water_delta_t` | 冷却水温差 | 回水温度 - 供水温度 | cooling_return_temp, cooling_supply_temp | L3-L4 | ℃ |
| `tower_fan_power` | 冷却塔风机功率 | 实际功率 | power | L5-L6 | kW |
| `tower_efficiency` | 冷却塔效率 | (回水温度-供水温度) / 冷却塔耗电量 | temp, tower_energy | L3-L5 | ℃/kWh |
| `tower_fan_runtime` | 风机累计运行时长 | 总运行时间 | runtime | L5-L6 | h |
| `tower_approach` | 冷却塔逼近度 | 回水温度 - 湿球温度 | cooling_return_temp, wet_bulb_temp | L3-L5 | ℃ |

#### 7.3.6 温度与温差指标 (temperature)

| 指标代码 | 指标名称 | 计算公式 | 依赖指标 | 适用层级 | 单位 |
|----------|----------|----------|----------|----------|------|
| `chilled_supply_temp` | 冷冻水供水温度 | 实际供水温度 | chilled_supply_temp | L3-L4 | ℃ |
| `chilled_return_temp` | 冷冻水回水温度 | 实际回水温度 | chilled_return_temp | L3-L4 | ℃ |
| `chilled_water_delta_t` | 冷冻水温差 | 回水 - 供水 | chilled_return_temp, chilled_supply_temp | L3-L4 | ℃ |
| `cooling_supply_temp` | 冷却水供水温度 | 实际供水温度 | cooling_supply_temp | L3-L4 | ℃ |
| `cooling_return_temp` | 冷却水回水温度 | 实际回水温度 | cooling_return_temp | L3-L4 | ℃ |
| `temp_deviation_ratio` | 温差偏离率 | (实际-标准) / 标准 | chilled_water_delta_t | L3-L4 | % |

#### 7.3.7 流量效率指标 (flow)

| 指标代码 | 指标名称 | 计算公式 | 依赖指标 | 适用层级 | 单位 |
|----------|----------|----------|----------|----------|------|
| `chilled_flow` | 冷冻水流量 | 实际流量 | chilled_flow | L3-L4 | m³/h |
| `cooling_flow` | 冷却水流量 | 实际流量 | cooling_flow | L3-L4 | m³/h |
| `flow_utilization` | 流量利用率 | 实际流量 / 额定流量 | flow, rated_flow | L3-L4 | % |
| `cooling_capacity` | 制冷量 | 流量 × 温差 × 4.186 / 3.6 | chilled_flow, chilled_water_delta_t | L3-L4 | kW |

#### 7.3.8 运行稳定性指标 (stability)

| 指标代码 | 指标名称 | 计算公式 | 依赖指标 | 适用层级 | 单位 |
|----------|----------|----------|----------|----------|------|
| `chiller_runtime_ratio` | 冷机运行时长占比 | 运行时长 / 评估周期 | runtime | L4-L5 | % |
| `chilled_pump_runtime_ratio` | 冷冻泵运行时长占比 | 运行时长 / 评估周期 | runtime | L4-L5 | % |
| `cooling_pump_runtime_ratio` | 冷却泵运行时长占比 | 运行时长 / 评估周期 | runtime | L4-L5 | % |
| `tower_fan_runtime_ratio` | 风机运行时长占比 | 运行时长 / 评估周期 | runtime | L5-L6 | % |

#### 7.3.9 预测性维护指标 (maintenance)

| 指标代码 | 指标名称 | 计算公式 | 依赖指标 | 适用层级 | 单位 |
|----------|----------|----------|----------|----------|------|
| `equipment_aging` | 设备老化系数 | 累计运行时长 / 设计寿命 | runtime, design_lifetime | L5 | - |
| `chiller_degradation` | 冷机性能退化率 | (当前COP-初始COP) / 初始COP | chiller_cop, initial_cop | L5 | % |
| `pump_vfd_risk` | 水泵变频失效风险 | 频率固定不变时长 / 总运行时长 | frequency, runtime | L5 | % |
| `chiller_overload_risk` | 过载风险指数 | (负载率-80%) / 80% | load_rate | L5 | - |

#### 7.3.10 节能潜力评估指标 (saving)

| 指标代码 | 指标名称 | 计算公式 | 依赖指标 | 适用层级 | 单位 |
|----------|----------|----------|----------|----------|------|
| `vfd_saving_potential` | 变频节能潜力 | (定频功耗-变频功耗) × 时间 | power, frequency | L4-L5 | kWh |
| `delta_t_optimization` | 温差优化潜力 | 流量减少 × 泵功率 × 优化率 | delta_t, flow, pump_power | L3-L4 | kWh |
| `load_optimization` | 负载优化潜力 | COP提升差值 × 能耗 | chiller_cop, chiller_energy | L3-L4 | kWh |
| `fan_optimization` | 风机优化潜力 | 功率降低 × 时间 | tower_fan_power, runtime | L4-L5 | kWh |

### 7.4 指标计算流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 1: 读取 metric_definition                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│  获取指标定义: metric_code, formula, required_metrics, applicable_levels    │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 2: 查询依赖数据                                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│  从 agg_hour / agg_day 表获取 required_metrics 对应的聚合数据               │
│  从 equipment_registry 获取 required_params 对应的额定参数                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 3: 执行计算                                                            │
├─────────────────────────────────────────────────────────────────────────────┤
│  根据 formula_expr 计算指标值                                               │
│  处理数据缺失、异常值、层级回退                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 4: 写入 metric_result                                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  存储计算结果，包含:                                                        │
│  - 指标值 (value)                                                          │
│  - 基准值 (baseline_value)                                                 │
│  - 偏离率 (deviation_pct)                                                  │
│  - 数据来源追溯 (data_sources, sql_trace_id)                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 7.5 查询示例

#### 查询某系统的 COP 趋势

```sql
SELECT 
    bucket_time,
    value AS cop,
    baseline_value AS rated_cop,
    deviation_pct
FROM metric_result
WHERE metric_code = 'chiller_cop'
  AND system_id = 'G11-1'
  AND bucket_type = 'hour'
  AND bucket_time BETWEEN '2025-07-15' AND '2025-07-16'
ORDER BY bucket_time;
```

#### 查询各系统的能耗占比

```sql
SELECT 
    system_id,
    metric_code,
    metric_name,
    AVG(value) AS avg_ratio
FROM metric_result mr
JOIN metric_definition md ON mr.metric_code = md.metric_code
WHERE md.category_code = 'energy_structure'
  AND mr.bucket_type = 'day'
  AND mr.bucket_time BETWEEN '2025-07-01' AND '2025-07-31'
GROUP BY system_id, metric_code, metric_name
ORDER BY system_id, metric_code;
```

#### 查询过载风险指数异常的冷机

```sql
SELECT 
    bucket_time,
    system_id,
    equipment_id,
    value AS overload_risk
FROM metric_result
WHERE metric_code = 'chiller_overload_risk'
  AND value > 0  -- 过载风险指数 > 0 表示存在过载
  AND bucket_type = 'hour'
ORDER BY value DESC
LIMIT 20;
```

---

## 8. 指标可计算性详细说明

### 8.1 完全可计算的指标 (24个)

这些指标基于现有数据可以直接计算：

| 指标代码 | 指标名称 | 数据来源 |
|----------|----------|----------|
| `chiller_energy_ratio` | 冷机能耗占比 | 冷机电量、系统总电量 |
| `pump_energy_ratio` | 水泵能耗占比 | 冷冻泵+冷却泵电量、系统总电量 |
| `tower_energy_ratio` | 风机能耗占比 | 冷塔电量、系统总电量 |
| `total_energy` | 总能耗 | 四类设备电量累计 |
| `chiller_avg_load` | 冷机平均负载率 | 冷机负载率Tag数据 |
| `chiller_max_load` | 冷机最大负载率 | 冷机负载率Tag数据 |
| `chiller_load_cv` | 冷机负载波动系数 | 冷机负载率时间序列 |
| `chiller_start_count` | 冷机启停次数 | 运行状态变化统计 |
| `chilled_pump_frequency` | 冷冻泵工作频率 | 变频频率Tag数据 |
| `cooling_pump_frequency` | 冷却泵工作频率 | 变频频率Tag数据 |
| `cooling_water_delta_t` | 冷却水温差 | 冷却水供回水温度 |
| `tower_fan_runtime` | 风机累计运行时长 | 运行状态累计 |
| `chilled_supply_temp` | 冷冻水供水温度 | Tag数据 |
| `chilled_return_temp` | 冷冻水回水温度 | Tag数据 |
| `chilled_water_delta_t` | 冷冻水温差 | 供回水温度差 |
| `cooling_supply_temp` | 冷却水供水温度 | Tag数据 |
| `cooling_return_temp` | 冷却水回水温度 | Tag数据 |
| `chilled_flow` | 冷冻水流量 | Tag数据 |
| `cooling_flow` | 冷却水流量 | Tag数据 |
| `cooling_capacity` | 制冷量 | 流量×温差×4.186 |
| `chiller_runtime_ratio` | 冷机运行时长占比 | 运行状态历史 |
| `chilled_pump_runtime_ratio` | 冷冻泵运行时长占比 | 运行状态历史 |
| `cooling_pump_runtime_ratio` | 冷却泵运行时长占比 | 运行状态历史 |
| `tower_fan_runtime_ratio` | 风机运行时长占比 | 运行状态历史 |
| `pump_vfd_risk` | 水泵变频失效风险 | 频率数据分析 |
| `chiller_overload_risk` | 过载风险指数 | 负载率数据 |

### 8.2 部分可计算的指标 (16个)

这些指标需要额外数据或需建立模型：

| 指标代码 | 指标名称 | 缺失/需补充 | 处理建议 |
|----------|----------|-------------|----------|
| `hvac_efficiency` | 空调系统能效比 | 需完整流量+温度 | 依赖Tag数据质量 |
| `system_cop` | 系统综合能效比 | 同上 | 依赖Tag数据质量 |
| `chiller_cop` | 冷机COP | 系统级温度流量 | 用系统级数据近似 |
| `chiller_efficiency_ratio` | 冷机能效比 | 额定COP参数 | 从参数表提取 |
| `chilled_pump_energy_density` | 冷冻泵能耗密度 | 系统级流量 | 可计算 |
| `cooling_pump_energy_density` | 冷却泵能耗密度 | 系统级流量 | 可计算 |
| `pump_power_utilization` | 水泵功率利用率 | 功率数据不全 | G12-1部分可算 |
| `pump_efficiency` | 水泵效率 | 功率数据不全 | G12-1部分可算 |
| `tower_fan_power` | 冷却塔风机功率 | 仅G12-3有 | G12-3可算 |
| `tower_efficiency` | 冷却塔效率 | 功率数据不全 | 用电量替代 |
| `temp_deviation_ratio` | 温差偏离率 | 需定义标准值 | 配置标准温差 |
| `flow_utilization` | 流量利用率 | 额定流量参数 | 从参数表提取 |
| `vfd_saving_potential` | 变频节能潜力 | 需建立模型 | 简化版本可算 |
| `delta_t_optimization` | 温差优化潜力 | 需建立模型 | 简化版本可算 |
| `load_optimization` | 负载优化潜力 | 需建立模型 | 简化版本可算 |
| `fan_optimization` | 风机优化潜力 | 需建立模型 | 简化版本可算 |

### 8.3 无法计算的指标 (8个)

这些指标因关键数据缺失而无法计算：

| 指标代码 | 指标名称 | 缺失数据 | 状态 |
|----------|----------|----------|------|
| `pue` | PUE | IT用电量 | ❌ 待数据提供 |
| `pue_trend` | 月度PUE趋势 | IT用电量 | ❌ 待数据提供 |
| `pue_std` | 系统能效标准差 | IT用电量 | ❌ 待数据提供 |
| `tower_approach` | 冷却塔逼近度 | 湿球温度 | ❌ 可从气象API获取 |
| `equipment_aging` | 设备老化系数 | 设计寿命 | ❌ 需手动配置 |
| `chiller_degradation` | 冷机性能退化率 | 初始COP | ❌ 需记录初始值 |

---

## 9. 变更日志

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| 2.3.0 | 2026-02-10 | Pipeline迁移到run_pipeline.py，ETL流程新增quality阶段，Tag解析规则补充点分层级格式，数据统计更新，表数量14→16 |
| 2.2.0 | 2026-02-05 | 新增agg_hour_quality、agg_day_quality表，新增数据质量验证章节 |
| 2.1.1 | 2025-02-04 | 新增G12-3冷塔总用电解析、数据可用性章节、指标可计算性分析 |
| 2.1.0 | 2025-02-04 | 新增metric_definition表、修改metric_result表结构 |
| 2.0.0 | 2025-02-01 | 初始版本，统一分层表结构 |

---

## 10. 数据质量表（V2.2新增）

V2.2 版本新增两张数据质量明细表，用于记录聚合过程中发现的数据质量问题。

### 10.1 小时聚合质量表 (agg_hour_quality)

**用途**: 记录每个小时聚合记录的数据质量详情，与 `agg_hour` 表通过复合键关联。

**表结构**:

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | BIGINT | 主键 |
| `bucket_time` | DATETIME | 小时开始时间 |
| `building_id` | VARCHAR(16) | 机楼编号 |
| `system_id` | VARCHAR(16) | 系统编号 |
| `equipment_type` | VARCHAR(32) | 设备类型 |
| `equipment_id` | VARCHAR(64) | 设备编号 |
| `sub_equipment_id` | VARCHAR(64) | 子设备编号 |
| `metric_name` | VARCHAR(64) | 指标名称 |
| `expected_samples` | INT | 预期样本数（默认12条/小时） |
| `actual_samples` | INT | 实际样本数 |
| `completeness_rate` | DECIMAL(5,2) | 完整率 % |
| `gap_count` | INT | 时间缺口数（间隔>10分钟） |
| `max_gap_seconds` | INT | 最大缺口秒数 |
| `negative_count` | INT | 负值数量 |
| `jump_count` | INT | 异常跳变数量 |
| `out_of_range_count` | INT | 超量程数量 |
| `quality_score` | DECIMAL(5,2) | 质量评分 0-100 |
| `quality_level` | ENUM | 质量等级: good/warning/poor |
| `issues_json` | JSON | 问题详情列表 |
| `computed_at` | TIMESTAMP | 计算时间 |

**唯一键**: `(bucket_time, building_id, system_id, equipment_type, equipment_id, sub_equipment_id, metric_name)`

**索引**:
- `idx_quality_level` - 按质量等级查询
- `idx_completeness` - 按完整率查询
- `idx_equipment` - 按设备类型和设备ID查询

### 10.2 日聚合质量表 (agg_day_quality)

**用途**: 记录每日聚合记录的数据质量汇总，与 `agg_day` 表通过复合键关联。

**表结构**:

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | BIGINT | 主键 |
| `bucket_time` | DATE | 日期 |
| `building_id` | VARCHAR(16) | 机楼编号 |
| `system_id` | VARCHAR(16) | 系统编号 |
| `equipment_type` | VARCHAR(32) | 设备类型 |
| `equipment_id` | VARCHAR(64) | 设备编号 |
| `sub_equipment_id` | VARCHAR(64) | 子设备编号 |
| `metric_name` | VARCHAR(64) | 指标名称 |
| `expected_hours` | INT | 预期小时数（默认24） |
| `actual_hours` | INT | 实际有数据的小时数 |
| `completeness_rate` | DECIMAL(5,2) | 完整率 % |
| `total_gap_hours` | DECIMAL(5,2) | 总缺口小时数 |
| `total_negative_count` | INT | 当日负值总数 |
| `total_jump_count` | INT | 当日跳变总数 |
| `quality_score` | DECIMAL(5,2) | 质量评分 0-100 |
| `quality_level` | ENUM | 质量等级: good/warning/poor |
| `issues_json` | JSON | 问题详情列表 |
| `computed_at` | TIMESTAMP | 计算时间 |

**唯一键**: `(bucket_time, building_id, system_id, equipment_type, equipment_id, sub_equipment_id, metric_name)`

**索引**:
- `idx_quality_level` - 按质量等级查询
- `idx_completeness` - 按完整率查询

### 10.3 质量等级规则

| 等级 | quality_score | 说明 |
|------|---------------|------|
| `good` | ≥ 90 | 数据质量良好，可直接使用 |
| `warning` | 60-89 | 数据存在问题，需关注 |
| `poor` | < 60 | 数据质量差，计算结果可能不可靠 |

**评分规则**:
- 基础分 100 分
- 完整率每降低 1%，扣 1 分
- 每个时间缺口扣 2 分
- 每个负值扣 1 分
- 每个异常跳变扣 3 分

---

## 11. 数据质量验证

### 11.1 Delta Check 验证方法

为验证聚合逻辑的正确性，对 `agg_hour.agg_delta` 进行了独立验证。

**验证公式**:
```
agg_delta = 末值(agg_last) - 首值(agg_first)
```

**验证方法**: 从 `canonical_measurement` 重新计算每小时的 delta 值，与 `agg_hour.agg_delta` 对比。

### 11.2 验证结果（2025-07-01 样本日）

| 设备类型 | 总记录数 | 完全匹配 | 不匹配 | 匹配率 |
|----------|----------|----------|--------|--------|
| chiller | 1,392 | 1,392 | 0 | **100.00%** |
| tower_fan | 288 | 288 | 0 | **100.00%** |
| cooling_tower | 768 | 646 | 122 | 84.11% |
| cooling_pump | 1,152 | 840 | 312 | 72.92% |
| chilled_pump | 936 | 556 | 380 | 59.40% |

### 11.3 结果分析

**结论**: 聚合逻辑正确，不匹配是由原始数据质量问题导致。

**证据**:
1. **chiller 和 tower_fan 100% 匹配** - 证明聚合算法本身正确
2. **pump 类匹配率低** - 与深度体检报告中的跳变异常数量高度相关

### 11.4 与深度体检报告的关联

| 数据源表 | 跳变异常数 | 对应设备类型 | Delta匹配率 |
|----------|------------|--------------|-------------|
| chiller_energy_history | 15 | chiller | 100% |
| pump_energy_history | 4,230 | pump类 | 59-73% |
| cooling_tower_energy_history | 2 | cooling_tower | 84% |

**关键发现**: pump_energy_history 的跳变异常数（4,230）是 chiller_energy_history（15）的 **282倍**，直接导致了 pump 类设备的低匹配率。

### 11.5 结论

1. **聚合逻辑正确** - chiller 和 tower_fan 的 100% 匹配率证明了这一点
2. **数据真实性保持** - 不匹配反映的是原始数据的质量问题，而非聚合错误
3. **质量问题可追溯** - 通过 `agg_hour_quality` 表可以追溯每条记录的质量详情


## 12. 2026-02-09 回归验证与 Tag 口径更新

### 12.1 本次回归结果（用户实测）

在执行以下命令后：

```bash
python norm/create_sql/run_pipeline.py --ingest --map --canonical --agg --quality --energy-dir data1 --params-dir data1 --no-progress
```

关键结果如下：

- `raw_by_source`
  - `tag`: `6,282,202`
  - `device`: `1,923,533`
- `mapping_by_source`
  - `tag`: `178`
  - `device`: `135`
- `agg_hour_metrics`
  - `energy`: `375,803`
  - `runtime`: `219,773`
  - `power`: `154,557`
  - `frequency`: `49,444`
  - `chilled_flow`: `25,661`
  - `chilled_return_temp`: `25,653`
  - `chilled_supply_temp`: `25,653`
  - `cooling_supply_temp`: `25,632`
  - `cooling_flow`: `24,104`
  - `cooling_return_temp`: `16,785`
  - `load_rate`: `12,847`

结论：`tag` 数据已成功入库并进入映射与聚合层，温度/流量/频率/负载/运行时长相关原子指标已落到 `agg_hour`。

### 12.2 Tag 解析口径（mapping.py）

当前 `parse_tag_name` 按实际点名口径解析，例如：

- `11楼.G111.冷冻水.冷冻水温度.冷冻水供水主管2温度1`
  - `building_id=G11`, `system_id=G11-1`, `metric_name=chilled_supply_temp`
- `11楼.G111.冷冻水.冷冻水温度.冷冻水回水主管2温度1`
  - `metric_name=chilled_return_temp`
- `11楼.G111.冷却水.冷却水温度.冷却水上塔环网温度1`
  - `metric_name=cooling_return_temp` (上塔=热水进塔=回水)
- `11楼.G112.冷冻水.流量.冷冻水回水主管1流量1`
  - `metric_name=chilled_flow`
- `11楼.G111.泵.1号冷冻泵频率.1号冷冻水泵_频率反馈`
  - `equipment_type=chilled_pump`, `equipment_id=pump_01`, `metric_name=frequency`
- `11楼.G111.其它.冷机电流百分比.1号冷机电流百分比`
  - `equipment_type=chiller`, `equipment_id=chiller_01`, `metric_name=load_rate`
- `11楼.G111.运行时间.主要设备累计运行时长.1号冷却水泵 累计运行时间`
  - `equipment_type=cooling_pump`, `equipment_id=pump_01`, `metric_name=runtime`
- `11楼.G111.运行时间.主要设备累计运行时长.冷却塔1_1号风机 累计运行时间`
  - `equipment_type=cooling_tower`, `equipment_id=tower_01`, `sub_equipment_id=fan_01`, `metric_name=runtime`

### 12.3 说明

- 本轮“补数”是 ETL 重放原始 Excel 数据，不是人工补填缺失值。
- 若不先清空中间层重复跑 `--ingest`，会导致 `raw_measurement` 叠加重复数据。
- 日常调口径建议仅运行：

```bash
python norm/create_sql/run_pipeline.py --map --canonical --agg --quality --no-progress
```

仅在原始 Excel 数据源发生变化时再执行 `--ingest`。
