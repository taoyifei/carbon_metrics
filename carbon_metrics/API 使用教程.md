# 制冷系统指标平台 API 使用教程

## 目录

1. [快速开始](#一快速开始)
2. [Swagger UI 界面介绍](#二swagger-ui-界面介绍)
3. [手把手教程：计算系统总电量](#三手把手教程计算系统总电量)
4. [手把手教程：查看数据质量](#四手把手教程查看数据质量)
5. [所有 API 详细说明](#五所有-api-详细说明)
6. [常见问题](#六常见问题)
7. [参考信息](#七参考信息)

---

## 一、快速开始

### 1.1 启动服务

打开 PowerShell，执行以下命令：

```powershell
cd D:\algo-gmcii\carbon\src
uvicorn carbon_metrics.backend.main:app --reload
```

看到以下输出表示启动成功：
```
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
INFO:     Started reloader process [xxxxx]
```

### 1.2 打开 API 文档

打开浏览器，在地址栏输入：
```
http://localhost:8000/docs
```

你会看到一个网页，这就是 **Swagger UI** - 一个可以直接测试 API 的交互式文档。

---

## 二、Swagger UI 界面介绍

打开 http://localhost:8000/docs 后，你会看到这样的页面：

```
┌─────────────────────────────────────────────────────────────┐
│  制冷系统指标平台                                              │
│  基于 cooling_system_v2 数据库的指标计算与展示平台              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ▼ 指标计算                                                  │
│    ├─ GET  /api/metrics/list        获取所有可用指标列表      │
│    └─ GET  /api/metrics/calculate   计算指定指标              │
│                                                             │
│  ▼ 数据质量                                                  │
│    ├─ GET  /api/quality/summary     获取数据质量汇总统计      │
│    ├─ GET  /api/quality/list        获取数据质量明细列表      │
│    ├─ GET  /api/quality/issues      获取数据异常问题列表      │
│    └─ GET  /api/quality/equipment/{equipment_id}/trend       │
│                                                             │
│  ▼ default                                                  │
│    ├─ GET  /                        根路径                   │
│    └─ GET  /health                  健康检查                 │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**界面说明：**
- 每个 `GET` 就是一个 API 接口
- 点击任意一行可以展开详情
- 绿色的 `GET` 表示这是一个获取数据的接口

---

## 三、手把手教程：计算系统总电量

这是最常用的功能，让我们一步步来操作。

### 步骤 1：点击展开接口

在页面上找到 `GET /api/metrics/calculate`，点击它。

展开后你会看到：
```
┌─────────────────────────────────────────────────────────────┐
│ GET  /api/metrics/calculate                    [Try it out] │
├─────────────────────────────────────────────────────────────┤
│ 计算指定指标                                                 │
│                                                             │
│ Parameters                                                  │
│ ─────────────────────────────────────────────────────────── │
│ Name              Description                               │
│ ─────────────────────────────────────────────────────────── │
│ metric_name *     指标名称                    [          ]  │
│ time_start *      开始时间                    [          ]  │
│ time_end *        结束时间                    [          ]  │
│ building_id       机楼筛选                    [          ]  │
│ system_id         系统筛选                    [          ]  │
│ equipment_type    设备类型                    [          ]  │
│ equipment_id      设备ID                      [          ]  │
│ sub_equipment_id  子设备ID                    [          ]  │
└─────────────────────────────────────────────────────────────┘
```

**说明：**
- 带 `*` 的是必填参数
- 不带 `*` 的是可选参数

### 步骤 2：点击 "Try it out" 按钮

在右上角找到蓝色的 **[Try it out]** 按钮，点击它。

点击后，所有输入框变成可编辑状态。

### 步骤 3：填写参数

在输入框中填写以下内容：

| 参数 | 填写内容 | 说明 |
|------|----------|------|
| metric_name | `系统总电量` | 要计算的指标名称 |
| time_start | `2025-07-01T00:00:00` | 开始时间（注意格式） |
| time_end | `2025-07-02T00:00:00` | 结束时间 |

**其他参数可以不填**（留空表示查询所有数据）

### 步骤 4：点击 "Execute" 执行

点击蓝色的 **[Execute]** 按钮。

### 步骤 5：查看结果

在下方会出现响应结果：

```
┌─────────────────────────────────────────────────────────────┐
│ Responses                                                   │
├─────────────────────────────────────────────────────────────┤
│ Code: 200                                                   │
│                                                             │
│ Response body                                               │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ {                                                       │ │
│ │   "metric_name": "系统总电量",                           │ │
│ │   "value": 123456.78,                                   │ │
│ │   "unit": "kWh",                                        │ │
│ │   "status": "success",                                  │ │
│ │   "quality_score": 100,                                 │ │
│ │   "trace": {                                            │ │
│ │     "formula": "系统总电量 = 冷机电量 + ...",            │ │
│ │     ...                                                 │ │
│ │   },                                                    │ │
│ │   "breakdown": [                                        │ │
│ │     {"equipment_type": "chiller", "value": 50000},      │ │
│ │     {"equipment_type": "chilled_pump", "value": 30000}, │ │
│ │     ...                                                 │ │
│ │   ]                                                     │ │
│ │ }                                                       │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

**结果说明：**
- `value`: 计算出的总电量值
- `unit`: 单位（kWh）
- `status`: 状态（success 表示成功）
- `breakdown`: 各设备类型的电量明细

---

## 四、手把手教程：查看数据质量

### 4.1 查看质量汇总

**步骤：**

1. 点击 `GET /api/quality/summary`
2. 点击 **[Try it out]**
3. 填写参数：
   - time_start: `2025-07-01T00:00:00`
   - time_end: `2025-07-02T00:00:00`
4. 点击 **[Execute]**

**返回结果示例：**
```json
{
  "total_records": 2784,
  "good_count": 2500,
  "warning_count": 200,
  "poor_count": 84,
  "avg_quality_score": 85.5,
  "avg_completeness_rate": 92.3,
  "total_gaps": 150,
  "total_negatives": 20,
  "total_jumps": 45
}
```

**结果含义：**
- 总共 2784 条记录
- 其中 2500 条质量好，200 条有警告，84 条质量差
- 平均质量分 85.5 分
- 发现 150 个时间缺口、20 个负值、45 个异常跳变

### 4.2 查看异常问题列表

**步骤：**

1. 点击 `GET /api/quality/issues`
2. 点击 **[Try it out]**
3. 填写参数：
   - time_start: `2025-07-01T00:00:00`
   - time_end: `2025-07-02T00:00:00`
   - issue_type: `gap`（只看时间缺口问题）
   - severity: `high`（只看严重问题）
4. 点击 **[Execute]**

**返回结果示例：**
```json
{
  "items": [
    {
      "issue_type": "gap",
      "bucket_time": "2025-07-01T10:00:00",
      "equipment_type": "chiller",
      "equipment_id": "G11-1-CH-01",
      "description": "存在 5 个时间缺口，最大缺口 3600 秒",
      "severity": "high"
    }
  ],
  "total": 10,
  "page": 1,
  "page_size": 20,
  "total_pages": 1
}
```

---

## 五、所有 API 详细说明

### 5.1 获取可用指标列表

```
GET /api/metrics/list
```

**用途：** 查看系统支持哪些指标

**参数：** 无

**示例返回：**
```json
{
  "metrics": ["系统总电量"]
}
```

---

### 5.2 计算指标

```
GET /api/metrics/calculate
```

**用途：** 计算指定时间范围内的指标值

**参数：**

| 参数名 | 必填 | 类型 | 说明 | 示例值 |
|--------|------|------|------|--------|
| metric_name | 是 | string | 指标名称 | `系统总电量` |
| time_start | 是 | datetime | 开始时间 | `2025-07-01T00:00:00` |
| time_end | 是 | datetime | 结束时间 | `2025-07-02T00:00:00` |
| building_id | 否 | string | 按机楼筛选 | `G11` |
| system_id | 否 | string | 按系统筛选 | `G11-1` |
| equipment_type | 否 | string | 按设备类型筛选 | `chiller` |
| equipment_id | 否 | string | 按设备ID筛选 | `G11-1-CH-01` |

**使用场景举例：**

1. **查询全站一天的总电量：**
   - metric_name: `系统总电量`
   - time_start: `2025-07-01T00:00:00`
   - time_end: `2025-07-02T00:00:00`

2. **查询 G11 机楼一天的总电量：**
   - metric_name: `系统总电量`
   - time_start: `2025-07-01T00:00:00`
   - time_end: `2025-07-02T00:00:00`
   - building_id: `G11`

3. **查询所有冷机一天的总电量：**
   - metric_name: `系统总电量`
   - time_start: `2025-07-01T00:00:00`
   - time_end: `2025-07-02T00:00:00`
   - equipment_type: `chiller`

---

### 5.3 数据质量汇总

```
GET /api/quality/summary
```

**用途：** 查看数据质量的整体情况

**参数：**

| 参数名 | 必填 | 说明 | 示例值 |
|--------|------|------|--------|
| time_start | 是 | 开始时间 | `2025-07-01T00:00:00` |
| time_end | 是 | 结束时间 | `2025-07-02T00:00:00` |
| building_id | 否 | 机楼筛选 | `G11` |
| system_id | 否 | 系统筛选 | `G11-1` |
| equipment_type | 否 | 设备类型 | `chiller` |
| quality_level | 否 | 质量等级 | `good` / `warning` / `poor` |
| granularity | 否 | 时间粒度 | `hour`(默认) / `day` |

---

### 5.4 数据质量明细

```
GET /api/quality/list
```

**用途：** 查看每条数据的质量详情

**参数：** 同上，额外支持分页：
- page: 页码（默认 1）
- page_size: 每页数量（默认 20，最大 100）

---

### 5.5 数据异常问题

```
GET /api/quality/issues
```

**用途：** 查看具体的数据异常问题

**参数：**

| 参数名 | 必填 | 说明 | 可选值 |
|--------|------|------|--------|
| time_start | 是 | 开始时间 | - |
| time_end | 是 | 结束时间 | - |
| issue_type | 否 | 问题类型 | `gap`(缺口) / `negative`(负值) / `jump`(跳变) / `out_of_range`(超量程) |
| severity | 否 | 严重程度 | `high` / `medium` / `low` |
| building_id | 否 | 机楼筛选 | - |
| equipment_type | 否 | 设备类型 | - |
| granularity | 否 | 时间粒度 | `hour` / `day` |
| page | 否 | 页码 | 默认 1 |
| page_size | 否 | 每页数量 | 默认 20 |

---

### 5.6 设备质量趋势

```
GET /api/quality/equipment/{equipment_id}/trend
```

**用途：** 查看某个设备的质量变化趋势

**参数：**

| 参数名 | 必填 | 说明 |
|--------|------|------|
| equipment_id | 是 | 设备ID（写在URL路径中） |
| time_start | 是 | 开始时间 |
| time_end | 是 | 结束时间 |
| granularity | 否 | 时间粒度 |

**示例：** 查看设备 G11-1-CH-01 的质量趋势
- URL: `/api/quality/equipment/G11-1-CH-01/trend`
- time_start: `2025-07-01T00:00:00`
- time_end: `2025-07-07T00:00:00`

---

## 六、常见问题

### Q1: 时间格式怎么写？

**答：** 使用 ISO 8601 格式：`YYYY-MM-DDTHH:MM:SS`

例如：
- `2025-07-01T00:00:00` = 2025年7月1日 0点0分0秒
- `2025-07-01T08:30:00` = 2025年7月1日 8点30分0秒

### Q2: 返回 "开始时间必须小于结束时间" 错误？

**答：** 检查 time_start 是否早于 time_end。

错误示例：
- time_start: `2025-07-02T00:00:00`
- time_end: `2025-07-01T00:00:00`  ← 结束时间比开始时间早

### Q3: 返回 "查询失败" 或 500 错误？

**答：** 可能原因：
1. 数据库连接失败 - 检查 MySQL 是否运行
2. 表不存在 - 检查 agg_hour_quality 表是否创建
3. 时间范围内没有数据

### Q4: 怎么知道有哪些 building_id 可以用？

**答：** 目前系统中的机楼ID包括：
- `G11` - G11机楼
- `G12` - G12机楼
- 等等（具体以数据库中的数据为准）

### Q5: equipment_type 有哪些可选值？

**答：** 见下方参考信息。

---

## 七、参考信息

### 设备类型对照表

| equipment_type 值 | 中文名称 |
|-------------------|----------|
| `chiller` | 冷机 |
| `chilled_pump` | 冷冻泵 |
| `cooling_pump` | 冷却泵 |
| `cooling_tower` | 开式冷却塔 |
| `cooling_tower_closed` | 闭式冷却塔 |
| `tower_fan` | 冷塔风机 |
| `closed_tower_pump` | 闭式塔泵 |
| `user_side_pump` | 用户侧循环泵 |
| `source_side_pump` | 水源侧循环泵 |
| `heat_recovery_primary_pump` | 余热回收一次泵 |
| `heat_recovery_secondary_pump` | 余热回收二次泵 |
| `fire_pump` | 消防泵 |

### 质量等级说明

| quality_level | 含义 | 质量分范围 |
|---------------|------|------------|
| `good` | 数据质量好 | 80-100 分 |
| `warning` | 有警告 | 60-80 分 |
| `poor` | 数据质量差 | 0-60 分 |

### 问题类型说明

| issue_type | 含义 | 说明 |
|------------|------|------|
| `gap` | 时间缺口 | 数据采集中断，某些时间点没有数据 |
| `negative` | 负值 | 出现不合理的负数值（如负电量） |
| `jump` | 异常跳变 | 数值突然大幅变化，可能是仪表故障 |
| `out_of_range` | 超量程 | 数值超出设备正常工作范围 |

### 严重程度说明

| severity | 含义 | 判断标准 |
|----------|------|----------|
| `high` | 严重 | 问题数量 >= 5 |
| `medium` | 中等 | 问题数量 2-4 |
| `low` | 轻微 | 问题数量 = 1 |

---

## 八、直接在浏览器中测试

除了使用 Swagger UI，你也可以直接在浏览器地址栏输入 URL 来测试：

**示例 1：获取指标列表**
```
http://localhost:8000/api/metrics/list
```

**示例 2：计算系统总电量**
```
http://localhost:8000/api/metrics/calculate?metric_name=系统总电量&time_start=2025-07-01T00:00:00&time_end=2025-07-02T00:00:00
```

**示例 3：查看数据质量汇总**
```
http://localhost:8000/api/quality/summary?time_start=2025-07-01T00:00:00&time_end=2025-07-02T00:00:00
```

**示例 4：查看严重的时间缺口问题**
```
http://localhost:8000/api/quality/issues?time_start=2025-07-01T00:00:00&time_end=2025-07-02T00:00:00&issue_type=gap&severity=high
```

---

## 九、停止服务

在运行 uvicorn 的 PowerShell 窗口中，按 `Ctrl + C` 即可停止服务。
