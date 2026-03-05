# carbon_metrics

`carbon_metrics` 是制冷系统能耗数据平台，包含 **FastAPI 后端** 和 **React 前端** 两部分。

- **后端**：23 个指标计算 + 数据质量查询，基于 FastAPI + PyMySQL。
- **前端**：管理界面，基于 React 18 + TypeScript + Ant Design 5，提供总览、数据质量、指标计算、设备管理四个页面。
- **数据库**：MySQL `cooling_system_v2`，数据范围 2025-07-01 ~ 2026-01-20，116 台设备（冷机 31 + 水泵 70 + 冷却塔 15）。

## Latest Update (2026-03-04)

- `carbon_metrics/backend/metrics/chiller.py`:
  - **修复冷机指标 sub_equipment_id 过滤问题**: 冷机平均负载率、最大负载率、波动系数在详情页计算失败，显示"缺少依赖数据: load_rate, load_ratio"。
  - **根因**: 依赖检查和数据查询复用了业务指标的 `sub_equipment_id` 过滤条件，导致基础依赖数据（`load_rate`/`load_ratio`）被错误过滤。
  - **解决方案**: 新增 `ctx_without_sub`（`sub_equipment_id=None`）用于基础数据依赖检查与查询，仅在业务结果聚合阶段应用用户选择的子设备口径。
  - **影响范围**: 仅调整冷机负载相关指标（平均负载率、最大负载率、波动系数）的依赖查询路径，不影响其他指标计算逻辑。
- `carbon_metrics/frontend/src/hooks/useMetrics.ts`:
  - 修复指标切换卡顿问题：为 `useMetricCalculate` 和 `useMetricCalculateBySubScopes` 添加 800ms 防抖，避免快速切换指标时触发大量并发请求。
  - 防抖机制与 `useMetricCoverage` 保持一致，使用 `useState + useEffect + setTimeout` 模式。
- `carbon_metrics/backend/metrics/chiller.py`:
  - 新增设备过载告警：当冷机最大负载率 > 100% 时，添加 `equipment_overload` 质量问题，提示"冷机负载率超过100%，设备处于过载运行状态"。
  - 告警包含详细信息：`max_load_rate`（实际值）和 `overload_percentage`（超载百分比）。
  - 状态保持为 `partial`（警告），不影响数据有效性。负载率 > 100% 表示设备实际运行超出额定容量，属于真实运行状态而非数据错误。
  - **性能优化**: 修复 `_select_load_metric()` 函数未使用查询缓存的问题，该函数为每个冷机指标执行 2 次 COUNT 查询检查依赖数据，导致批量计算时产生大量冗余查询。现改用 `_cached_fetchone()` 方法，相同查询只执行一次，显著提升计算性能。
- **性能建议**:
  - 对于大范围查询（如全月数据），建议设置 `DB_READ_TIMEOUT=60` 或更高，避免长查询超时。
  - 服务器 MySQL `innodb_buffer_pool_size` 建议设置为物理内存的 50-75%（当前服务器 754GB RAM，建议 64G）。
  - 确保 `DB_POOL_SIZE >= METRIC_CALC_WORKERS`，避免并行计算时连接池耗尽。

---

## Latest Update (2026-02-26)

- `carbon_metrics/backend/services/metric_calculator.py`:
  - 修复并行批量计算的 thundering herd 问题：并行模式（`METRIC_CALC_WORKERS > 1`）下，各线程原先各自独立查询数据库（`query_cache=None`），相同 SQL 重复执行 N 次。现新增 `ThreadSafeCache` 线程安全缓存包装类，并行路径共享同一缓存实例，相同查询只执行一次。
  - `ThreadSafeCache` 使用 `threading.Lock` 保护读写操作，防止缓存 stampede。
- `carbon_metrics/backend/metrics/base.py`:
  - `_cached_fetchone` / `_cached_fetchall` 新增锁感知逻辑：通过 `getattr(cache, 'lock', None)` 检测缓存是否为线程安全实例，是则在 check-execute-store 全程加锁；普通 dict 缓存（串行模式）无额外开销。
- `carbon_metrics/backend/metrics/energy.py`, `pump.py`, `tower.py`:
  - MySQL 会话变量 SET 语句合并：`energy.py` 每个查询函数的 3 条 `SET @var` 合并为 1 条（6→2），`pump.py`（2→1）、`tower.py`（2→1），总计减少 6 次 DB 网络往返。
- `carbon_metrics/backend/metrics/energy.py`:
  - 内联缓存检查（`_query_energy_by_type`、`_query_energy_by_bucket_type`）同步适配锁感知模式，与 `base.py` 保持一致。
- `carbon_metrics/backend/db.py`:
  - 新增注释：建议 `DB_POOL_SIZE >= METRIC_CALC_WORKERS` 以避免并行计算时频繁创建新连接。
- `carbon_metrics/frontend/src/pages/MetricDetail/QualityIssuesPanel.tsx`:
  - 修复前端能耗页面崩溃：10 个 `pagination={false}` 的 Table 改为分页模式（`pageSize: 10, showSizeChanger: true`），避免大数据量渲染导致浏览器 OOM。
- `carbon_metrics/frontend/src/components/ErrorBoundary.tsx`（新增）:
  - React Error Boundary 组件：捕获渲染异常，展示 Ant Design `Result` 错误页面并提供刷新按钮。支持 `resetKey`（绑定 `location.pathname`），页面导航时自动重置错误状态。
- `carbon_metrics/frontend/src/App.tsx`:
  - 路由层包裹 `<ErrorBoundary>`，防止单页面渲染崩溃导致整个应用白屏。
- 新增 pytest 测试基础设施：
  - `pytest.ini` + `tests/conftest.py`（4 个共享 fixture）
  - `tests/test_cache.py`：ThreadSafeCache 基本操作、线程安全、并行路径缓存验证（6 tests）
  - `tests/test_set_merge.py`：SET 语句合并计数验证（5 tests）
  - `tests/test_frontend_build.py`：前端构建通过性验证（1 test）

---

## Latest Update (2026-02-24)

- `carbon_metrics/backend/metrics/energy.py`:
  - 能耗占比指标（冷机能耗占比/水泵能耗占比/风机能耗占比）交集策略从"宽松交集"改为"严格交集"：每个小时必须四类组件（chiller/chilled_pump/cooling_pump/tower）均有数据且总能耗>0才纳入计算，与 Ground Truth 计算逻辑对齐。
  - 新增 `STRICT_INTERSECTION_KEYS` 常量，显式定义严格交集所需的四类组件键。
- `carbon_metrics/backend/metrics/energy.py`, `pump.py`, `tower.py`:
  - 增量数据源动态检测（agg_last fallback）：部分设备类型的原始能耗数据为增量格式（`正向有功电度`），pipeline 误用 `delta` 聚合导致 `agg_delta` 近零。后端通过动态阈值检测（`AVG(ABS(agg_last)) < 500`）自动判断每个 `equipment_type` 的数据格式，增量数据读 `agg_last`，累计数据读 `agg_delta`。
  - 动态检测机制：SQL 使用 CTE `energy_type_mode` 按 `equipment_type` 分组计算 `AVG(ABS(agg_last))`，低于阈值（500.0）判定为增量数据。阈值通过 MySQL 会话变量 `@incr_threshold` 传入。
  - `energy.py`：两个 SQL 查询函数（`_query_energy_by_type`、`_query_energy_by_bucket_type`）均使用 `energy_type_mode` + `energy_raw` 双 CTE 结构，动态选择 `agg_last` 或 `agg_delta`。常量 `_INCREMENTAL_DATA_THRESHOLD = 500.0`。
  - `pump.py`：`_PumpEnergyDensityMetric` 使用 `energy_type_mode` CTE + `CROSS JOIN` 动态选择能耗列。
  - `tower.py`：`TowerEfficiencyMetric` 使用 `tower_energy_mode` CTE + `JOIN` 动态选择能耗列。
  - 此方案适用于所有建筑（G11、G11-3、G12），无需维护静态设备类型列表。
- `carbon_metrics/backend/metrics/energy.py`, `pump.py`, `tower.py`:
  - 新增正增量钳位（Positive Delta Clamp）：当单小时 `agg_delta` 超过阈值（默认 1000 kWh）时视为电表重置噪声，该值从 SUM 中剔除。
  - 阈值通过环境变量 `POSITIVE_DELTA_CLAMP_THRESHOLD` 控制（默认 `1000.0`）。
- `carbon_metrics/backend/metrics/base.py`:
  - 新增 `_positive_delta_clamp_threshold` 属性及 `_parse_positive_delta_clamp_threshold()` 静态方法，从环境变量读取正增量钳位阈值。

---

## Latest Update (2026-02-16)

- `carbon_metrics/frontend/src/pages/Dashboard/MetricCategoryCards.tsx`:
  - 修复总览页与指标分析页数值不一致问题：总览页原先用单次 batch 调用（无 `equipment_type` 筛选）计算所有指标，导致冷机COP 等需要特定设备类型的指标值与详情页不同。
  - 现按 `fixedEquipmentType` 分组发起多个 batch 请求（使用 `useQueries`），每组携带正确的 `equipment_type`，与指标分析页的计算口径一致。
- `carbon_metrics/frontend/src/pages/Metrics/index.tsx`:
  - 从总览页点击进入指标分析页时，`sub_scope` 默认值从 `'all'`（主备分算模式）改为 `'null'`（未区分/单值模式），使初始展示值与总览页一致。用户可手动切换到分算模式。
  - 当所有指标均无可用数据时，显示 Alert 警告 Banner 提示用户调整筛选条件。
- `carbon_metrics/backend/metrics/chiller.py`, `flow.py`, `temperature.py`, `tower.py`:
  - 4 个 JOIN 类指标的 `minimum_calculable_principle` 质量信息补齐 `intersection_hours` 和 `expected_hours` 字段，前端交集标签（"基于 X/Y 小时交集"）现在可以正常渲染。
  - `expected_hours` 按 `ceil((time_end - time_start) / 3600)` 计算，与 `energy.py` 保持一致。
- `carbon_metrics/backend/metrics/energy.py` + `stability.py`:
  - `clamp_threshold`（负增量阈值）改用 MySQL 会话变量 `@ndc_threshold`，消除位置参数对齐风险。
- `carbon_metrics/backend/metrics/stability.py`:
  - 运行时长占比分母从 `period_hours × device_count`（理论值）改为 `record_count`（实际有数据的设备·小时数），符合最小可算原则。
  - 新增 `minimum_calculable_principle` 质量信息，展示实际覆盖率与理论最大值对比。
- `carbon_metrics/frontend/src/constants/metricFilterConfig.ts`:
  - `冷机COP` 筛选配置新增 `fixedEquipmentType: 'chiller'`，主备口径下拉仅查询冷机设备的可用范围，避免其他设备类型干扰。
- `carbon_metrics/frontend/src/hooks/useMetrics.ts`:
  - 统一 split 模式（主/备/未区分并列）与单独查询的缓存 key 格式，切换口径时可命中已有缓存。
- `carbon_metrics/backend/routers/metrics.py`:
  - `data_version` 查询结果缓存 3 秒，减少连续请求的重复 DB 查询。
- `carbon_metrics/backend/db.py`:
  - 连接池默认 `pool_size` 从 4 提升至 8，新增 `DB_POOL_SIZE` 环境变量支持（范围 2-32）。

---

## Latest Update (2026-02-14)

- `pipeline/pipeline/mapping.py`:
  - Added deterministic chiller id extraction for tag variants like `1号冷机`, `冷机1`, `1#冷机`, `1_冷机电流百分比`, `1_冷机累计运行时间`.
  - `point_mapping` upsert now updates all mutable mapping fields (`equipment_type/equipment_id/sub_equipment_id/metric_category/agg_method/unit/...`), so remap can backfill historical NULL equipment ids.
  - Added non-blocking chiller-core mapping audit logs for NULL `equipment_id` rows in `load_rate/runtime/power/cooling_capacity`.
- `carbon_metrics/backend/metrics/chiller.py`:
  - COP power scope is now strict by selected `sub_equipment_id` and no longer mixes `main` with NULL scope by default.
- `carbon_metrics/backend/metrics/energy.py`:
  - `系统总电量` and all energy ratio metrics apply the minimum-calculable principle in unscoped mode:
    - denominator is calculated only on hourly intersection of required components (`chiller/chilled_pump/cooling_pump/tower`),
    - no intersection -> `no_data`,
    - intersection scope and excluded hours are recorded in `quality_issues` and `data_source_condition`.

### Recommended Recompute (No Re-Ingest)

```powershell
python -m pipeline.run_pipeline --map --canonical --agg --quality --metrics --no-progress
```

Use `--ingest` only when raw Excel sources changed.

---

## 目录

- [1. 功能概览](#1-功能概览)
  - [1.1 指标计算](#11-指标计算)
  - [1.2 数据质量与设备查询](#12-数据质量与设备查询)
  - [1.3 前端页面](#13-前端页面)
  - [1.4 健康与文档](#14-健康与文档)
- [2. 技术栈](#2-技术栈)
  - [2.1 后端](#21-后端)
  - [2.2 前端](#22-前端)
- [3. 目录结构](#3-目录结构)
- [4. 数据依赖与表说明](#4-数据依赖与表说明)
  - [4.1 核心事实表](#41-核心事实表)
  - [4.2 质量聚合表](#42-质量聚合表)
  - [4.3 设备相关数据来源](#43-设备相关数据来源)
  - [4.4 离线质量报告](#44-离线质量报告)
- [5. 环境变量](#5-环境变量)
  - [5.1 必需（数据库）](#51-必需数据库)
  - [5.2 可选](#52-可选)
- [6. 安装与启动](#6-安装与启动)
  - [6.1 后端依赖](#61-后端依赖)
  - [6.2 前端依赖](#62-前端依赖)
  - [6.3 启动后端](#63-启动后端)
  - [6.4 启动前端](#64-启动前端)
  - [6.5 构建前端](#65-构建前端)
  - [6.6 局域网访问](#66-局域网访问不暴露公网)
- [7. API 一览](#7-api-一览)
  - [7.1 指标 API](#71-指标-api)
  - [7.2 质量 API](#72-质量-api)
  - [7.3 设备 API](#73-设备-api)
- [8. 请求示例](#8-请求示例)
- [9. 日志与可观测性](#9-日志与可观测性)
- [10. 开发说明](#10-开发说明)
  - [10.1 新增后端指标的推荐流程](#101-新增后端指标的推荐流程)
  - [10.2 前端开发说明](#102-前端开发说明)
  - [10.3 代码约定](#103-代码约定)
- [11. 本地校验](#11-本地校验)
- [12. 相关文档](#12-相关文档)
- [13. 已知限制](#13-已知限制)

---

## 1. 功能概览

### 1.1 指标计算
- 指标列表查询：`/api/metrics/list`
- 单指标计算：`/api/metrics/calculate`
- 支持可选过滤维度：`building_id`、`system_id`、`equipment_type`、`equipment_id`、`sub_equipment_id`
- 返回内容包括：指标值、单位、状态、质量评分、公式追踪、SQL、分解明细

### 1.2 数据质量与设备查询
- 质量汇总：`/api/quality/summary`
- 质量明细分页：`/api/quality/list`
- 异常问题分页：`/api/quality/issues`
- 设备质量趋势：`/api/quality/equipment/{equipment_id}/trend`
- 离线报告读取：`/api/quality/raw-report`（读取 `docs/data_quality_deep_report.csv`）
- 设备下拉列表：`/api/equipment/ids`

### 1.3 前端页面

| 路由 | 页面 | 说明 |
|------|------|------|
| `/` | Dashboard 总览 | 质量概览卡片 + 指标分类卡片 |
| `/quality` | 数据质量 | 5 个 Tab：质量汇总、质量明细、异常问题、原始报告、设备列表 |
| `/metrics` | 指标分析 | 选择时间范围计算指标，展示结果、公式追溯、质量问题、分解明细 |
| `/quality/equipment/:equipmentId` | 设备详情 | 单设备质量趋势 |

补充：
- `指标分析 -> 计算追溯` 显式展示 `主机(main)`、`备机(backup)`、`未区分(null)` 三类口径状态（计入/排除/未限制/按筛选）。
- 其中 `null` 表示原始点位未拆分主备，属于单通道采集。
- `指标分析` 筛选栏新增 `主备口径`：
  - 选择 `全部(主/备/null)` 时，页面会并列输出 `main / backup / null` 三组独立计算结果；
  - 选择 `main` / `backup` / `null` 时，仅计算对应口径。
- `指标分析 -> 质量问题 -> 缺失时段样例` 默认按 `metric_name + bucket_time` 聚合展示；可切换“查看设备明细”查看设备维度明细行，避免把同一小时多设备误解为时间异常。
- `指标分析` 页在时间范围超过 31 天时，默认不自动拉取 coverage 概览；需点击“展开详情”后按需加载，避免大范围请求阻塞页面。

### 1.4 健康与文档
- 健康检查：`/health`
- 根路径（API）：`/`
- Swagger：`/docs`

---

## 2. 技术栈

### 2.1 后端
- Python 3.9+
- FastAPI + Pydantic
- PyMySQL
- Uvicorn

### 2.2 前端
- React 18 + TypeScript
- Vite 6
- Ant Design 5 + @ant-design/icons
- TanStack React Query 5
- React Router 6
- Axios
- dayjs

---

## 3. 目录结构

```text
src/
├── README.md
└── carbon_metrics/
    ├── API 使用教程.md
    ├── requirements.txt
    ├── logs/
    │   └── metric_calculations.log
    ├── backend/
    │   ├── main.py                 # FastAPI 入口
    │   ├── config.py               # 环境变量配置
    │   ├── db.py                   # DB 连接与游标上下文
    │   ├── models.py               # Pydantic 模型
    │   ├── routers/
    │   │   ├── metrics.py          # 指标 API
    │   │   ├── quality.py          # 质量 API
    │   │   └── equipment.py        # 设备 API
    │   ├── services/
    │   │   ├── metric_calculator.py
    │   │   └── quality_service.py
    │   └── metrics/
    │       ├── base.py             # 指标基类 + CalculationResult
    │       ├── energy.py           # 4 个能耗指标
    │       ├── temperature.py      # 5 个温度指标
    │       ├── flow.py             # 3 个流量指标
    │       ├── chiller.py          # 4 个冷机指标（含冷机COP）
    │       ├── pump.py             # 2 个水泵指标
    │       ├── tower.py            # 2 个冷却塔指标
    │       ├── stability.py        # 2 个稳定性指标
    │       └── maintenance.py      # 1 个维护指标
    └── frontend/
        ├── package.json
        ├── vite.config.ts          # Vite 配置 + /api 代理
        ├── tsconfig.json
        └── src/
            ├── main.tsx            # 入口
            ├── App.tsx             # 路由定义
            ├── api/                # Axios client + API 调用 + 类型
            ├── hooks/              # React Query hooks
            ├── constants/          # 设备类型、指标分类、质量等级映射
            ├── layouts/            # AppLayout (Sider + Header + Content)
            ├── components/         # 共享组件 (9 个)
            └── pages/
                ├── Dashboard/
                ├── Quality/        # 数据质量页（5 Tab）
                ├── Metrics/        # 指标分析页
                ├── Equipment/
                └── MetricDetail/
```

---

## 4. 数据依赖与表说明

### 4.1 核心事实表
服务当前直接查询 `agg_hour`，主要使用字段包括：
- 维度：`bucket_time`、`building_id`、`system_id`、`equipment_type`、`equipment_id`、`sub_equipment_id`、`metric_name`
- 聚合值：`agg_avg`、`agg_min`、`agg_max`、`agg_delta`、`agg_last`
- 质量标记：`quality_flags`

说明：
- 对能耗占比与运行时长占比类指标，后端使用负值阈值规则（`NEGATIVE_DELTA_CLAMP_THRESHOLD`，默认 `0.1`）：
  - `-threshold <= agg_delta < 0`：按 `0` 参与求和（视为小噪声，剔除出 SUM）。
  - `agg_delta < -threshold`：同样剔除出 SUM，并额外触发告警。
- 处理痕迹会写入 `quality_issues`（`negative_delta_clamped` / `negative_delta_alert` / `result_beautified`），便于追溯“净化口径”影响。
- 对能耗类指标，后端还使用正增量钳位规则（`POSITIVE_DELTA_CLAMP_THRESHOLD`，默认 `1000.0`）：
  - `agg_delta > threshold`：视为电表重置噪声，按 `0` 参与求和。
  - 处理痕迹同样写入 `quality_issues`。
- 对能耗占比指标（冷机/水泵/风机能耗占比），后端使用严格交集原则：
  - 每个小时必须四类组件（冷机/冷冻泵/冷却泵/冷塔）均有能耗数据，且该小时总能耗 > 0，才纳入占比计算。
  - 无严格交集小时时返回 `no_data` 状态。
- 对能耗查询，后端使用动态阈值检测自动判断每个 `equipment_type` 的数据格式：
  - 机制：SQL CTE 按 `equipment_type` 计算 `AVG(ABS(agg_last))`，低于 500 判定为增量数据（读 `agg_last`），高于 500 判定为累计数据（读 `agg_delta`）。
  - 原因：部分设备原始数据为增量格式（`正向有功电度`），pipeline 误用 `delta` 聚合导致 `agg_delta` 近零。
  - 此方案无需维护静态设备类型列表，适用于所有建筑。

### 4.2 质量聚合表
质量接口依赖：
- `agg_hour_quality`
- `agg_day_quality`
- 完整率问题明细字段：
  - `missing_bucket_samples`：按小时聚合后的缺失样例（默认展示）
  - `missing_bucket_device_samples`：设备维度缺失样例（明细开关展示）

可参考建表脚本：
- `norm/create_sql/database_v2_2.sql`

### 4.3 设备相关数据来源
- 设备下拉接口 `/api/equipment/ids`：基于 `agg_hour` 去重查询，仅返回有聚合数据的设备。
- 设备主数据维护：`equipment_registry`。

### 4.4 离线质量报告
接口 `/api/quality/raw-report` 读取：
- `docs/data_quality_deep_report.csv`

---

## 5. 环境变量

### 5.1 必需（数据库）
- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`

### 5.2 可选
- `DEBUG`：`true/false`
- `DB_CONNECT_TIMEOUT`：连接超时（秒，正整数）
- `DB_READ_TIMEOUT`：读取超时（秒，正整数）
- `DB_WRITE_TIMEOUT`：写入超时（秒，正整数）
- `METRIC_CALC_WORKERS`：指标批量计算并行线程数（1-16，默认 4）
- `METRIC_API_CACHE_TTL_SECONDS`：指标接口响应缓存秒数（默认 `30`，`0` 表示关闭）
- `NEGATIVE_DELTA_CLAMP_THRESHOLD`：负增量小噪声归零阈值（浮点数，默认 `0.1`）
- `SENSOR_BIAS_POINT_BLACKLIST`：传感器偏置黑名单关键词（逗号分隔，默认 `A3_GYK1113`）
- `SENSOR_BIAS_MIN_NEGATIVE_COUNT`：命中黑名单后触发告警的最小负值条数（默认 `20`）
- `DB_POOL_SIZE`：数据库连接池大小（2-32，默认 `8`）
- `CHILLER_COP_MIN_POWER_KW`：冷机COP口径中“有效功率小时”下限（默认 `20`，过滤小功率待机噪声）
- `POSITIVE_DELTA_CLAMP_THRESHOLD`：正增量钳位阈值（浮点数，默认 `1000.0`），超过此值的单小时 `agg_delta` 视为电表重置噪声并从 SUM 中剔除

说明：
- `DB_PORT` 非法时会回退到 `3306`。
- 超时变量未配置时保持默认连接行为。
- `METRIC_CALC_WORKERS=1` 时会走串行计算并启用共享查询缓存；>1 时走并行计算。
- `METRIC_API_CACHE_TTL_SECONDS` 仅缓存 `/api/metrics/calculate` 与 `/api/metrics/coverage` 响应；计算公式和口径不变。缓存会结合 `agg_hour`/`agg_hour_quality` 的最新版本信息自动失效。
- `NEGATIVE_DELTA_CLAMP_THRESHOLD` 用于区分小负值与严重负值；两者都会从 SUM 中剔除，严重负值会额外告警。
- `SENSOR_BIAS_POINT_BLACKLIST` 用于标记重点关注点位，命中后会输出 `sensor_bias` 质量告警。
- `冷机COP` 分母口径使用冷机主功率（`sub_equipment_id in (NULL,'','main')`），不与 `backup` 混算。
- `POSITIVE_DELTA_CLAMP_THRESHOLD` 用于过滤电表重置导致的异常大正增量；与 `NEGATIVE_DELTA_CLAMP_THRESHOLD` 互补，分别处理正/负方向的噪声。
- 部署建议：`DB_POOL_SIZE` 应 >= `METRIC_CALC_WORKERS`，避免并行计算时频繁创建新连接。低配服务器建议设置 `METRIC_CALC_WORKERS=1`，此时后端走串行计算并启用共享查询缓存，减少重复 SQL 查询。

---

## 6. 安装与启动

### 6.1 后端依赖

```powershell
pip install -r src/carbon_metrics/requirements.txt
```

如果你当前就在 `carbon_metrics/` 目录内，可改用：

```powershell
pip install -r requirements.txt
```

### 6.2 前端依赖

```powershell
cd src/carbon_metrics/frontend
npm install
```

### 6.3 启动后端

推荐在仓库根目录先进入 `src/` 后启动：

```powershell
cd src
uvicorn carbon_metrics.backend.main:app --reload
```

如果你当前就在 `src/carbon_metrics/` 目录内，可改用：

```powershell
uvicorn backend.main:app --reload
```

启动后访问：
- Swagger: `http://127.0.0.1:8000/docs`
- 健康检查: `http://127.0.0.1:8000/health`

### 6.3.1 切换数据库（推荐做法）

后端只通过环境变量读取数据库连接信息。切换数据库时，不需要改代码，按以下顺序执行：

1. 在当前终端设置新的连接参数（PowerShell）：

```powershell
$env:DB_HOST="127.0.0.1"
$env:DB_PORT="3306"
$env:DB_USER="root"
$env:DB_PASSWORD="你的密码"
$env:DB_NAME="cooling_system_v2"
```

2. 在同一个终端启动后端（让新变量生效）：

```powershell
cd src
uvicorn carbon_metrics.backend.main:app --reload
```

3. 快速验证是否连到目标库：

```powershell
curl "http://127.0.0.1:8000/health"
curl "http://127.0.0.1:8000/api/metrics/list"
```

4. 需要切到另一个库时，重复第 1 步并重启后端。

说明：
- 变量只对“当前终端会话”生效；开新终端后需要重新设置。
- 不要把真实密码写进仓库文件或提交到 Git。
- 如果日志出现 `Access denied ... (using password: NO)`，说明当前进程没读到 `DB_PASSWORD`，请在同一终端重新设置后重启后端。

### 6.4 启动前端

```powershell
cd src/carbon_metrics/frontend
npm run dev
```

启动后访问：
- 前端页面: `http://127.0.0.1:5173`
- Vite 自动将 `/api` 请求代理到 `http://localhost:8000`

### 6.5 构建前端

```powershell
cd src/carbon_metrics/frontend
npm run build
```

### 6.6 局域网访问（不暴露公网）

后端：

```powershell
cd src
uvicorn carbon_metrics.backend.main:app --reload --host 0.0.0.0 --port 8000
```

前端（已在 `vite.config.ts` 配置 `host: '0.0.0.0'`）：

```powershell
cd src/carbon_metrics/frontend
npm run dev
```

局域网其他设备访问：
- 前端：`http://<你的局域网IP>:5173`
- 后端文档：`http://<你的局域网IP>:8000/docs`

注意：
- 需要放行本机防火墙端口 `5173`、`8000`。

---

## 7. API 一览

### 7.1 指标 API
- `GET /api/metrics/list`
- `GET /api/metrics/calculate`
- `GET /api/metrics/coverage`
- `POST /api/metrics/calculate_batch`

`/api/metrics/calculate` 必填参数：
- `metric_name`
- `time_start`
- `time_end`

可选参数：
- `building_id`
- `system_id`
- `equipment_type`
- `equipment_id`
- `sub_equipment_id`

`sub_equipment_id` 说明：
- `main`：仅主机口径
- `backup`：仅备机口径
- `__NULL__`：仅未拆分主备（`sub_equipment_id IS NULL OR = ''`）
- 不传：不按子设备过滤（前端“全部”模式会分别发起 `main / backup / __NULL__` 三路请求）

返回说明（重点）：
- `quality_issues` 中的 `missing_dependency` 会带 `details.missing_metric_diagnostics`
- `missing_metric_diagnostics` 可区分：
  - 库里有数据但被筛选掉（scope 过滤）
  - canonical 有数据但 agg 无数据
  - raw+mapping 有数据但 canonical 无数据
  - mapping 未命中（会附 `unmapped_tag_samples` Top 样本）
- `quality_issues` 中的 `completeness` 会带：
  - `details.incomplete_bucket_count`：当前范围完整率不足的时段总数
  - `details.missing_bucket_samples`：按小时聚合后的缺失样例（含 `metric_name`、`bucket_time`、`actual_samples`、`expected_samples`、`completeness_rate`）
  - `details.missing_bucket_device_samples`：设备维度缺失样例（含 `building_id`、`system_id`、`equipment_type`、`equipment_id`、`sub_equipment_id`）
- `quality_issues` 可能包含：
  - `negative_delta_clamped`：阈值内小负值已从 SUM 剔除（会返回阈值与影响量）
  - `negative_delta_alert`：超阈值负值已从 SUM 剔除并告警
  - `result_beautified`：结果使用“净化口径”（负值不进 SUM），会返回被剔除负值总量
  - `sensor_bias`：命中黑名单且负值频繁的疑似传感器偏置点位
  - `ratio_out_of_range`：结果比例超出常规范围（保留真实值）

`/api/metrics/calculate_batch` 请求体参数：
- `metric_names`（可选，不传则批量计算全部已注册指标）
- `time_start` / `time_end`（必填）
- `building_id` / `system_id` / `equipment_type` / `equipment_id` / `sub_equipment_id`（可选）

`/api/metrics/coverage` 返回重点字段：
- `summary`：可计算率与状态分布
- `items[].input_records` / `items[].valid_records`：每个指标的取数样本
- `items[].issue_types`：指标命中的质量问题类型
- `available_metric_counts`：原子指标覆盖计数（如 `energy` / `power`）
- `metric_input_counts`：业务指标输入记录计数（如 `系统总电量` / `冷机COP`）
- `missing_dependency_counts`：缺失依赖频次（用于前端 Top 风险提示）

### 7.2 质量 API
- `GET /api/quality/summary`
- `GET /api/quality/list`
- `GET /api/quality/issues`
- `GET /api/quality/equipment/{equipment_id}/trend`
- `GET /api/quality/raw-report`

### 7.3 设备 API
- `GET /api/equipment/ids`

---

## 8. 请求示例

### 8.1 列出支持指标
```bash
curl "http://127.0.0.1:8000/api/metrics/list"
```

### 8.2 计算指标
```bash
curl "http://127.0.0.1:8000/api/metrics/calculate?metric_name=系统总电量&time_start=2025-07-01T00:00:00&time_end=2025-07-02T00:00:00"
```

建议：
- 先调用 `/api/metrics/list` 获取当前可用 `metric_name`，再发计算请求。

### 8.3 查询质量汇总
```bash
curl "http://127.0.0.1:8000/api/quality/summary?time_start=2025-07-01T00:00:00&time_end=2025-07-02T00:00:00&granularity=hour"
```

### 8.4 查询问题列表
```bash
curl "http://127.0.0.1:8000/api/quality/issues?time_start=2025-07-01T00:00:00&time_end=2025-07-02T00:00:00&issue_type=gap&severity=high&page=1&page_size=20"
```

---

## 9. 日志与可观测性

### 9.1 指标计算日志
- 文件：`src/carbon_metrics/logs/metric_calculations.log`
- 轮转：`10MB * 5` 份
- 内容：指标名、结果、状态、时间范围、过滤条件、公式、SQL、记录数、质量评分

### 9.2 质量问题扫描日志
`/api/quality/issues` 会记录扫描统计：
- `total_issues`
- `page`
- `page_size`
- `elapsed_ms`

### 9.3 离线报告容错日志
`/api/quality/raw-report` 对坏行会 `warning` 并跳过，不会因单行脏数据导致全接口失败。

---

## 10. 开发说明

### 10.1 新增后端指标的推荐流程
1. 在 `backend/metrics/` 新建指标类，继承 `BaseMetric`。
2. 实现 `metric_name`、`unit`、`formula`、`calculate()`。
3. 在 `services/metric_calculator.py` 的 `METRICS` 注册映射。
4. 通过 `/api/metrics/list` 验证是否可见。
5. 通过 `/api/metrics/calculate` 验证计算与 trace 返回。

### 10.2 前端开发说明
- 前端通过 Vite proxy 将 `/api` 请求转发到后端 `localhost:8000`，开发时需同时启动后端。
- 状态管理使用 TanStack React Query，API 调用封装在 `api/` 目录，hooks 封装在 `hooks/` 目录。
- 页面筛选条件变化时自动重置分页到第 1 页。
- `time_start/time_end` 使用 URL 查询参数作为全局时间范围：总览页选择后，切换到指标页/质量页会继承该时间范围。
- Table 的 `rowKey` 使用 `building_id + system_id + equipment_type + equipment_id + sub_equipment_id + metric_name` 复合键确保唯一性。

### 10.3 代码约定
- 后端 SQL 一律参数化，避免字符串拼接注入风险。
- 后端返回 `CalculationResult` 的 `failed/no_data/success` 明确状态。
- 前端路由参数和 API 路径中的动态 ID 使用 `encodeURIComponent` 编码。
- 对 IO/解析错误做可控降级，不要让单点坏数据放大成全量失败。

---

## 11. 本地校验

### 11.1 后端语法检查

```powershell
python -m py_compile src/carbon_metrics/backend/main.py
python -m py_compile src/carbon_metrics/backend/services/metric_calculator.py
python -m py_compile src/carbon_metrics/backend/services/quality_service.py
python -m py_compile src/carbon_metrics/backend/metrics/tower.py
```

### 11.2 前端构建检查

```powershell
cd src/carbon_metrics/frontend
npm run build
```

### 11.3 一体化抽查（validate_data.py）

仅跑抽查（推荐日常回归）：

```powershell
python norm/create_sql/validate_data.py --spotcheck-only --spotcheck-energy-dir data1 --spotcheck-params-dir data1
```

全量校验后追加抽查：

```powershell
python norm/create_sql/validate_data.py --spotcheck --spotcheck-energy-dir data1 --spotcheck-params-dir data1
```

可选参数：
- `--spotcheck-metrics`：逗号分隔的指标列表
- `--spotcheck-time-start` / `--spotcheck-time-end`：指标抽查时间范围
- `--spotcheck-files-per-type`：每类(`tag`/`device`)抽查文件数
- `--spotcheck-seed`：随机种子（固定样本）

> 后端：如果环境对 `__pycache__` 写入有限制，可改用 AST/静态检查脚本进行语法验证。

---

## 12. 相关文档

- 接口使用手册：`src/carbon_metrics/API 使用教程.md`
- 质量表 SQL：`norm/create_sql/database_v2_2.sql`
- 质量离线报告样例：`docs/data_quality_deep_report.csv`

---

## 13. 已知限制

- 指标名称以系统注册为准，建议通过 `/api/metrics/list` 动态获取，不要硬编码。
- `/api/quality/issues` 为 issue 级分页，时间窗口很大时仍会有扫描成本，建议合理控制查询范围。
- 本服务默认直接读取 MySQL 聚合表，数据库索引质量会显著影响响应时间。
- 前端目前为纯表格/卡片展示，ECharts 图表可视化尚未实现（Phase 5 待开发）。
