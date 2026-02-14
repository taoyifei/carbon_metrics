# carbon_metrics

`carbon_metrics` 是制冷系统能耗数据平台，包含 **FastAPI 后端** 和 **React 前端** 两部分。

- **后端**：23 个指标计算 + 数据质量查询，基于 FastAPI + PyMySQL。
- **前端**：管理界面，基于 React 18 + TypeScript + Ant Design 5，提供总览、数据质量、指标计算、设备管理四个页面。
- **数据库**：MySQL `cooling_system_v2`，数据范围 2025-07-01 ~ 2026-01-20，116 台设备（冷机 31 + 水泵 70 + 冷却塔 15）。

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
- 聚合值：`agg_avg`、`agg_min`、`agg_max`、`agg_delta`
- 质量标记：`quality_flags`

说明：
- 对能耗占比与运行时长占比类指标，后端使用负值阈值规则（`NEGATIVE_DELTA_CLAMP_THRESHOLD`，默认 `0.1`）：
  - `-threshold <= agg_delta < 0`：按 `0` 参与求和（视为小噪声，剔除出 SUM）。
  - `agg_delta < -threshold`：同样剔除出 SUM，并额外触发告警。
- 处理痕迹会写入 `quality_issues`（`negative_delta_clamped` / `negative_delta_alert` / `result_beautified`），便于追溯“净化口径”影响。

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
- `CHILLER_COP_MIN_POWER_KW`：冷机COP口径中“有效功率小时”下限（默认 `20`，过滤小功率待机噪声）

说明：
- `DB_PORT` 非法时会回退到 `3306`。
- 超时变量未配置时保持默认连接行为。
- `METRIC_CALC_WORKERS=1` 时会走串行计算并启用共享查询缓存；>1 时走并行计算。
- `METRIC_API_CACHE_TTL_SECONDS` 仅缓存 `/api/metrics/calculate` 与 `/api/metrics/coverage` 响应；计算公式和口径不变。缓存会结合 `agg_hour`/`agg_hour_quality` 的最新版本信息自动失效。
- `NEGATIVE_DELTA_CLAMP_THRESHOLD` 用于区分小负值与严重负值；两者都会从 SUM 中剔除，严重负值会额外告警。
- `SENSOR_BIAS_POINT_BLACKLIST` 用于标记重点关注点位，命中后会输出 `sensor_bias` 质量告警。
- `冷机COP` 分母口径使用冷机主功率（`sub_equipment_id in (NULL,'','main')`），不与 `backup` 混算。

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
