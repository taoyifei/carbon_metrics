# carbon_metrics

`carbon_metrics` 是制冷系统能耗数据平台，包含 **FastAPI 后端** 和 **React 前端** 两部分。

- **后端**：21 个指标计算 + 数据质量查询，基于 FastAPI + PyMySQL。
- **前端**：管理界面，基于 React 18 + TypeScript + Ant Design 5，提供总览、数据质量、指标计算、设备管理四个页面。
- **数据库**：MySQL `cooling_system_v2`，数据范围 2025-07-01 ~ 2026-01-20，116 台设备（冷机 31 + 水泵 70 + 冷却塔 15）。

---

## 1. 功能概览

### 1.1 指标计算
- 指标列表查询：`/api/metrics/list`
- 单指标计算：`/api/metrics/calculate`
- 支持可选过滤维度：`building_id`、`system_id`、`equipment_type`、`equipment_id`、`sub_equipment_id`
- 返回内容包括：指标值、单位、状态、质量评分、公式追踪、SQL、分解明细

### 1.2 数据质量查询
- 质量汇总：`/api/quality/summary`
- 质量明细分页：`/api/quality/list`
- 异常问题分页：`/api/quality/issues`
- 设备质量趋势：`/api/quality/equipment/{equipment_id}/trend`
- 离线报告读取：`/api/quality/raw-report`（读取 `docs/data_quality_deep_report.csv`）

### 1.3 前端页面

| 路由 | 页面 | 说明 |
|------|------|------|
| `/` | Dashboard 总览 | 质量概览卡片 + 指标分类卡片 |
| `/quality` | 数据质量 | 4 个 Tab：质量汇总、质量明细、异常问题、原始报告 |
| `/metrics` | 指标分析 | 选择时间范围计算指标，展示结果、公式追溯、质量问题、分解明细 |
| `/quality`（设备列表 Tab） | 设备管理 | 设备质量列表，支持按类型/等级/粒度筛选 |
| `/quality/equipment/:equipmentId` | 设备详情 | 单设备质量趋势 |

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
src/carbon_metrics/
├── README.md
├── API 使用教程.md
├── logs/
│   └── metric_calculations.log
├── backend/
│   ├── main.py                 # FastAPI 入口
│   ├── config.py               # 环境变量配置
│   ├── db.py                   # DB 连接与游标上下文
│   ├── models.py               # Pydantic 模型
│   ├── routers/
│   │   ├── metrics.py          # 指标 API
│   │   └── quality.py          # 质量 API
│   ├── services/
│   │   ├── metric_calculator.py
│   │   └── quality_service.py
│   └── metrics/
│       ├── base.py             # 指标基类 + CalculationResult
│       ├── energy.py           # 4 个能耗指标
│       ├── temperature.py      # 5 个温度指标
│       ├── flow.py             # 3 个流量指标
│       ├── chiller.py          # 3 个冷机指标
│       ├── pump.py             # 2 个水泵指标
│       ├── tower.py            # 2 个冷却塔指标
│       ├── stability.py        # 2 个稳定性指标
│       └── maintenance.py      # 1 个维护指标
└── frontend/
    ├── package.json
    ├── vite.config.ts           # Vite 配置 + /api 代理
    ├── tsconfig.json
    └── src/
        ├── main.tsx             # 入口
        ├── App.tsx              # 路由定义
        ├── api/                 # Axios client + API 调用 + 类型
        ├── hooks/               # React Query hooks
        ├── constants/           # 设备类型、指标分类、质量等级映射
        ├── layouts/             # AppLayout (Sider + Header + Content)
        ├── components/          # 共享组件 (8 个)
        └── pages/
            ├── Dashboard/       # 总览页
            ├── Quality/         # 数据质量页 (4 Tab)
            ├── MetricDetail/    # 指标详情页
            └── Equipment/       # 设备管理页
```

---

## 4. 数据依赖与表说明

### 4.1 核心事实表
服务当前直接查询 `agg_hour`，主要使用字段包括：
- 维度：`bucket_time`、`building_id`、`system_id`、`equipment_type`、`equipment_id`、`sub_equipment_id`、`metric_name`
- 聚合值：`agg_avg`、`agg_min`、`agg_max`、`agg_delta`
- 质量标记：`quality_flags`

### 4.2 质量聚合表
质量接口依赖：
- `agg_hour_quality`
- `agg_day_quality`

可参考建表脚本：
- `norm/create_sql/database_v2_2.sql`

### 4.3 设备主数据表
设备列表/趋势相关依赖：
- `equipment_registry`

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

说明：
- `DB_PORT` 非法时会回退到 `3306`。
- 超时变量未配置时保持默认连接行为。
- `METRIC_CALC_WORKERS=1` 时会走串行计算并启用共享查询缓存；>1 时走并行计算。

---

## 6. 安装与启动

### 6.1 后端依赖

```powershell
pip install -r carbon_metrics/requirements.txt
```

如果你当前就在 `carbon_metrics/` 目录内，可改用：

```powershell
pip install -r requirements.txt
```

### 6.2 前端依赖

```powershell
cd carbon_metrics/frontend
npm install
```

### 6.3 启动后端

推荐在包含 `carbon_metrics/` 目录的仓库根目录启动：

```powershell
uvicorn carbon_metrics.backend.main:app --reload
```

如果你当前就在 `carbon_metrics/` 目录内，可改用：

```powershell
uvicorn backend.main:app --reload
```

启动后访问：
- Swagger: `http://127.0.0.1:8000/docs`
- 健康检查: `http://127.0.0.1:8000/health`

### 6.4 启动前端

```powershell
cd carbon_metrics/frontend
npm run dev
```

启动后访问：
- 前端页面: `http://127.0.0.1:5173`
- Vite 自动将 `/api` 请求代理到 `http://localhost:8000`

### 6.5 构建前端

```powershell
cd carbon_metrics/frontend
npm run build
```

### 6.6 局域网访问（不暴露公网）

后端：

```powershell
uvicorn carbon_metrics.backend.main:app --reload --host 0.0.0.0 --port 8000
```

前端（已在 `vite.config.ts` 配置 `host: '0.0.0.0'`）：

```powershell
cd carbon_metrics/frontend
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

返回说明（重点）：
- `quality_issues` 中的 `missing_dependency` 会带 `details.missing_metric_diagnostics`
- `missing_metric_diagnostics` 可区分：
  - 库里有数据但被筛选掉（scope 过滤）
  - canonical 有数据但 agg 无数据
  - raw+mapping 有数据但 canonical 无数据
  - mapping 未命中（会附 `unmapped_tag_samples` Top 样本）

`/api/metrics/calculate_batch` 请求体参数：
- `metric_names`（可选，不传则批量计算全部已注册指标）
- `time_start` / `time_end`（必填）
- `building_id` / `system_id` / `equipment_type` / `equipment_id` / `sub_equipment_id`（可选）

`/api/metrics/coverage` 返回重点字段：
- `summary`：可计算率与状态分布
- `items[].input_records` / `items[].valid_records`：每个指标的取数样本
- `items[].issue_types`：指标命中的质量问题类型
- `missing_dependency_counts`：缺失依赖频次（用于前端 Top 风险提示）

### 7.2 质量 API
- `GET /api/quality/summary`
- `GET /api/quality/list`
- `GET /api/quality/issues`
- `GET /api/quality/equipment/{equipment_id}/trend`
- `GET /api/quality/raw-report`

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
- `scanned_rows`
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

### 11.3 Phase B 只读联调脚本

```powershell
python src/carbon_metrics/backend/tools/readonly_metric_integration_report.py --time-start 2025-07-01T00:00:00 --time-end 2026-01-21T00:00:00
```

脚本固定输出四段：
- `指标状态分布`
- `缺失依赖矩阵`
- `指标取数审计`
- `可疑指标清单`

### 11.4 补数后库表对齐校验（只读）

```powershell
python norm/create_sql/validate_backfill_alignment.py --energy-dir data1 --params-dir data1 --output-json docs/backfill_alignment_report.json
```

脚本输出：
- `backfill_alignment_summary`：按 `tag/device` 的本地预期行数 vs DB 行数
- `backfill_alignment_mismatch_top`：文件级差异
- `backfill_alignment_warnings`：读取异常文件

如需在回归中“发现不一致即失败”，可增加：
```powershell
python norm/create_sql/validate_backfill_alignment.py --energy-dir data1 --params-dir data1 --fail-on-mismatch
```

### 11.5 一体化抽查（validate_data.py）

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
