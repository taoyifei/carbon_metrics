# 鲁棒性与准确性修复计划

## TL;DR

> **目标**: 修复项目审计中发现的 5 个活跃问题 + 1 个配置调优
> 
> **交付物**:
> - 后端 stability.py: 运行时长占比分母改为实际数据小时数
> - 后端 energy.py + stability.py: clamp_threshold 改用 MySQL 会话变量
> - 后端 chiller.py / flow.py / temperature.py / tower.py: JOIN 类指标增加 minimum_calculable_principle 质量信息
> - 前端 index.tsx + useMetrics.ts: visibleCategories 空数据 UX + split 模式缓存优化
> - 后端 routers/metrics.py + db.py: data_version 缓存 + 连接池扩容
> 
> **预估工作量**: Medium（5 个任务，约 3-5 小时）
> **并行执行**: YES — 2 波
> **关键路径**: Task 1 → Task 2

---

## Context

### 原始需求
用户在全面项目审计后，要求修复 8 个鲁棒性/准确性问题中的 5 个活跃项（3 个需要 Excel 调查的已推迟）。核心约束："展示真实的数据和最小可算原则"、"在不影响结果真实准确的情况下"优化性能。

### 审计结论
- 23 个指标计算公式全部正确 ✅
- Pipeline 数据导入未改变原始数据属性 ✅
- 2.14修复计划 v2 全部 5 项已完成 ✅
- 最小可算原则在能耗类指标中正确实现 ✅

### 推迟项（需要 Excel 调查）
- 水泵 equipment_id 前缀一致性
- agg_hour NULL unique key
- 冷却水温度默认映射

---

## Work Objectives

### 核心目标
修复运行时长占比分母假设、参数安全性、前端 UX、性能瓶颈，并确保所有 JOIN 类指标在前端展示交集时间范围。

### Must Have
- 运行时长占比使用实际数据小时数作为分母
- clamp_threshold 参数不再依赖位置对齐
- JOIN 类指标输出 `minimum_calculable_principle` 质量信息
- 前端 coverage 全部 no_data 时显示明确提示
- split 模式缓存 key 与单独查询统一

### Must NOT Have（护栏）
- 不得改变任何指标的计算公式逻辑
- 不得修改 pipeline 模块
- 不得引入新的 npm/pip 依赖
- 不得改变 API 接口签名（只增加返回字段）
- 不得在性能优化中牺牲数据准确性

---

## Verification Strategy

- **Test Infrastructure**: NO（项目无测试框架）
- **Automated tests**: None
- **Agent-Executed QA**: MANDATORY — 每个任务都有 curl 验证场景

---

## Execution Strategy

```
Wave 1 (Start Immediately):
├── Task 1: clamp_threshold 参数安全 (energy.py + stability.py)
├── Task 3: JOIN 类指标交集时间显示 (chiller.py, flow.py, temperature.py, tower.py)
├── Task 4: 前端 UX + 性能优化 (index.tsx, useMetrics.ts)
└── Task 5: 后端性能 + 连接池 (routers/metrics.py, db.py)

Wave 2 (After Wave 1):
└── Task 2: 运行时长占比分母修复 (stability.py) — depends on Task 1
```

| Task | Depends On | Blocks | Can Parallelize With |
|------|------------|--------|---------------------|
| 1 | None | 2 | 3, 4, 5 |
| 2 | 1 | None | None |
| 3 | None | None | 1, 4, 5 |
| 4 | None | None | 1, 3, 5 |
| 5 | None | None | 1, 3, 4 |

---

## TODOs

---

### - [ ] 1. clamp_threshold 参数安全（MySQL 会话变量）

**What to do**:

当前 `energy.py` L126-131 和 `stability.py` L136-142 中，`clamp_threshold` 通过位置参数传入 SQL：
```python
query_params: List[Any] = [
    clamp_threshold, clamp_threshold, clamp_threshold, clamp_threshold,
    *params,
]
```
如果 SQL 模板中 CASE WHEN 数量变化，参数会静默错位。

**修复方案**: 在执行主查询前，先 `cursor.execute("SET @ndc_threshold = %s", [clamp_threshold])`，然后 SQL 中用 `@ndc_threshold` 替换所有 `%s`。

**具体步骤**:
1. `energy.py` — `_query_energy_by_bucket_type()` (L70-147): 主查询前 SET 会话变量，SQL 中 `-%s` → `-@ndc_threshold`，`query_params` 只保留 `*params`
2. `stability.py` — `_RuntimeRatioMetric.calculate()` (L87-284): 同样模式

**Must NOT do**:
- 不要用 f-string 拼接 threshold 到 SQL
- 不要改变 threshold 默认值或计算逻辑

**Recommended Agent Profile**:
- **Category**: `quick`
- **Skills**: `[]`

**Parallelization**: Wave 1 | Blocks: Task 2 | Blocked By: None

**References**:
- `carbon_metrics/backend/metrics/energy.py:70-147` — `_query_energy_by_bucket_type()` 当前参数拼接
- `carbon_metrics/backend/metrics/stability.py:87-142` — 同样模式
- `carbon_metrics/backend/db.py:103-112` — cursor 上下文管理器，同一 cursor 内连接不变

**Acceptance Criteria**:

```
Scenario: 能耗指标计算结果不变
  Tool: Bash (curl)
  Steps:
    1. curl -s "http://localhost:8000/api/metrics/calculate?metric_name=系统总电量&time_start=2025-07-01T00:00:00&time_end=2025-07-02T00:00:00"
    2. 对比修改前后 value 完全一致
  Expected Result: 数值不变

Scenario: 运行时长指标计算结果不变
  Tool: Bash (curl)
  Steps:
    1. curl -s "http://localhost:8000/api/metrics/calculate?metric_name=冷机运行时长占比&time_start=2025-07-01T00:00:00&time_end=2025-08-01T00:00:00"
    2. 对比修改前后 value 完全一致

Scenario: Python 语法检查
  Tool: Bash
  Steps:
    1. python -m py_compile carbon_metrics/backend/metrics/energy.py
    2. python -m py_compile carbon_metrics/backend/metrics/stability.py
  Expected Result: 无输出（编译成功）
```

**Commit**: `fix(metrics): use MySQL session variable for clamp_threshold to prevent parameter misalignment`
**Files**: `energy.py`, `stability.py`

---

### - [ ] 2. 运行时长占比分母修复（最小可算原则）

**What to do**:

当前 `stability.py` L166-173:
```python
total_runtime = float(selected_row["total_runtime"] or 0)
device_count = max(1, int(selected_row["device_count"] or 0))
delta = ctx.time_end - ctx.time_start
period_hours = max(0.0, delta.total_seconds() / 3600)
max_runtime = period_hours * device_count  # ← 假设所有设备应运行整个周期
```

**问题**: `period_hours × device_count` 假设所有设备在整个评估周期内都应运行。实际上某些设备可能只有部分时段有数据，违反最小可算原则。

**修复方案**: 分母改为 `record_count`（实际有数据的设备·小时数）。每条 `agg_hour` 记录 = 1 台设备 × 1 小时观测，最大可能运行时长 = 1 小时。

**具体步骤**:

1. 修改分母 (L166-173):
```python
total_runtime = float(selected_row["total_runtime"] or 0)
record_count = int(selected_row["record_count"] or 0)
device_count = max(1, int(selected_row["device_count"] or 0))
max_runtime = float(record_count)  # 最小可算：实际有数据的设备·小时
ratio = 0.0 if max_runtime == 0 else round(total_runtime / max_runtime * 100, 2)
```

2. 更新 formula_with_values (L250-254):
```python
formula_with_values = (
    f"= {round(total_runtime, 1)}h / {record_count}设备·小时"
    f" ({device_count}台设备, 实际覆盖) = {ratio}%"
)
```

3. 添加 minimum_calculable_principle 质量信息 (在 L183 calc_issues 区域):
```python
delta = ctx.time_end - ctx.time_start
period_hours = max(0.0, delta.total_seconds() / 3600)
theoretical_max = period_hours * device_count
calc_issues.append({
    "type": "minimum_calculable_principle",
    "description": (
        f"运行时长占比基于实际有数据的 {record_count} 设备·小时计算"
        f"（理论最大 {round(theoretical_max, 1)} = {round(period_hours, 1)}h × {device_count}台）"
    ),
    "details": {
        "actual_device_hours": record_count,
        "theoretical_device_hours": round(theoretical_max, 1),
        "device_count": device_count,
        "period_hours": round(period_hours, 1),
        "coverage_rate": round(record_count / theoretical_max * 100, 1) if theoretical_max > 0 else 0,
    },
})
```

**Must NOT do**:
- 不要改变 SQL 查询（SUM/COUNT 逻辑不变）
- 不要改变负值处理逻辑
- 不要改变 `_metric_candidates` 回退逻辑

**Recommended Agent Profile**:
- **Category**: `unspecified-high`
- **Skills**: `[]`

**Parallelization**: Wave 2 | Blocks: None | Blocked By: Task 1

**References**:
- `carbon_metrics/backend/metrics/stability.py:87-284` — 完整 calculate() 方法
- `carbon_metrics/backend/metrics/stability.py:166-173` — 当前分母计算（修改核心）
- `carbon_metrics/backend/metrics/stability.py:250-254` — formula_with_values（需更新）
- `carbon_metrics/backend/metrics/energy.py:718-726` — TotalEnergyMetric 的 minimum_calculable_principle 格式（参考模式）
- `carbon_metrics/frontend/src/pages/MetricDetail/MetricResultCard.tsx:33-63` — 前端渲染交集标签

**Acceptance Criteria**:

```
Scenario: 分母使用实际数据小时数
  Tool: Bash (curl)
  Steps:
    1. curl -s "http://localhost:8000/api/metrics/calculate?metric_name=冷机运行时长占比&time_start=2025-07-01T00:00:00&time_end=2025-08-01T00:00:00" | python -m json.tool
    2. formula_with_values 包含 "设备·小时" 而非 "h x N台"
    3. quality_issues 包含 type="minimum_calculable_principle"
    4. details 包含 actual_device_hours, theoretical_device_hours, coverage_rate

Scenario: 风机运行时长占比同样修复
  Tool: Bash (curl)
  Steps:
    1. curl -s "http://localhost:8000/api/metrics/calculate?metric_name=风机运行时长占比&time_start=2025-07-01T00:00:00&time_end=2025-08-01T00:00:00" | python -m json.tool
    2. 同样检查 formula_with_values 和 quality_issues
```

**Commit**: `fix(stability): use actual data hours as denominator for runtime ratio (minimum calculable principle)`
**Files**: `stability.py`

---

### - [ ] 3. JOIN 类指标添加交集时间显示

**What to do**:

4 个使用 CTE + JOIN 的指标通过 INNER JOIN 隐式实现时间交集，但不输出 `minimum_calculable_principle` 质量信息。用户需要在前端看到"这些结果是用什么时间之内的数据计算的"。

**需要修改的指标**:

1. **ChillerCopMetric** (`chiller.py` L536 区域): 已有 `overlapped_hours` (L517)，在 calc_issues 中添加
2. **CoolingCapacityMetric** (`flow.py` L249 区域): 已有 `overlapped_hours` (L245)
3. **ChilledWaterDeltaTMetric** (`temperature.py` L212 区域): 已有 `overlapped_hours` (L210)
4. **CoolingWaterDeltaTMetric** (`tower.py` L73 区域): 已有 `overlapped_hours` (L71)

**每个指标添加的代码模式**（在成功路径、构造 CalculationResult 前）:

```python
intersection_issue = {
    "type": "minimum_calculable_principle",
    "description": f"基于 {overlapped_hours} 小时交集计算（各组件按 bucket_time 对齐）",
    "details": {
        "overlapped_hours": overlapped_hours,
        "components": [<该指标依赖的 metric_name 列表>],
        "join_key": "bucket_time",
    },
}
quality_issues = quality_issues + [intersection_issue]
```

各指标的 components:
- COP: `["chilled_flow", "chilled_return_temp", "chilled_supply_temp", "power"]`
- 制冷量: `["chilled_flow", "chilled_return_temp", "chilled_supply_temp"]`
- 冷冻水温差: `["chilled_return_temp", "chilled_supply_temp"]`
- 冷却水温差: `["cooling_return_temp", "cooling_supply_temp"]`

**Must NOT do**:
- 不要改变 JOIN 逻辑或计算公式
- 不要增加额外 DB 查询（用已有 overlapped_hours）
- 不要修改 formula_with_values

**Recommended Agent Profile**:
- **Category**: `quick`
- **Skills**: `[]`

**Parallelization**: Wave 1 | Blocks: None | Blocked By: None

**References**:
- `carbon_metrics/backend/metrics/energy.py:718-726` — TotalEnergyMetric 的 minimum_calculable_principle 格式（**标准模式**）
- `carbon_metrics/backend/metrics/chiller.py:510-555` — COP overlapped_hours 和 calc_issues 区域
- `carbon_metrics/backend/metrics/flow.py:240-276` — CoolingCapacity 成功路径
- `carbon_metrics/backend/metrics/temperature.py:207-234` — ChilledWaterDeltaT 成功路径
- `carbon_metrics/backend/metrics/tower.py:68-95` — CoolingWaterDeltaT 成功路径
- `carbon_metrics/frontend/src/pages/MetricDetail/MetricResultCard.tsx:33-63` — 前端已能渲染此标签
- `carbon_metrics/frontend/src/pages/MetricDetail/QualityIssuesPanel.tsx:360-396` — 前端已能渲染交集详情

**Acceptance Criteria**:

```
Scenario: COP 返回 minimum_calculable_principle
  Tool: Bash (curl)
  Steps:
    1. curl -s "http://localhost:8000/api/metrics/calculate?metric_name=冷机COP&time_start=2025-07-01T00:00:00&time_end=2025-08-01T00:00:00" | python -m json.tool
    2. quality_issues 中存在 type="minimum_calculable_principle"
    3. details.components 包含 4 个组件

Scenario: 制冷量返回交集信息
  Tool: Bash (curl)
  Steps:
    1. curl -s "http://localhost:8000/api/metrics/calculate?metric_name=制冷量&time_start=2025-07-01T00:00:00&time_end=2025-08-01T00:00:00" | python -m json.tool
    2. quality_issues 中有 minimum_calculable_principle

Scenario: 冷冻水温差 + 冷却水温差返回交集信息
  Tool: Bash (curl)
  Steps:
    1. 分别请求两个指标，检查 quality_issues

Scenario: 计算结果数值不变
  Tool: Bash (curl)
  Steps:
    1. 对比修改前后 4 个指标的 value 值完全一致
```

**Commit**: `feat(metrics): emit minimum_calculable_principle for JOIN-based metrics (COP, cooling capacity, delta-T)`
**Files**: `chiller.py`, `flow.py`, `temperature.py`, `tower.py`

---

### - [ ] 4. 前端 UX + 性能优化

**What to do**:

**4A. visibleCategories 空数据提示** (`index.tsx` L141-157):

当前所有指标 no_data 时回退到显示所有分类，用户看到一堆分类但点进去全是空的。

修复：保持回退逻辑（避免空白页面），添加全局 Alert 提示。

1. 添加 `allNoData` 计算变量:
```typescript
const allNoData = useMemo(() => {
  const items = metricCoverageQuery.data?.items ?? [];
  if (items.length === 0) return false;
  return items.every((item) => item.status === 'no_data' || item.status === 'failed');
}, [metricCoverageQuery.data]);
```

2. 在 L316 `<Card size="small">` 之前添加 Alert:
```tsx
{allNoData && !metricCoverageQuery.isLoading && (
  <Alert
    type="warning"
    showIcon
    message="当前条件下所有指标均无可用数据"
    description="请调整时间范围或设备筛选条件。下方分类仅供参考，点击后可查看具体缺失原因。"
    style={{ marginBottom: 16 }}
  />
)}
```

**4B. Split 模式缓存 key 统一** (`useMetrics.ts` L36-59):

当前 split 模式 queryKey (L49): `['metrics', 'calculate', metric_name, baseFilters, scope]`
单独调用 queryKey (L28): `['metrics', 'calculate', metric_name, filters]`

key 结构不同，从 "all" 切换到 "main" 时无法命中缓存。

修复：统一 key 格式，让 split 子查询与单独查询使用相同 key:
```typescript
// useMetricCalculateBySubScopes 中每个子查询的 key 改为:
queryKey: ['metrics', 'calculate', metric_name, { ...baseFilters, sub_equipment_id: scope }],
```
这样与 `useMetricCalculate` 的 key 格式一致（filters 对象包含 sub_equipment_id）。

**Must NOT do**:
- 不要改变 API 调用逻辑
- 不要移除现有的 staleTime 配置
- 不要改变路由参数结构

**Recommended Agent Profile**:
- **Category**: `visual-engineering`
- **Skills**: `["frontend-ui-ux"]`

**Parallelization**: Wave 1 | Blocks: None | Blocked By: None

**References**:
- `carbon_metrics/frontend/src/pages/Metrics/index.tsx:141-157` — visibleCategories 当前逻辑
- `carbon_metrics/frontend/src/pages/Metrics/index.tsx:316` — 指标选择 Card 位置（Alert 插入点）
- `carbon_metrics/frontend/src/hooks/useMetrics.ts:22-34` — useMetricCalculate queryKey
- `carbon_metrics/frontend/src/hooks/useMetrics.ts:36-59` — useMetricCalculateBySubScopes queryKey
- `carbon_metrics/frontend/src/main.tsx:13-20` — QueryClient 全局配置（已有 refetchOnWindowFocus: false）

**Acceptance Criteria**:

```
Scenario: 全部 no_data 时显示警告 Banner
  Tool: Playwright
  Preconditions: 前后端运行，选择一个无数据的时间范围
  Steps:
    1. Navigate to http://localhost:5173/metrics
    2. 设置时间范围为无数据区间（如 2024-01-01 ~ 2024-01-02）
    3. 等待 coverage 加载完成
    4. Assert: 页面出现 "当前条件下所有指标均无可用数据" 警告
    5. Assert: 分类菜单仍然可见可点击
  Evidence: .sisyphus/evidence/task-4-no-data-banner.png

Scenario: 有数据时不显示警告
  Tool: Playwright
  Steps:
    1. Navigate to http://localhost:5173/metrics
    2. 使用默认时间范围（有数据）
    3. Assert: 不存在 "所有指标均无可用数据" 警告
  Evidence: .sisyphus/evidence/task-4-normal-state.png

Scenario: 前端构建通过
  Tool: Bash
  Steps:
    1. cd carbon_metrics/frontend && npm run build
  Expected Result: 构建成功无错误
```

**Commit**: `fix(frontend): add no-data banner for empty coverage + unify split-mode cache keys`
**Files**: `index.tsx`, `useMetrics.ts`

---

### - [ ] 5. 后端性能 + 连接池调优

**What to do**:

**5A. data_version 查询缓存** (`routers/metrics.py`):

当前每次 `/calculate` 请求都执行 `_load_data_version()` (L73-93) 查询 DB 获取版本信息，即使缓存命中也要先查。

修复：缓存 data_version 结果几秒钟，避免重复查询:

```python
_DATA_VERSION_CACHE_LOCK = threading.Lock()
_data_version_cache: Optional[Tuple[float, Tuple[Any, ...]]] = None  # (expire_at, version)
_DATA_VERSION_CACHE_TTL = 3.0  # 秒

def _load_data_version(calculator: MetricCalculator) -> Tuple[Any, ...] | None:
    global _data_version_cache
    now = time.time()
    with _DATA_VERSION_CACHE_LOCK:
        if _data_version_cache and _data_version_cache[0] > now:
            return _data_version_cache[1]
    # cache miss — query DB
    version = _load_data_version_from_db(calculator)
    if version is not None:
        with _DATA_VERSION_CACHE_LOCK:
            _data_version_cache = (now + _DATA_VERSION_CACHE_TTL, version)
    return version
```

将原有查询逻辑提取为 `_load_data_version_from_db()`。

**5B. 连接池扩容** (`db.py` L22):

当前 `pool_size=4`，而 `METRIC_CALC_WORKERS` 默认也是 4。并发请求时可能耗尽连接池。

修复：将默认 pool_size 从 4 改为 8，并从环境变量读取:
```python
def __init__(self, config: DatabaseConfig = None, pool_size: int = None):
    self.config = config or get_db_config()
    if pool_size is None:
        pool_size = Database._parse_pool_size()
    self._pool: Queue[pymysql.Connection] = Queue(maxsize=pool_size)
    ...

@staticmethod
def _parse_pool_size() -> int:
    raw = os.getenv("DB_POOL_SIZE", "8").strip()
    try:
        val = int(raw)
        return max(2, min(val, 32))
    except (TypeError, ValueError):
        return 8
```

**Must NOT do**:
- 不要改变缓存的 key 结构或 TTL 语义
- 不要改变 data_version 的查询 SQL
- 不要移除现有的 `_CACHE_LOCK` 机制

**Recommended Agent Profile**:
- **Category**: `quick`
- **Skills**: `[]`

**Parallelization**: Wave 1 | Blocks: None | Blocked By: None

**References**:
- `carbon_metrics/backend/routers/metrics.py:73-93` — `_load_data_version()` 当前实现
- `carbon_metrics/backend/routers/metrics.py:96-123` — 缓存 get/set 机制
- `carbon_metrics/backend/db.py:19-27` — Database.__init__ pool_size
- `carbon_metrics/backend/db.py:68-82` — get_connection() 从池获取
- `carbon_metrics/backend/config.py` — 环境变量读取模式参考

**Acceptance Criteria**:

```
Scenario: data_version 缓存减少 DB 查询
  Tool: Bash (curl)
  Steps:
    1. 快速连续发送 3 次相同请求:
       for i in 1 2 3; do curl -s -o /dev/null -w "%{time_total}\n" "http://localhost:8000/api/metrics/calculate?metric_name=系统总电量&time_start=2025-07-01T00:00:00&time_end=2025-07-02T00:00:00"; done
    2. 第 2、3 次应明显快于第 1 次（data_version 缓存命中）
  Expected Result: 后续请求响应时间降低

Scenario: 连接池扩容生效
  Tool: Bash
  Steps:
    1. 启动后端，检查日志无连接池耗尽警告
    2. python -c "from carbon_metrics.backend.db import get_db; db=get_db(); print(db._pool.maxsize)"
  Expected Result: 输出 8（或环境变量指定值）

Scenario: Python 语法检查
  Tool: Bash
  Steps:
    1. python -m py_compile carbon_metrics/backend/routers/metrics.py
    2. python -m py_compile carbon_metrics/backend/db.py
  Expected Result: 编译成功
```

**Commit**: `perf(backend): cache data_version query + increase default pool_size to 8`
**Files**: `routers/metrics.py`, `db.py`

---

## Commit Strategy

| After Task | Message | Files | Verification |
|------------|---------|-------|--------------|
| 1 | `fix(metrics): use MySQL session variable for clamp_threshold` | energy.py, stability.py | curl 对比结果不变 |
| 2 | `fix(stability): use actual data hours as runtime ratio denominator` | stability.py | curl 检查 formula + quality_issues |
| 3 | `feat(metrics): emit minimum_calculable_principle for JOIN-based metrics` | chiller.py, flow.py, temperature.py, tower.py | curl 检查 quality_issues |
| 4 | `fix(frontend): add no-data banner + unify split-mode cache keys` | index.tsx, useMetrics.ts | npm run build + Playwright |
| 5 | `perf(backend): cache data_version + increase pool_size` | metrics.py, db.py | curl 响应时间对比 |

---

## Success Criteria

### Final Checklist
- [ ] 所有 5 个指标计算结果数值与修改前一致（Task 1, 3 不改变计算）
- [ ] 运行时长占比分母使用实际数据小时数（Task 2）
- [ ] 4 个 JOIN 类指标返回 minimum_calculable_principle（Task 3）
- [ ] 前端全部 no_data 时显示警告 Banner（Task 4）
- [ ] 前端 npm run build 通过（Task 4）
- [ ] 后端 py_compile 全部通过（Task 1, 2, 3, 5）
- [ ] 连续请求响应时间有改善（Task 5）
