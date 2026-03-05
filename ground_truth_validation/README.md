# Ground Truth 验证系统

从本地 Excel 数据验证后端指标计算的正确性。

## 功能说明

本系统从 `D:\Github\carbon_metrics\data1` 读取 Excel 原始数据，在本地重新计算 27 个指标，并与后端 API 返回结果对比，验证数据库和后端计算逻辑是否正确。

**这是验证后端正确性的唯一方式。**

## 环境准备

### 1. 安装依赖

```bash
pip install polars openpyxl requests pyyaml jupyter
```

**依赖说明：**
- `polars`: 高性能数据处理库
- `openpyxl`: Excel 文件读取引擎
- `requests`: HTTP 客户端，调用后端 API
- `pyyaml`: 读取已知差异配置文件
- `jupyter`: 运行验证 notebook

### 2. 启动后端服务

验证前必须先启动后端 API：

```bash
cd D:\Github\carbon_metrics\src
uvicorn carbon_metrics.backend.main:app --reload
```

确认后端可访问：`http://127.0.0.1:8000/health`

## 使用方法

### 方式一：Jupyter Notebook（推荐）

1. 打开 notebook：
```bash
cd D:\Github\carbon_metrics\ground_truth_validation
jupyter notebook validation.ipynb
```

2. 按顺序执行所有单元格

3. 查看生成的报告：`validation_report.md`

## 核心组件说明

### 1. config.py - 配置模块
定义数据路径、时间范围、容差阈值，列出全部 27 个待验证指标。

### 2. excel_reader.py - Excel 读取器
读取 Format A 格式 Excel（标签/采集时间/采集值/单位），自动解析标签名称。

### 3. calculators/ - 指标计算器
8 个计算器模块，覆盖 27 个指标，复制后端计算逻辑。

### 4. orchestrator.py - 计算编排器
注册全部 27 个指标，根据指标名称路由到对应计算器。

### 5. backend_client.py - 后端 API 客户端
调用后端 `/api/metrics/calculate` 接口获取后端计算结果。

### 6. comparison.py - 对比引擎
比较 Ground Truth 和后端结果，支持 epsilon 容差（默认 0.1%）。

### 7. report.py - 报告生成器
生成 Markdown 格式验证报告，包含汇总统计和详细对比表格。

## 配置说明

编辑 `config.py` 修改验证参数：

```python
DATA_DIR = Path(r"D:\Github\carbon_metrics\data1")
BUILDING_ID = "G11"
SYSTEM_ID = "1"
CALIBRATION_MONTH = "2025-07"
EPSILON_TOLERANCE = 0.001  # 0.1%
BACKEND_URL = "http://127.0.0.1:8000"
```

## 验证流程

1. **读取 Excel 数据** → 扫描 data1/ 目录，按指标和时间过滤
2. **本地计算指标** → 调用对应计算器，复制后端逻辑
3. **调用后端 API** → 获取后端计算结果
4. **对比结果** → 比较两者差异，判断是否在容差范围内
5. **生成报告** → 输出 Markdown 报告

## 故障排查

- **后端 API 无法访问**：确认后端已启动，访问 `http://127.0.0.1:8000/health`
- **Excel 文件读取失败**：检查 `DATA_DIR` 路径是否正确
- **指标计算返回 no_data**：检查时间范围内是否有数据
- **所有指标都不匹配**：检查 `BUILDING_ID` 和 `SYSTEM_ID` 是否与后端一致
