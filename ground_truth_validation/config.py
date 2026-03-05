"""Ground Truth 验证系统配置文件。"""

from pathlib import Path
from typing import Literal

# 项目路径
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data1"
VALIDATION_DIR = PROJECT_ROOT / "ground_truth_validation"

# 后端 API 地址
BACKEND_URL = "http://127.0.0.1:8000"

# 验证设置
BUILDING_ID = None  # 数据库中无此字段，不筛选
SYSTEM_ID = None  # 数据库中无此字段，不筛选
CALIBRATION_MONTH = "2025-07"  # 数据质量最佳月份
EPSILON_TOLERANCE = 0.001  # 浮点数比较容差 0.1%

# 数据可用性（来自研究）
DATA_RANGE = {
    "G11-1": ("2025-07-01", "2026-01-20"),
    "G11-2": ("2025-07-01", "2025-10-08"),
    "G11-3": ("2025-07-01", "2026-01-20"),
    "G12-1": ("2025-07-01", "2026-01-20"),
    "G12-2": ("2025-07-01", "2026-01-20"),
    "G12-3": ("2025-07-01", "2026-01-20"),
}

# 指标分类（共 27 个）
METRIC_CATEGORIES = {
    "energy": ["系统总电量", "冷机能耗占比", "水泵能耗占比", "风机能耗占比"],
    "temperature": ["冷冻水供水温度", "冷冻水回水温度", "冷却水供水温度", "冷却水回水温度", "冷冻水温差"],
    "flow": ["冷冻水流量", "冷却水流量", "制冷量"],
    "chiller": ["冷机平均负载率", "冷机最大负载率", "冷机负载波动系数", "冷机COP", "制冷系统COP"],
    "pump": ["冷冻泵工作频率", "冷却泵工作频率", "冷冻泵能耗密度", "冷却泵能耗密度"],
    "tower": ["冷却水温差", "冷却塔风机功率", "冷却塔效率"],
    "stability": ["冷机运行时长占比", "风机运行时长占比"],
    "maintenance": ["过载风险指数"],
}

ALL_METRICS = [m for metrics in METRIC_CATEGORIES.values() for m in metrics]
