from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Iterable, cast

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]

DATA_DIRS = {
    "pump_component": Path("冷源设备相关数据反馈20260120") / "冷冻水泵冷却水泵",
    "tower_component": Path("冷源设备相关数据反馈20260120") / "冷却塔",
    "chiller_component": Path("冷源设备相关数据反馈20260120") / "制冷主机",
    "tower_energy": Path("冷塔功率和电量"),
    "pump_energy": Path("水泵功率和电量"),
    "chiller_energy": Path("冷源设备相关数据反馈20260120") / "制冷主机" / "冷机功率和电量",
}

METRIC_CATEGORY_RULES = [
    ("回水温度", ["上塔", "进塔"]),
    ("供水温度", ["下塔", "出塔"]),
    ("供水温度", ["供水"]),
    ("回水温度", ["回水", "进水"]),
    ("功率", ["功率", "公共功率"]),
    ("电量", ["电量", "公共电能"]),
    ("频率", ["频率", "变频"]),
    ("运行时间", ["运行时间", "累计运行"]),
    ("运行状态", ["运行状态"]),
    ("流量", ["流量"]),
    ("负载率", ["负载", "电流百分比"]),
    ("温度", ["温度"]),
]


def resolve_path(rel_path: Path) -> Path:
    return BASE_DIR / rel_path


def list_excel_files(rel_path: Path) -> list[Path]:
    abs_path = resolve_path(rel_path)
    if not abs_path.exists():
        return []
    return sorted(abs_path.rglob("*.xlsx"))


def normalize_column(col: str) -> str:
    return str(col).strip()


def pick_column(columns: Iterable[str], keywords: list[str]) -> str | None:
    for col in columns:
        for kw in keywords:
            if kw in col:
                return col
    return None


def extract_component_id(text: str) -> str:
    """
    [修复] 改进设备ID提取逻辑，支持多种文件名格式：
    
    格式1: G11-1冷机1# -> G11-1-冷机1#
    格式2: 11-1冷冻水供水 -> G11-1 (自动补 G 前缀)
    格式3: G11-1冷机功率和电量 (目录名) + G11-1冷机1#电量 -> G11-1-冷机1#
    
    关键改动：
    - 支持 11-1, 11-2, 12-1 等不带 G 前缀的格式
    - 统一转换为 G11-1, G11-2, G12-1 格式
    - 对于冷机，优先提取到具体机组号 (冷机1#, 冷机2# 等)
    """
    # 先尝试提取 G11-1, G12-3 等标准格式
    group_match = re.search(r"G(\d+)-(\d)", text)
    
    # 如果没有 G 前缀，尝试匹配 11-1, 12-3 等格式
    if not group_match:
        bare_match = re.search(r"(\d{2})-(\d)", text)
        if bare_match:
            # 自动补上 G 前缀
            group_id = f"G{bare_match.group(1)}-{bare_match.group(2)}"
        else:
            group_id = None
    else:
        group_id = group_match.group(0)

    if not group_id:
        g_match = re.search(r"G(\d)(\d)(\d)", text)
        if g_match:
            group_id = f"G{g_match.group(1)}{g_match.group(2)}-{g_match.group(3)}"
    
    # 尝试提取具体设备号
    device_patterns = [
        r"冷冻泵\d+#",
        r"冷却泵\d+#",
        r"冷塔\d+#",
        r"冷机\d+#",
        r"CT\d+",
    ]
    
    device_id = None
    for pat in device_patterns:
        match = re.search(pat, text)
        if match:
            device_id = match.group(0)
            break
    
    # 组合结果
    if group_id and device_id:
        return f"{group_id}-{device_id}"
    elif group_id:
        return group_id
    elif device_id:
        return device_id
    else:
        return "未知设备"


def normalize_component_id_for_matching(component_id: str) -> str:
    """
    [新增] 将设备ID标准化为统一的组ID，用于跨指标匹配
    
    例如：
    - G11-1-冷机1# -> G11-1
    - G11-1 -> G11-1
    - G12-3-冷机2# -> G12-3
    
    这样流量数据 (G11-1) 和功率数据 (G11-1-冷机1#) 就能关联
    """
    match = re.search(r"G\d+-\d", component_id)
    if match:
        return match.group(0)
    return component_id


def infer_component_type(file_path: Path) -> str:
    path_str = str(file_path)
    if "冷冻水泵冷却水泵" in path_str or "水泵功率和电量" in path_str:
        return "水泵"
    if "冷却塔" in path_str or "冷塔功率和电量" in path_str:
        return "冷却塔"
    if "制冷主机" in path_str or "冷机功率和电量" in path_str:
        return "制冷主机"
    return "未知"


def infer_metric_category(text: str, hint: str | None = None) -> str:
    combined = f"{text} {hint or ''}"
    text_only = text
    hint_text = hint or ""
    for category, rules in METRIC_CATEGORY_RULES:
        if any(rule in text_only for rule in rules):
            return category
    for category, rules in METRIC_CATEGORY_RULES:
        if any(rule in hint_text for rule in rules):
            return category
    return "其他"


def infer_power_role(file_path: Path) -> str | None:
    stem = file_path.stem
    if "主功率" in stem:
        return "主功率"
    if "备功率" in stem:
        return "备功率"
    if "主电量" in stem:
        return "主电量"
    if "备电量" in stem:
        return "备电量"
    return None


def is_energy_file(file_path: Path) -> bool:
    name = file_path.stem
    return "功率" in name or "电量" in name


def read_standard_sheet_optimized(file_path: Path) -> pd.DataFrame:
    try:
        xl = pd.ExcelFile(file_path, engine="openpyxl")
    except Exception:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for sheet in xl.sheet_names:
        try:
            df_header = pd.read_excel(xl, sheet_name=sheet, nrows=0, engine="openpyxl")
        except Exception:
            continue

        columns = [normalize_column(c) for c in df_header.columns]
        col_point = pick_column(columns, ["点名"])
        col_time = pick_column(columns, ["采集时间", "时间"])
        col_value = pick_column(columns, ["采集值", "值"])
        col_unit = pick_column(columns, ["单位"])

        if not (col_point and col_time and col_value):
            continue

        usecols = [c for c in [col_point, col_time, col_value, col_unit] if c]
        try:
            df = pd.read_excel(xl, sheet_name=sheet, usecols=usecols, engine="openpyxl")
        except Exception:
            continue
        df = pd.DataFrame(df)

        rename_map = {
            col_point: "point_name",
            col_time: "timestamp",
            col_value: "value",
        }
        if col_unit:
            rename_map[col_unit] = "unit"
        df = df.rename(columns=rename_map)
        if "unit" not in df.columns:
            df["unit"] = None

        df = cast(pd.DataFrame, df.dropna(subset=["timestamp", "value"]))
        if df.empty:
            continue

        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = cast(pd.DataFrame, df.dropna(subset=["timestamp", "value"]))
        if df.empty:
            continue

        df["source_file"] = str(file_path.relative_to(BASE_DIR))
        df["sheet"] = sheet
        frames.append(df)

    xl.close()
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def choose_energy_sheet(sheet_names: list[str]) -> str | None:
    priorities = ["数据表(真实值))", "折线图数据表", "数据表"]
    for name in priorities:
        if name in sheet_names:
            return name
    return sheet_names[0] if sheet_names else None


def read_energy_sheet_optimized(file_path: Path) -> pd.DataFrame:
    try:
        xl = pd.ExcelFile(file_path, engine="openpyxl")
    except Exception:
        return pd.DataFrame()

    sheet = choose_energy_sheet(xl.sheet_names)
    if not sheet:
        xl.close()
        return pd.DataFrame()

    try:
        df = pd.read_excel(xl, sheet_name=sheet, engine="openpyxl")
    except Exception:
        xl.close()
        return pd.DataFrame()
    df = pd.DataFrame(df)

    xl.close()

    columns = [normalize_column(c) for c in df.columns]
    col_time = pick_column(columns, ["时间"])
    if not col_time:
        return pd.DataFrame()

    value_candidates = [c for c in columns if c not in {col_time, "序号"}]
    if not value_candidates:
        return pd.DataFrame()
    col_value = value_candidates[-1]

    df = df.rename(columns={col_time: "timestamp", col_value: "value"})

    point_name = None
    meta_rows = df[df["timestamp"].isna()]
    if not meta_rows.empty:
        for val in meta_rows["value"].tolist():
            if isinstance(val, str) and val.strip():
                point_name = val.strip()
                break

    df = cast(pd.DataFrame, df[df["timestamp"].notna()])
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = cast(pd.DataFrame, df.dropna(subset=["timestamp", "value"]))
    if df.empty:
        return pd.DataFrame()

    df["point_name"] = point_name if point_name else col_value
    df["unit"] = None
    df["source_file"] = str(file_path.relative_to(BASE_DIR))
    df["sheet"] = sheet
    return df[["point_name", "timestamp", "value", "unit", "source_file", "sheet"]]


def attach_metadata(df: pd.DataFrame, file_path: Path) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    unique_points = df["point_name"].unique()
    point_metadata: dict[str, dict[str, str]] = {}

    for point_name in unique_points:
        text = f"{file_path.parent.name} {file_path.stem} {point_name}"
        hint = text
        point_metadata[str(point_name)] = {
            "component_type": infer_component_type(file_path),
            "component_id": extract_component_id(text),
            "metric_category": infer_metric_category(str(point_name), hint),
        }

    df["component_type"] = df["point_name"].map(
        lambda x: point_metadata.get(str(x), {}).get("component_type", "未知")
    )
    df["component_id"] = df["point_name"].map(
        lambda x: point_metadata.get(str(x), {}).get("component_id", "未知设备")
    )
    df["metric_category"] = df["point_name"].map(
        lambda x: point_metadata.get(str(x), {}).get("metric_category", "其他")
    )
    df["group_id"] = df["component_id"].apply(normalize_component_id_for_matching)
    df["metric_name"] = df["point_name"].fillna(file_path.stem)
    role = infer_power_role(file_path)
    if role:
        df["metric_name"] = df["metric_name"].apply(lambda name: f"{name}({role})")
    return df


def load_component_data() -> pd.DataFrame:
    files = (
        list_excel_files(DATA_DIRS["pump_component"])
        + list_excel_files(DATA_DIRS["tower_component"])
        + list_excel_files(DATA_DIRS["chiller_component"])
    )
    frames: list[pd.DataFrame] = []
    for file_path in files:
        df = read_standard_sheet_optimized(file_path)
        if df.empty and is_energy_file(file_path):
            df = read_energy_sheet_optimized(file_path)
        df = attach_metadata(df, file_path)
        if not df.empty:
            frames.append(df)
    return cast(pd.DataFrame, pd.concat(frames, ignore_index=True)) if frames else pd.DataFrame()


def load_energy_data() -> pd.DataFrame:
    files = (
        list_excel_files(DATA_DIRS["tower_energy"])
        + list_excel_files(DATA_DIRS["pump_energy"])
        + list_excel_files(DATA_DIRS["chiller_energy"])
    )
    frames: list[pd.DataFrame] = []
    for file_path in files:
        df = read_energy_sheet_optimized(file_path)
        df = attach_metadata(df, file_path)
        if not df.empty:
            frames.append(df)
    return cast(pd.DataFrame, pd.concat(frames, ignore_index=True)) if frames else pd.DataFrame()


def compute_anomaly_scores(series: pd.Series, window: int = 48) -> pd.DataFrame:
    rolling_mean = series.rolling(window, min_periods=8).mean()
    rolling_std = series.rolling(window, min_periods=8).std()
    z_scores = (series - rolling_mean) / rolling_std.replace(0, np.nan)
    diffs = series.diff()
    diff_std = diffs.rolling(window, min_periods=8).std()
    spike_score = diffs.abs() / diff_std.replace(0, np.nan)
    prev_value = series.shift(1)
    next_value = series.shift(-1)
    return pd.DataFrame(
        {
            "mean": rolling_mean,
            "std": rolling_std,
            "z_score": z_scores,
            "diff": diffs,
            "diff_std": diff_std,
            "spike_score": spike_score,
            "prev_value": prev_value,
            "next_value": next_value,
        }
    )


def safe_float(value: object) -> float | None:
    try:
        if value is None:
            return None
        if isinstance(value, (int, float, np.floating, np.integer)):
            return float(value)
        if isinstance(value, str):
            return float(value)
        return None
    except Exception:
        return None


def classify_anomaly_reason(row: pd.Series, metric_category: str) -> str:
    reasons = []
    z_score = safe_float(row.get("z_score"))
    spike_score = safe_float(row.get("spike_score"))
    diff_value = safe_float(row.get("diff"))
    std_value = safe_float(row.get("std"))
    mean_value = safe_float(row.get("mean"))
    prev_value = safe_float(row.get("prev_value"))
    current_value = safe_float(row.get("value"))

    if mean_value is not None and current_value is not None and mean_value != 0:
        ratio = abs(current_value / mean_value)
    else:
        ratio = None

    if z_score is not None and abs(z_score) > 3:
        reasons.append("偏离近期均值过大")

    if spike_score is not None and spike_score > 6:
        reasons.append("突升/突降")

    if metric_category in {"功率", "频率", "流量"}:
        if diff_value is not None:
            if diff_value < 0:
                reasons.append("突降")
            elif diff_value > 0:
                reasons.append("突升")
        if mean_value is not None and std_value is not None and mean_value != 0:
            if std_value / abs(mean_value) > 1.0:
                reasons.append("波动剧烈")
        if prev_value is not None and current_value is not None:
            if prev_value > 0 and current_value <= 0:
                reasons.append("突停")
            elif prev_value <= 0 and current_value > 0:
                reasons.append("突启")
        if ratio is not None and (ratio >= 10 or ratio <= 0.1):
            reasons.append("疑似单位跳变")

    if metric_category in {"温度", "供水温度", "回水温度"}:
        if diff_value is not None and abs(diff_value) >= 5:
            reasons.append("温度跳变")

    if metric_category in {"功率", "频率"}:
        if mean_value is not None and mean_value == 0:
            reasons.append("疑似突停")

    if not reasons:
        reasons.append("异常值（Z-Score 超阈值）")

    return "、".join(dict.fromkeys(reasons))


def summarize_energy(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算能耗增量
    [修复] 保留原始增量用于诊断，同时计算修正后的增量
    """
    if df.empty:
        return pd.DataFrame()
    df = df.copy().sort_values("timestamp")
    # 计算原始增量（用于诊断负增量问题）
    df["value_delta_raw"] = df.groupby(["component_id", "metric_name"])["value"].diff()
    # 修正后的增量（负值截断为0，因为电量不应该下降，除非计量复位）
    df["value_delta"] = df["value_delta_raw"].clip(lower=0)
    return df


def merge_energy_power(component_df: pd.DataFrame, energy_df: pd.DataFrame, component_type: str) -> pd.DataFrame:
    if energy_df.empty:
        return component_df
    energy_power = energy_df[(energy_df["component_type"] == component_type) & (energy_df["metric_category"] == "功率")]
    if energy_power.empty:
        return component_df
    return pd.concat([component_df, energy_power], ignore_index=True)


def compute_cop(chiller_df: pd.DataFrame) -> pd.DataFrame:
    """
    [修复] 改进 COP 计算逻辑：
    
    问题：功率数据的 component_id 是 G11-1-冷机1#，而流量/温度数据的 component_id 是 G11-1
    解决：使用 group_id (G11-1) 来关联不同指标的数据
    
    COP = 冷量 / 功率
    冷量 = 1.163 * 流量 * (回水温度 - 供水温度) [单位: kW]
    """
    if chiller_df.empty:
        return pd.DataFrame()

    power_df = cast(pd.DataFrame, chiller_df[chiller_df["metric_category"] == "功率"])
    flow_df = cast(pd.DataFrame, chiller_df[chiller_df["metric_category"] == "流量"])
    supply_df = cast(pd.DataFrame, chiller_df[chiller_df["metric_category"] == "供水温度"])
    return_df = cast(pd.DataFrame, chiller_df[chiller_df["metric_category"] == "回水温度"])

    # 调试信息
    print(f"[DEBUG] 功率数据行数: {len(power_df)}, 唯一group_id: {power_df['group_id'].unique().tolist() if not power_df.empty else []}")
    print(f"[DEBUG] 流量数据行数: {len(flow_df)}, 唯一group_id: {flow_df['group_id'].unique().tolist() if not flow_df.empty else []}")
    print(f"[DEBUG] 供水温度数据行数: {len(supply_df)}, 唯一group_id: {supply_df['group_id'].unique().tolist() if not supply_df.empty else []}")
    print(f"[DEBUG] 回水温度数据行数: {len(return_df)}, 唯一group_id: {return_df['group_id'].unique().tolist() if not return_df.empty else []}")

    if power_df.empty or flow_df.empty or supply_df.empty or return_df.empty:
        return pd.DataFrame()

    results = []
    
    # [修复] 使用 group_id 而不是 component_id 来分组和匹配
    for group_id in power_df["group_id"].unique():
        # 获取该组的功率数据（可能有多台冷机，取平均或汇总）
        power_group = power_df[power_df["group_id"] == group_id]
        power = (
            power_group
            .set_index("timestamp")["value"]
            .resample("H").mean()
        )
        
        # 获取该组的流量数据
        flow_group = flow_df[flow_df["group_id"] == group_id]
        if flow_group.empty:
            print(f"[DEBUG] {group_id}: 无流量数据")
            continue
        flow = (
            flow_group
            .set_index("timestamp")["value"]
            .resample("H").mean()
        )
        
        # 获取该组的供水温度数据
        supply_group = supply_df[supply_df["group_id"] == group_id]
        if supply_group.empty:
            print(f"[DEBUG] {group_id}: 无供水温度数据")
            continue
        supply = (
            supply_group
            .set_index("timestamp")["value"]
            .resample("H").mean()
        )
        
        # 获取该组的回水温度数据
        return_group = return_df[return_df["group_id"] == group_id]
        if return_group.empty:
            print(f"[DEBUG] {group_id}: 无回水温度数据")
            continue
        ret = (
            return_group
            .set_index("timestamp")["value"]
            .resample("H").mean()
        )

        df = cast(pd.DataFrame, pd.concat([power, flow, supply, ret], axis=1, keys=["power", "flow", "supply", "return"]))
        df = cast(pd.DataFrame, df.dropna())
        if df.empty:
            print(f"[DEBUG] {group_id}: 合并后数据为空")
            continue
            
        df["delta_t"] = df["return"] - df["supply"]
        df = cast(pd.DataFrame, df[df["delta_t"] > 0])
        if df.empty:
            print(f"[DEBUG] {group_id}: 无有效温差数据")
            continue
            
        # 冷量计算: Q = 1.163 * 流量(m³/h) * 温差(℃) [kW]
        # 注意：这里假设流量单位是 m³/h，如果是 L/h 需要除以 1000
        df["cooling_kw"] = 1.163 * df["flow"] * df["delta_t"]
        df["cop"] = df["cooling_kw"] / df["power"]
        df = cast(pd.DataFrame, df.replace([np.inf, -np.inf], np.nan).dropna(subset=["cop"]))
        
        if df.empty:
            print(f"[DEBUG] {group_id}: COP 计算结果为空")
            continue
            
        # 过滤异常 COP 值（通常在 2-8 之间）
        df = cast(pd.DataFrame, df[(df["cop"] > 0.5) & (df["cop"] < 15)])
        if df.empty:
            print(f"[DEBUG] {group_id}: 过滤后无有效 COP")
            continue
            
        results.append(
            {
                "component_id": group_id,
                "avg_cop": df["cop"].mean(),
                "min_cop": df["cop"].min(),
                "max_cop": df["cop"].max(),
                "samples": len(df),
            }
        )
        print(f"[DEBUG] {group_id}: COP 计算成功, 平均值 {df['cop'].mean():.2f}, 样本数 {len(df)}")

    return pd.DataFrame(results)


def build_report(component_df: pd.DataFrame, energy_df: pd.DataFrame) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = ["# 数据问题与指标总结（罗总指导版）", "", f"生成时间: {now}", ""]

    lines.append("## 一句话结论")
    if component_df.empty and energy_df.empty:
        lines.append("- 当前没有读取到任何可用数据，无法评估运行情况。")
    else:
        lines.append("- 数据已读取，已生成效率、稳定性、异常与能耗的概览与问题提示。")

    lines.append("")
    lines.append("## 术语说明（简要）")
    lines.append("- **效率**：用能耗换来多少产出（如冷量/功率）。")
    lines.append("- **稳定性**：指标波动是否明显（波动越大，越不稳定）。")
    lines.append("- **异常值**：与历史行为明显不同的点（可能故障或测量异常）。")
    lines.append("- **能耗**：电量、功率的使用情况与变化。")

    lines.append("## 数据覆盖情况")
    missing_dirs = [key for key, rel in DATA_DIRS.items() if not resolve_path(rel).exists()]
    if missing_dirs:
        lines.append(f"- 缺失目录: {', '.join(missing_dirs)}（这些数据未参与统计）")
    else:
        lines.append("- 数据目录完整")

    if component_df.empty:
        lines.append("- 组件数据为空（无法计算组件级指标）")
    else:
        coverage = (
            component_df.groupby(["component_type", "metric_category"])["value"]
            .count()
            .reset_index()
        )
        lines.append("- 组件数据指标分布（采样条数）:")
        for _, row in coverage.iterrows():
            lines.append(f"  - {row['component_type']} / {row['metric_category']}: {row['value']}")

    if energy_df.empty:
        lines.append("- 能耗数据为空（无法计算能耗趋势）")
    else:
        energy_coverage = energy_df.groupby(["component_type", "metric_category"])["value"].count().reset_index()
        lines.append("- 能耗数据指标分布（采样条数）:")
        for _, row in energy_coverage.iterrows():
            lines.append(f"  - {row['component_type']} / {row['metric_category']}: {row['value']}")

    lines.append("")
    lines.append("## 效率概况")
    if component_df.empty:
        lines.append("- 无法计算效率（无组件数据）")
    else:
        # 制冷主机 COP
        lines.append("### 制冷主机")
        chiller_df = cast(pd.DataFrame, component_df[component_df["component_type"] == "制冷主机"])
        chiller_df = merge_energy_power(chiller_df, energy_df, "制冷主机")
        
        # 输出调试信息
        print(f"\n[DEBUG] 制冷主机数据量: {len(chiller_df)}")
        print(f"[DEBUG] 指标分布: {chiller_df['metric_category'].value_counts().to_dict()}")
        print(f"[DEBUG] 设备ID示例: {chiller_df['component_id'].unique()[:10].tolist()}")
        print(f"[DEBUG] 组ID示例: {chiller_df['group_id'].unique()[:10].tolist()}")
        
        cop_df = compute_cop(chiller_df)
        if cop_df.empty:
            lines.append("- COP: 无法计算（缺少功率/流量/供回水温度中的至少一项，或数据无法关联）")
            # 显示可用的指标
            for cat in ["功率", "流量", "供水温度", "回水温度"]:
                count = len(chiller_df[chiller_df["metric_category"] == cat])
                lines.append(f"  - {cat}数据: {count} 条")
        else:
            for _, row in cop_df.iterrows():
                lines.append(
                    f"- {row['component_id']} COP: 平均 {row['avg_cop']:.2f} "
                    f"(范围 {row['min_cop']:.2f}-{row['max_cop']:.2f}, 样本 {int(row['samples'])})"
                )
        
        # 水泵详细信息
        lines.append("")
        lines.append("### 水泵")
        pump_df = cast(pd.DataFrame, component_df[component_df["component_type"] == "水泵"])
        pump_df = merge_energy_power(pump_df, energy_df, "水泵")
        if pump_df.empty:
            lines.append("- 无水泵数据")
        else:
            # 按设备分组显示
            for group_id in sorted(pump_df["group_id"].unique()):
                group_data = pump_df[pump_df["group_id"] == group_id]
                freq_data = group_data[group_data["metric_category"] == "频率"]
                status_data = group_data[group_data["metric_category"] == "运行状态"]
                power_data = group_data[group_data["metric_category"] == "功率"]
                
                info_parts = [f"**{group_id}**:"]
                if not freq_data.empty:
                    info_parts.append(f"频率均值 {freq_data['value'].mean():.2f} Hz")
                if not power_data.empty:
                    info_parts.append(f"功率均值 {power_data['value'].mean():.2f}")
                if not status_data.empty:
                    info_parts.append(f"运行状态记录 {len(status_data)} 条")
                lines.append(f"- {', '.join(info_parts)}")
        
        # 冷却塔详细信息
        lines.append("")
        lines.append("### 冷却塔")
        tower_df = cast(pd.DataFrame, component_df[component_df["component_type"] == "冷却塔"])
        tower_df = merge_energy_power(tower_df, energy_df, "冷却塔")
        if tower_df.empty:
            lines.append("- 无冷却塔数据")
        else:
            # 按设备分组显示
            for group_id in sorted(tower_df["group_id"].unique()):
                group_data = tower_df[tower_df["group_id"] == group_id]
                supply_data = group_data[group_data["metric_category"] == "供水温度"]
                return_data = group_data[group_data["metric_category"] == "回水温度"]
                status_data = group_data[group_data["metric_category"] == "运行状态"]
                power_data = group_data[group_data["metric_category"] == "功率"]
                
                info_parts = [f"**{group_id}**:"]
                if not supply_data.empty:
                    info_parts.append(f"供水温度 {supply_data['value'].mean():.2f}°C")
                if not return_data.empty:
                    info_parts.append(f"回水温度 {return_data['value'].mean():.2f}°C")
                if not supply_data.empty and not return_data.empty:
                    delta_t = return_data['value'].mean() - supply_data['value'].mean()
                    info_parts.append(f"温差 {delta_t:.2f}°C")
                if not power_data.empty:
                    info_parts.append(f"功率均值 {power_data['value'].mean():.2f}")
                if not status_data.empty:
                    info_parts.append(f"运行状态记录 {len(status_data)} 条")
                lines.append(f"- {', '.join(info_parts)}")

    lines.append("")
    lines.append("## 稳定性概况")
    stability_records = []
    focus_categories = {"功率", "频率", "温度", "负载率", "流量"}
    for (component_id, metric_category), group in component_df.groupby(["component_id", "metric_category"]):
        group = cast(pd.DataFrame, group)
        if metric_category not in focus_categories:
            continue
        if group["value"].mean() == 0:
            continue
        cv = group["value"].std() / group["value"].mean()
        stability_records.append(
            {
                "component_id": component_id,
                "metric_category": metric_category,
                "cv": cv,
            }
        )

    if not stability_records:
        lines.append("- 无法计算稳定性（数据不足）")
    else:
        stability_df = pd.DataFrame(stability_records).sort_values("cv", ascending=False)
        lines.append("- 稳定性波动最高的前 10 项（CV=标准差/均值，越大越不稳定）:")
        for _, row in stability_df.head(10).iterrows():
            lines.append(f"  - {row['component_id']} / {row['metric_category']}：CV={row['cv']:.2f}")

    lines.append("")
    lines.append("## 异常值概况")
    anomaly_records = []
    anomaly_details: list[pd.DataFrame] = []
    for (component_id, metric_category), group in component_df.groupby(["component_id", "metric_category"]):
        group = cast(pd.DataFrame, group)
        group = group.sort_values("timestamp")
        if len(group) < 10:
            continue
        scores = compute_anomaly_scores(cast(pd.Series, group["value"]))
        z_abs = scores["z_score"].abs()
        mask = z_abs > 3
        count = int(mask.sum())
        if count > 0:
            anomaly_records.append(
                {
                    "component_id": component_id,
                    "metric_category": metric_category,
                    "count": count,
                }
            )
            detail = group.loc[mask, ["timestamp", "value", "unit", "metric_name", "source_file"]].copy()
            detail["component_id"] = component_id
            detail["metric_category"] = metric_category
            detail["z_score"] = z_abs[mask].values
            detail["rolling_mean"] = scores.loc[mask, "mean"].values
            detail["rolling_std"] = scores.loc[mask, "std"].values
            detail["reason"] = detail.apply(
                lambda r: classify_anomaly_reason(r, metric_category),
                axis=1,
            )
            anomaly_details.append(detail)

    if not anomaly_records:
        lines.append("- 未发现明显异常值")
    else:
        anomaly_df = pd.DataFrame(anomaly_records).sort_values("count", ascending=False)
        lines.append("- 异常值数量最高的前 10 项:")
        for _, row in anomaly_df.head(10).iterrows():
            lines.append(f"  - {row['component_id']} / {row['metric_category']}：{row['count']} 个异常点")
        lines.append("")
        lines.append("- **异常值说明**：")
        lines.append("  - 异常值通过滚动Z-Score方法检测，表示数据点偏离近期平均值超过3个标准差")
        lines.append("  - 异常值**不一定代表设备故障**，可能原因包括：")
        lines.append("    - 设备启停瞬间的正常波动（突启/突停）")
        lines.append("    - 负载突变导致的合理变化（突升/突降）")
        lines.append("    - 传感器偶发性读数偏差或单位变化（疑似单位跳变）")
        lines.append("    - 真实的异常工况（需现场确认）")
        lines.append("  - 建议：结合运行日志和现场状态，重点关注异常值持续出现的设备")

        if anomaly_details:
            anomaly_table = pd.concat(anomaly_details, ignore_index=True)
            anomaly_table = anomaly_table.sort_values("z_score", ascending=False)
            anomaly_table = anomaly_table[
                [
                    "component_id",
                    "metric_category",
                    "metric_name",
                    "timestamp",
                    "value",
                    "unit",
                    "z_score",
                    "rolling_mean",
                    "rolling_std",
                    "reason",
                    "source_file",
                ]
            ]
            details_path = BASE_DIR / "docs" / "异常明细.csv"
            details_path.parent.mkdir(parents=True, exist_ok=True)
            anomaly_table.to_csv(details_path, index=False, encoding="utf-8-sig")
            lines.append("  - 详表输出: docs/异常明细.csv（包含设备/指标/时间/值/异常分数/原因）")

    lines.append("")
    lines.append("## 能耗概况")
    if energy_df.empty:
        lines.append("- 无能耗数据")
    else:
        # 分析能耗数据
        energy_data = energy_df[energy_df["metric_category"] == "电量"]
        power_data = energy_df[energy_df["metric_category"] == "功率"]
        
        # 调试信息
        print(f"[DEBUG] 能耗数据总行数: {len(energy_df)}")
        print(f"[DEBUG] 指标分布: {energy_df['metric_category'].value_counts().to_dict()}")
        
        lines.append("### 电量统计")
        if energy_data.empty:
            lines.append("- 未找到电量数据（可能指标分类有误）")
        else:
            # 按设备分组计算电量增量
            energy_by_device = energy_data.groupby("component_id").agg({
                "value": ["min", "max"],
                "value_delta": "sum"
            }).reset_index()
            energy_by_device.columns = ["设备", "起始读数", "结束读数", "增量合计"]
            energy_by_device["差值"] = energy_by_device["结束读数"] - energy_by_device["起始读数"]
            
            total_by_diff = energy_by_device["差值"].sum()
            total_by_delta = energy_by_device["增量合计"].sum()
            
            lines.append(f"- 统计设备数: {len(energy_by_device)}")
            lines.append(f"- 电量总计（首末差值法）: {total_by_diff:,.2f}")
            lines.append(f"- 电量总计（逐点增量法）: {total_by_delta:,.2f}")
            
            if total_by_diff > 1e9:
                lines.append(f"  - ⚠️ 电量数值异常大，请确认单位是否为kWh或存在数据问题")
        
        lines.append("")
        lines.append("### 功率统计")
        if power_data.empty:
            lines.append("- 未找到功率数据")
            # 显示所有指标类别帮助诊断
            lines.append(f"- 当前能耗数据中的指标类别: {energy_df['metric_category'].unique().tolist()}")
        else:
            power_mean = power_data["value"].mean()
            power_max = power_data["value"].max()
            power_min = power_data["value"].min()
            lines.append(f"- 平均功率: {power_mean:.2f}")
            lines.append(f"- 最大功率: {power_max:.2f}")
            lines.append(f"- 最小功率: {power_min:.2f}")
            lines.append(f"- 功率记录数: {len(power_data):,}")
        
        lines.append("")
        lines.append("### 数据质量检查")
        # 负增量检查（使用原始增量）
        if "value_delta_raw" in energy_df.columns:
            negative_deltas = int((energy_df["value_delta_raw"] < 0).sum())
            lines.append(f"- 电量负增量记录: {negative_deltas}")
            if negative_deltas > 0:
                lines.append(f"  - 说明：负增量通常表示电表复位或数据异常，已在统计时修正为0")
        else:
            negative_deltas = 0
            lines.append(f"- 电量负增量记录: 0")
        
        # 异常尖峰检查
        if not energy_data.empty:
            energy_series = energy_data["value_delta"].dropna()
            if not energy_series.empty and energy_series.std() > 0:
                q1 = energy_series.quantile(0.25)
                q3 = energy_series.quantile(0.75)
                iqr = q3 - q1
                spike_threshold = q3 + 3 * iqr
                spikes = int((energy_series > spike_threshold).sum())
                lines.append(f"- 电量异常尖峰记录: {spikes}")
                if spikes > 0:
                    lines.append(f"  - 说明：尖峰指增量值超过 {spike_threshold:.2f} 的记录，可能是数据跳变或真实高负载")
            else:
                spikes = 0
                lines.append(f"- 电量异常尖峰记录: 0")
        else:
            spikes = 0

    lines.append("")
    lines.append("## 当前发现的问题")
    issues = []
    if missing_dirs:
        issues.append("部分数据目录缺失")
    if component_df.empty:
        issues.append("组件数据为空")
    if energy_df.empty:
        issues.append("能耗数据为空")
    if stability_records:
        unstable = [r for r in stability_records if r["cv"] >= 0.5]
        if unstable:
            issues.append("存在稳定性波动较高的指标（CV>=0.5）")
    if anomaly_records:
        high_anomaly = [r for r in anomaly_records if r["count"] >= 20]
        if high_anomaly:
            issues.append("存在异常值较多的指标（>=20）")
    if energy_df.empty is False and negative_deltas > 0:
        issues.append("能耗数据存在负增量（可能为计量复位或异常）")
    if energy_df.empty is False and spikes > 0:
        issues.append("能耗数据存在尖峰（需复核）")

    if not issues:
        lines.append("- 未发现明显问题")
    else:
        for issue in issues:
            lines.append(f"- {issue}")

    lines.append("")
    lines.append("## 建议关注事项")
    lines.append("- 若 COP 无法计算，请优先确认：功率、流量、供回水温度是否都有数据且设备ID能够匹配。")
    lines.append("- 异常值多的设备，建议结合运行日志或现场状态确认是否为真实故障。")
    lines.append("- 能耗为 0 或无波动，可能是计量点未接入或被重置。")
    lines.append("- 稳定性波动大且持续的设备，建议重点排查控制策略或传感器漂移。")

    return "\n".join(lines)


def main() -> None:
    component_df = load_component_data()
    energy_df = summarize_energy(load_energy_data())

    report = build_report(component_df, energy_df)
    output_path = BASE_DIR / "docs" / "数据问题总结.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")
    print(f"\n报告已生成: {output_path}")


if __name__ == "__main__":
    main()
