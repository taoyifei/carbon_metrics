from __future__ import annotations

import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date as dt_date
from datetime import datetime
from pathlib import Path
from typing import Iterable, cast

import numpy as np
import pandas as pd
import streamlit as st

# ============================================================================
# 配置
# ============================================================================

BASE_DIR = Path(__file__).resolve().parents[1]
CACHE_DIR = BASE_DIR / ".cache"
CACHE_FILES = {
    "component": CACHE_DIR / "component_data.pkl",
    "energy": CACHE_DIR / "energy_data.pkl",
}

# [修复] 完整的数据目录结构
DATA_DIRS = {
    # 水泵组件数据
    "pump_component": Path("冷源设备相关数据反馈20260120") / "冷冻水泵冷却水泵",
    # 冷却塔组件数据
    "tower_component": Path("冷源设备相关数据反馈20260120") / "冷却塔",
    # 制冷主机组件数据
    "chiller_component": Path("冷源设备相关数据反馈20260120") / "制冷主机",
    # 能耗数据
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

# 并行处理的最大工作进程数
MAX_WORKERS = 4


# ============================================================================
# 工具函数
# ============================================================================


def resolve_path(rel_path: Path) -> Path:
    return BASE_DIR / rel_path


def list_excel_files(rel_path: Path) -> list[Path]:
    """递归获取目录下所有Excel文件"""
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
    [修复] 改进设备ID提取逻辑，支持多种文件名格式
    """
    # 先尝试提取 G11-1, G12-3 等标准格式
    group_match = re.search(r"G(\d+)-(\d)", text)
    
    # 如果没有 G 前缀，尝试匹配 11-1, 12-3 等格式
    if not group_match:
        bare_match = re.search(r"(\d{2})-(\d)", text)
        if bare_match:
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
        r"冷冻泵\d*#?",
        r"冷却泵\d*#?",
        r"冷塔\d*#?",
        r"冷机\d+#",
        r"CT\d+",
    ]
    
    device_id = None
    for pat in device_patterns:
        match = re.search(pat, text)
        if match:
            device_id = match.group(0)
            break
    
    if group_id and device_id:
        return f"{group_id}-{device_id}"
    elif group_id:
        return group_id
    elif device_id:
        return device_id
    else:
        return "未知设备"


def normalize_component_id_for_matching(component_id: str) -> str:
    """将设备ID标准化为统一的组ID，用于跨指标匹配"""
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
    """
    [修复] 结合指标名、文件名和目录名进行分类
    """
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
    """判断是否为能耗数据文件"""
    name = file_path.stem
    path_str = str(file_path)
    return "功率" in name or "电量" in name or "功率和电量" in path_str


# ============================================================================
# Excel 读取函数
# ============================================================================


def read_standard_sheet_optimized(file_path: Path) -> pd.DataFrame:
    """读取标准格式的Excel（带点名、采集时间、采集值列）"""
    try:
        xl = pd.ExcelFile(file_path, engine="openpyxl")
    except Exception:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    
    for sheet in xl.sheet_names:
        try:
            df_header = pd.read_excel(xl, sheet_name=sheet, nrows=0, engine="openpyxl")
            columns = [normalize_column(c) for c in df_header.columns]
            
            col_point = pick_column(columns, ["点名"])
            col_time = pick_column(columns, ["采集时间", "时间"])
            col_value = pick_column(columns, ["采集值", "值"])
            col_unit = pick_column(columns, ["单位"])

            if not (col_point and col_time and col_value):
                continue

            usecols = [c for c in [col_point, col_time, col_value, col_unit] if c]
            
            df = pd.read_excel(xl, sheet_name=sheet, usecols=usecols, engine="openpyxl")
            
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
                
            df = df.dropna(subset=["timestamp", "value"])
            if df.empty:
                continue
                
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df = df.dropna(subset=["timestamp", "value"])
            
            if df.empty:
                continue
                
            df["source_file"] = str(file_path.relative_to(BASE_DIR))
            df["sheet"] = sheet
            frames.append(df)
            
        except Exception:
            continue

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
    """读取能耗数据格式的Excel"""
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
                
    df = df[df["timestamp"].notna()]
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["timestamp", "value"])
    
    if df.empty:
        return pd.DataFrame()

    df["point_name"] = point_name if point_name else col_value
    df["unit"] = None
    df["source_file"] = str(file_path.relative_to(BASE_DIR))
    df["sheet"] = sheet
    
    return df[["point_name", "timestamp", "value", "unit", "source_file", "sheet"]]


def attach_metadata(df: pd.DataFrame, file_path: Path) -> pd.DataFrame:
    """添加元数据"""
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


# ============================================================================
# 数据加载
# ============================================================================


def process_component_file(file_path: Path) -> pd.DataFrame:
    """处理单个组件数据文件"""
    # 先尝试标准格式
    df = read_standard_sheet_optimized(file_path)
    # 如果失败且是能耗文件，尝试能耗格式
    if df.empty and is_energy_file(file_path):
        df = read_energy_sheet_optimized(file_path)
    return attach_metadata(df, file_path)


def process_energy_file(file_path: Path) -> pd.DataFrame:
    """处理单个能耗数据文件"""
    df = read_energy_sheet_optimized(file_path)
    return attach_metadata(df, file_path)


def load_component_data() -> pd.DataFrame:
    """加载组件数据"""
    files = (
        list_excel_files(DATA_DIRS["pump_component"])
        + list_excel_files(DATA_DIRS["tower_component"])
        + list_excel_files(DATA_DIRS["chiller_component"])
    )
    
    if not files:
        return pd.DataFrame()
    
    frames: list[pd.DataFrame] = []
    for file_path in files:
        try:
            df = process_component_file(file_path)
            if not df.empty:
                frames.append(df)
        except Exception:
            continue
    
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def load_energy_data() -> pd.DataFrame:
    """加载能耗数据"""
    files = (
        list_excel_files(DATA_DIRS["tower_energy"]) 
        + list_excel_files(DATA_DIRS["pump_energy"])
        + list_excel_files(DATA_DIRS["chiller_energy"])
    )
    
    if not files:
        return pd.DataFrame()
    
    frames: list[pd.DataFrame] = []
    for file_path in files:
        try:
            df = process_energy_file(file_path)
            if not df.empty:
                frames.append(df)
        except Exception:
            continue
    
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def read_component_file_cached(file_path: Path) -> pd.DataFrame:
    return process_component_file(file_path)


@st.cache_data(ttl=3600, show_spinner=False)
def read_energy_file_cached(file_path: Path) -> pd.DataFrame:
    return process_energy_file(file_path)


def load_all_data(show_progress: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    progress = None
    if show_progress:
        progress = st.session_state.get("load_progress")
    component_files = (
        list_excel_files(DATA_DIRS["pump_component"])
        + list_excel_files(DATA_DIRS["tower_component"])
        + list_excel_files(DATA_DIRS["chiller_component"])
    )
    energy_files = (
        list_excel_files(DATA_DIRS["tower_energy"])
        + list_excel_files(DATA_DIRS["pump_energy"])
        + list_excel_files(DATA_DIRS["chiller_energy"])
    )
    total_files = len(component_files) + len(energy_files)
    processed = 0

    component_frames: list[pd.DataFrame] = []
    if progress is not None:
        progress.progress(0, text=f"加载组件数据 0/{len(component_files)}")
    for file_path in component_files:
        try:
            df = read_component_file_cached(file_path)
            if not df.empty:
                component_frames.append(df)
        except Exception:
            pass
        processed += 1
        if progress is not None:
            progress.progress(
                int(processed / max(total_files, 1) * 100),
                text=f"加载组件数据 {processed}/{len(component_files)}",
            )

    energy_frames: list[pd.DataFrame] = []
    if progress is not None:
        progress.progress(
            int(processed / max(total_files, 1) * 100),
            text=f"加载能耗数据 0/{len(energy_files)}",
        )
    for file_path in energy_files:
        try:
            df = read_energy_file_cached(file_path)
            if not df.empty:
                energy_frames.append(df)
        except Exception:
            pass
        processed += 1
        if progress is not None:
            progress.progress(
                int(processed / max(total_files, 1) * 100),
                text=f"加载能耗数据 {processed - len(component_files)}/{len(energy_files)}",
            )

    component_df = pd.concat(component_frames, ignore_index=True) if component_frames else pd.DataFrame()
    energy_df = summarize_energy(
        pd.concat(energy_frames, ignore_index=True) if energy_frames else pd.DataFrame()
    )
    return component_df, energy_df


# ============================================================================
# 数据分析函数
# ============================================================================


def compute_anomalies(series: pd.Series, window: int = 48, z_threshold: float = 3.0) -> pd.Series:
    """计算异常值（基于滚动Z-Score）"""
    rolling_mean = series.rolling(window, min_periods=8).mean()
    rolling_std = series.rolling(window, min_periods=8).std()
    z_scores = (series - rolling_mean) / rolling_std.replace(0, np.nan)
    return z_scores.abs() > z_threshold


def summarize_energy(df: pd.DataFrame) -> pd.DataFrame:
    """计算能耗增量"""
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df = df.sort_values("timestamp")
    # 计算原始增量（用于诊断）
    df["value_delta_raw"] = df.groupby(["component_id", "metric_name"])["value"].diff()
    # 修正后的增量（负值截断为0）
    df["value_delta"] = df["value_delta_raw"].clip(lower=0)
    return df


def compute_stability(df: pd.DataFrame) -> pd.DataFrame:
    """计算稳定性指标（变异系数CV）"""
    if df.empty:
        return pd.DataFrame()
    
    records = []
    for (component_id, metric_category), group in df.groupby(["component_id", "metric_category"]):
        mean_val = group["value"].mean()
        if mean_val == 0 or pd.isna(mean_val):
            continue
        cv = group["value"].std() / mean_val
        records.append({
            "component_id": component_id,
            "metric_category": metric_category,
            "cv": cv,
            "mean": mean_val,
            "std": group["value"].std(),
            "count": len(group),
        })
    
    return pd.DataFrame(records)


def compute_cop(chiller_df: pd.DataFrame) -> pd.DataFrame:
    """计算制冷主机COP"""
    if chiller_df.empty:
        return pd.DataFrame()

    power_df = chiller_df[chiller_df["metric_category"] == "功率"]
    flow_df = chiller_df[chiller_df["metric_category"] == "流量"]
    supply_df = chiller_df[chiller_df["metric_category"] == "供水温度"]
    return_df = chiller_df[chiller_df["metric_category"] == "回水温度"]

    if power_df.empty or flow_df.empty or supply_df.empty or return_df.empty:
        return pd.DataFrame()

    results = []
    for group_id in power_df["group_id"].unique():
        power = power_df[power_df["group_id"] == group_id].set_index("timestamp")["value"].resample("H").mean()
        
        flow_group = flow_df[flow_df["group_id"] == group_id]
        if flow_group.empty:
            continue
        flow = flow_group.set_index("timestamp")["value"].resample("H").mean()
        
        supply_group = supply_df[supply_df["group_id"] == group_id]
        if supply_group.empty:
            continue
        supply = supply_group.set_index("timestamp")["value"].resample("H").mean()
        
        return_group = return_df[return_df["group_id"] == group_id]
        if return_group.empty:
            continue
        ret = return_group.set_index("timestamp")["value"].resample("H").mean()

        df = pd.concat([power, flow, supply, ret], axis=1, keys=["power", "flow", "supply", "return"])
        df = df.dropna()
        if df.empty:
            continue
            
        df["delta_t"] = df["return"] - df["supply"]
        df = df[df["delta_t"] > 0]
        if df.empty:
            continue
            
        df["cooling_kw"] = 1.163 * df["flow"] * df["delta_t"]
        df["cop"] = df["cooling_kw"] / df["power"]
        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["cop"])
        df = df[(df["cop"] > 0.5) & (df["cop"] < 15)]
        
        if df.empty:
            continue
            
        results.append({
            "group_id": group_id,
            "avg_cop": df["cop"].mean(),
            "min_cop": df["cop"].min(),
            "max_cop": df["cop"].max(),
            "samples": len(df),
        })

    return pd.DataFrame(results)


def safe_float(value: object) -> float | None:
    try:
        if value is None:
            return None
        value_f = float(value)
        if np.isnan(value_f):
            return None
        return value_f
    except Exception:
        return None


def filter_by_time(df: pd.DataFrame, start: datetime, end: datetime) -> pd.DataFrame:
    if df.empty:
        return df
    return df[(df["timestamp"] >= start) & (df["timestamp"] <= end)]


# ============================================================================
# 页面渲染 - 系统总览
# ============================================================================


def render_system_overview(component_df: pd.DataFrame, energy_df: pd.DataFrame) -> None:
    """系统总览页面"""
    st.subheader("🏭 系统总览(罗总指导版)")
    
    if component_df.empty and energy_df.empty:
        st.info("未加载到数据，请检查数据目录是否完整。")
        return

    # 系统级KPI
    col1, col2, col3, col4 = st.columns(4)
    
    total_energy = energy_df[energy_df["metric_category"] == "电量"]["value_delta"].sum() if not energy_df.empty else 0
    avg_power = energy_df[energy_df["metric_category"] == "功率"]["value"].mean() if not energy_df.empty else 0
    total_records = len(component_df) + len(energy_df)
    unique_devices = component_df["component_id"].nunique() if not component_df.empty else 0
    
    col1.metric("总电量（增量）", f"{safe_float(total_energy):.2f}" if safe_float(total_energy) else "N/A")
    col2.metric("平均功率", f"{safe_float(avg_power):.2f}" if safe_float(avg_power) else "N/A")
    col3.metric("数据记录数", f"{total_records:,}")
    col4.metric("设备数量", f"{unique_devices}")

    # 按组件类型统计
    st.markdown("### 📊 各组件数据覆盖")
    if not component_df.empty:
        coverage = component_df.groupby(["component_type", "metric_category"]).agg({
            "value": "count",
            "component_id": "nunique"
        }).reset_index()
        coverage.columns = ["组件类型", "指标类别", "记录数", "设备数"]
        st.dataframe(coverage, width="stretch")

    # 能耗趋势
    st.markdown("### 📈 系统能耗趋势")
    if not energy_df.empty:
        energy_trend = energy_df[energy_df["metric_category"] == "电量"]
        if not energy_trend.empty:
            trend = energy_trend.set_index("timestamp")["value_delta"].resample("H").sum()
            st.line_chart(trend)
        else:
            st.info("暂无电量数据趋势。")
    else:
        st.info("暂无能耗数据。")

    # 制冷主机COP
    st.markdown("### ❄️ 制冷主机效率 (COP)")
    if not component_df.empty:
        chiller_df = component_df[component_df["component_type"] == "制冷主机"].copy()
        if not energy_df.empty:
            energy_power = energy_df[(energy_df["component_type"] == "制冷主机") & (energy_df["metric_category"] == "功率")].copy()
            if not energy_power.empty:
                # 确保有group_id列
                if "group_id" not in energy_power.columns:
                    energy_power["group_id"] = energy_power["component_id"].apply(normalize_component_id_for_matching)
                chiller_df = pd.concat([chiller_df, energy_power], ignore_index=True)
        
        # 确保chiller_df有group_id
        if "group_id" not in chiller_df.columns and not chiller_df.empty:
            chiller_df["group_id"] = chiller_df["component_id"].apply(normalize_component_id_for_matching)
        
        cop_df = compute_cop(chiller_df)
        if not cop_df.empty:
            st.dataframe(cop_df.rename(columns={
                "group_id": "设备组",
                "avg_cop": "平均COP",
                "min_cop": "最小COP",
                "max_cop": "最大COP",
                "samples": "样本数"
            }), width="stretch")
        else:
            st.warning("无法计算COP（缺少功率/流量/供回水温度数据或数据无法关联）")


# ============================================================================
# 页面渲染 - 组件分析
# ============================================================================


def render_component_analysis(component_df: pd.DataFrame, energy_df: pd.DataFrame) -> None:
    """组件分析页面"""
    st.subheader("🔧 组件分析")
    
    if component_df.empty:
        st.info("暂无组件数据。")
        return

    # 选择组件类型和设备
    col1, col2, col3 = st.columns(3)
    with col1:
        component_type = st.selectbox(
            "组件类型",
            sorted(component_df["component_type"].dropna().unique().tolist()),
        )
    
    type_df = component_df[component_df["component_type"] == component_type]
    
    with col2:
        component_id = st.selectbox(
            "设备",
            sorted(type_df["component_id"].dropna().unique().tolist()),
        )
    
    with col3:
        metric_category = st.selectbox(
            "指标类别",
            sorted(type_df["metric_category"].dropna().unique().tolist()),
        )

    # 筛选数据
    focused = type_df[(type_df["component_id"] == component_id) & (type_df["metric_category"] == metric_category)]
    
    if focused.empty:
        st.info("该设备暂无选定指标数据。")
        return

    # 设备KPI
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("平均值", f"{focused['value'].mean():.2f}")
    col2.metric("最大值", f"{focused['value'].max():.2f}")
    col3.metric("最小值", f"{focused['value'].min():.2f}")
    col4.metric("记录数", f"{len(focused):,}")

    # 指标趋势
    st.markdown("#### 📈 指标趋势")
    focused_sorted = focused.sort_values("timestamp")
    st.line_chart(focused_sorted.set_index("timestamp")["value"])

    # 稳定性分析
    st.markdown("#### 📊 稳定性分析")
    mean_val = focused["value"].mean()
    std_val = focused["value"].std()
    cv = std_val / mean_val if mean_val != 0 else 0
    
    stability_col1, stability_col2, stability_col3 = st.columns(3)
    stability_col1.metric("标准差", f"{std_val:.2f}")
    stability_col2.metric("变异系数(CV)", f"{cv:.2f}")
    stability_col3.metric("稳定性评价", "稳定" if cv < 0.3 else "波动较大" if cv < 0.5 else "不稳定")

    # 异常检测
    st.markdown("#### ⚠️ 异常检测（Z-Score）")
    anomaly_mask = compute_anomalies(focused_sorted["value"])
    anomalies = focused_sorted[anomaly_mask]
    
    if anomalies.empty:
        st.success("未发现明显异常。")
    else:
        st.warning(f"发现 {len(anomalies)} 个异常点")
        st.dataframe(anomalies[["timestamp", "value", "metric_name", "unit"]].head(20), width="stretch")

    # 相关能耗
    if not energy_df.empty:
        st.markdown("#### ⚡ 相关能耗")
        group_id = normalize_component_id_for_matching(component_id)
        # [修复] 检查energy_df是否有group_id列，如果没有则使用component_id匹配
        if "group_id" in energy_df.columns:
            energy_focus = energy_df[energy_df["group_id"] == group_id]
        else:
            # 尝试用component_id直接匹配或部分匹配
            energy_focus = energy_df[energy_df["component_id"].str.contains(group_id, na=False)]
        if not energy_focus.empty:
            energy_sorted = energy_focus.sort_values("timestamp")
            st.line_chart(energy_sorted.set_index("timestamp")["value"])
        else:
            st.info("该设备暂无能耗数据。")


# ============================================================================
# 页面渲染 - 效率分析
# ============================================================================


def render_efficiency_analysis(component_df: pd.DataFrame, energy_df: pd.DataFrame) -> None:
    """效率分析页面"""
    st.subheader("⚡ 效率分析")
    
    if component_df.empty:
        st.info("暂无数据。")
        return

    # 制冷主机COP
    st.markdown("### ❄️ 制冷主机 COP")
    chiller_df = component_df[component_df["component_type"] == "制冷主机"].copy()
    
    # [修复] 合并能耗数据中的功率数据，并确保group_id列存在
    if not energy_df.empty:
        energy_power = energy_df[(energy_df["component_type"] == "制冷主机") & (energy_df["metric_category"] == "功率")].copy()
        if not energy_power.empty:
            # 确保有group_id列
            if "group_id" not in energy_power.columns:
                energy_power["group_id"] = energy_power["component_id"].apply(normalize_component_id_for_matching)
            chiller_df = pd.concat([chiller_df, energy_power], ignore_index=True)
    
    # 确保chiller_df有group_id
    if "group_id" not in chiller_df.columns and not chiller_df.empty:
        chiller_df["group_id"] = chiller_df["component_id"].apply(normalize_component_id_for_matching)
    
    cop_df = compute_cop(chiller_df)
    if not cop_df.empty:
        # COP对比图
        import plotly.express as px
        fig = px.bar(cop_df, x="group_id", y="avg_cop", 
                     error_y=cop_df["max_cop"] - cop_df["avg_cop"],
                     title="各设备组COP对比")
        st.plotly_chart(fig, width="stretch")
        
        st.dataframe(cop_df.rename(columns={
            "group_id": "设备组",
            "avg_cop": "平均COP",
            "min_cop": "最小COP",
            "max_cop": "最大COP",
            "samples": "样本数"
        }), width="stretch")
    else:
        st.warning("无法计算COP")
        # 显示原因
        st.markdown("**可能原因：**")
        for cat in ["功率", "流量", "供水温度", "回水温度"]:
            count = len(chiller_df[chiller_df["metric_category"] == cat]) if not chiller_df.empty else 0
            st.text(f"- {cat}数据: {count} 条")

    # 水泵效率
    st.markdown("### 💧 水泵效率")
    pump_df = component_df[component_df["component_type"] == "水泵"]
    if not pump_df.empty:
        pump_freq = pump_df[pump_df["metric_category"] == "频率"]
        if not energy_df.empty:
            pump_power = energy_df[(energy_df["component_type"] == "水泵") & (energy_df["metric_category"] == "功率")]
            if not pump_power.empty and not pump_freq.empty:
                avg_power = pump_power["value"].mean()
                avg_freq = pump_freq["value"].mean()
                if avg_freq > 0:
                    st.metric("功率/频率比", f"{avg_power/avg_freq:.3f}")
        st.metric("平均频率", f"{pump_freq['value'].mean():.2f} Hz" if not pump_freq.empty else "N/A")
    else:
        st.info("暂无水泵数据")

    # 冷却塔效率
    st.markdown("### 🗼 冷却塔效率")
    tower_df = component_df[component_df["component_type"] == "冷却塔"]
    if not tower_df.empty:
        tower_supply = tower_df[tower_df["metric_category"] == "供水温度"]
        tower_return = tower_df[tower_df["metric_category"] == "回水温度"]
        
        col1, col2 = st.columns(2)
        col1.metric("平均供水温度", f"{tower_supply['value'].mean():.2f}°C" if not tower_supply.empty else "N/A")
        col2.metric("平均回水温度", f"{tower_return['value'].mean():.2f}°C" if not tower_return.empty else "N/A")
        
        if not tower_supply.empty and not tower_return.empty:
            avg_delta = tower_return["value"].mean() - tower_supply["value"].mean()
            st.metric("平均温差", f"{avg_delta:.2f}°C")
    else:
        st.info("暂无冷却塔数据")


# ============================================================================
# 页面渲染 - 稳定性分析
# ============================================================================


def render_stability_analysis(component_df: pd.DataFrame) -> None:
    """稳定性分析页面"""
    st.subheader("📊 稳定性分析")
    
    if component_df.empty:
        st.info("暂无数据。")
        return

    stability_df = compute_stability(component_df)
    if stability_df.empty:
        st.info("无法计算稳定性指标。")
        return

    # 按组件类型筛选
    component_type = st.selectbox(
        "组件类型",
        ["全部"] + sorted(component_df["component_type"].dropna().unique().tolist()),
    )
    
    if component_type != "全部":
        component_ids = component_df[component_df["component_type"] == component_type]["component_id"].unique()
        stability_df = stability_df[stability_df["component_id"].isin(component_ids)]

    # 稳定性排名
    st.markdown("### 🔴 稳定性最差的指标（CV最高）")
    worst = stability_df.nlargest(15, "cv")
    st.dataframe(worst.rename(columns={
        "component_id": "设备",
        "metric_category": "指标",
        "cv": "变异系数",
        "mean": "平均值",
        "std": "标准差",
        "count": "记录数"
    }), width="stretch")

    # 稳定性分布
    st.markdown("### 📈 稳定性分布")
    bins = [0, 0.1, 0.3, 0.5, 1.0, float('inf')]
    labels = ["非常稳定(<0.1)", "稳定(0.1-0.3)", "一般(0.3-0.5)", "波动较大(0.5-1.0)", "不稳定(>1.0)"]
    stability_df["稳定性等级"] = pd.cut(stability_df["cv"], bins=bins, labels=labels)
    
    level_counts = stability_df["稳定性等级"].value_counts()
    st.bar_chart(level_counts)


# ============================================================================
# 页面渲染 - 异常检测
# ============================================================================


def render_anomaly_detection(component_df: pd.DataFrame) -> None:
    """异常检测页面"""
    st.subheader("⚠️ 异常检测")
    
    if component_df.empty:
        st.info("暂无数据。")
        return

    # 异常检测方法说明
    with st.expander("📖 异常检测方法说明", expanded=False):
        st.markdown("""
        **检测方法**: 滚动Z-Score
        
        - 计算每个数据点相对于前48个点（约2天）的平均值和标准差
        - 如果数据点偏离均值超过3个标准差，则标记为异常
        
        **异常不一定代表故障**，可能原因包括：
        - 设备启停瞬间的正常波动
        - 负载突变导致的合理变化  
        - 传感器偶发性读数偏差
        - 季节性或时段性的正常变化
        - 真实的异常工况（需现场确认）
        """)

    # 筛选条件
    col1, col2 = st.columns(2)
    with col1:
        component_type = st.selectbox(
            "组件类型",
            sorted(component_df["component_type"].dropna().unique().tolist()),
        )
    with col2:
        metric_category = st.selectbox(
            "指标类别",
            sorted(component_df[component_df["component_type"] == component_type]["metric_category"].dropna().unique().tolist()),
        )

    focused = component_df[
        (component_df["component_type"] == component_type) & 
        (component_df["metric_category"] == metric_category)
    ]
    
    if focused.empty:
        st.info("暂无符合条件的数据。")
        return

    # 计算整体统计信息用于判断异常原因
    overall_mean = focused["value"].mean()
    overall_std = focused["value"].std()
    overall_min = focused["value"].min()
    overall_max = focused["value"].max()

    # 计算异常
    anomaly_records = []
    for component_id, group in focused.groupby("component_id"):
        group = group.sort_values("timestamp")
        if len(group) < 10:
            continue
        
        # 计算设备级别的统计
        device_mean = group["value"].mean()
        device_std = group["value"].std()
        
        mask = compute_anomalies(group["value"])
        anomaly_points = group[mask]
        count = len(anomaly_points)
        
        if count > 0:
            # 分析异常原因
            anomaly_mean = anomaly_points["value"].mean()
            high_anomalies = len(anomaly_points[anomaly_points["value"] > device_mean + 2 * device_std])
            low_anomalies = len(anomaly_points[anomaly_points["value"] < device_mean - 2 * device_std])
            
            anomaly_records.append({
                "component_id": component_id,
                "anomaly_count": count,
                "total_count": len(group),
                "anomaly_ratio": count / len(group) * 100,
                "device_mean": device_mean,
                "device_std": device_std,
                "high_anomalies": high_anomalies,
                "low_anomalies": low_anomalies,
            })

    if not anomaly_records:
        st.success("未发现明显异常。")
        return

    anomaly_df = pd.DataFrame(anomaly_records).sort_values("anomaly_count", ascending=False)
    
    st.markdown("### 📋 异常统计")
    st.dataframe(anomaly_df[["component_id", "anomaly_count", "total_count", "anomaly_ratio"]].rename(columns={
        "component_id": "设备",
        "anomaly_count": "异常点数",
        "total_count": "总记录数",
        "anomaly_ratio": "异常比例(%)"
    }), width="stretch")

    # 显示具体异常
    st.markdown("### 🔍 异常详情")
    selected_device = st.selectbox("选择设备查看详情", anomaly_df["component_id"].tolist())
    
    # 获取选中设备的信息
    device_info = anomaly_df[anomaly_df["component_id"] == selected_device].iloc[0]
    
    # 显示异常原因分析
    st.markdown("#### 📊 异常原因分析")
    col1, col2, col3 = st.columns(3)
    col1.metric("设备平均值", f"{device_info['device_mean']:.2f}")
    col2.metric("偏高异常", f"{int(device_info['high_anomalies'])} 个")
    col3.metric("偏低异常", f"{int(device_info['low_anomalies'])} 个")
    
    # 判断异常类型并给出解释
    high_ratio = device_info['high_anomalies'] / device_info['anomaly_count'] if device_info['anomaly_count'] > 0 else 0
    
    if high_ratio > 0.7:
        st.info(f"🔺 **主要为偏高异常** ({high_ratio*100:.1f}%): 可能原因 - 设备高负载运行、传感器漂移偏高、启动瞬间冲击")
    elif high_ratio < 0.3:
        st.info(f"🔻 **主要为偏低异常** ({(1-high_ratio)*100:.1f}%): 可能原因 - 设备停机/待机、传感器故障、负载骤降")
    else:
        st.info(f"↕️ **高低异常均有**: 可能原因 - 正常波动较大、控制策略切换、工况变化频繁")

    # 显示异常数据点详情
    device_data = focused[focused["component_id"] == selected_device].sort_values("timestamp")
    mask = compute_anomalies(device_data["value"])
    anomalies = device_data[mask].copy()
    
    # 计算每个异常点的偏离程度
    anomalies["偏离均值"] = anomalies["value"] - device_info['device_mean']
    anomalies["偏离方向"] = anomalies["偏离均值"].apply(lambda x: "偏高 🔺" if x > 0 else "偏低 🔻")
    anomalies["偏离倍数(σ)"] = abs(anomalies["偏离均值"]) / device_info['device_std'] if device_info['device_std'] > 0 else 0
    
    st.markdown("#### 📋 异常数据点")
    display_cols = ["timestamp", "value", "偏离方向", "偏离倍数(σ)", "metric_name"]
    st.dataframe(anomalies[display_cols].head(50).rename(columns={
        "timestamp": "时间",
        "value": "数值",
        "metric_name": "指标名称"
    }), width="stretch")
    
    # 可视化：显示数据趋势和异常点
    st.markdown("#### 📈 数据趋势与异常点")
    import plotly.graph_objects as go
    
    fig = go.Figure()
    
    # 正常数据
    normal_data = device_data[~mask]
    fig.add_trace(go.Scatter(
        x=normal_data["timestamp"],
        y=normal_data["value"],
        mode='lines',
        name='正常数据',
        line=dict(color='blue', width=1),
        opacity=0.6
    ))
    
    # 异常点
    fig.add_trace(go.Scatter(
        x=anomalies["timestamp"],
        y=anomalies["value"],
        mode='markers',
        name='异常点',
        marker=dict(color='red', size=8, symbol='x')
    ))
    
    # 均值线
    fig.add_hline(y=device_info['device_mean'], line_dash="dash", 
                  annotation_text=f"均值: {device_info['device_mean']:.2f}")
    
    fig.update_layout(
        title=f"{selected_device} - {metric_category} 趋势图",
        xaxis_title="时间",
        yaxis_title="数值",
        height=400
    )
    
    st.plotly_chart(fig, width="stretch")


# ============================================================================
# 页面渲染 - 数据质量
# ============================================================================


def render_data_quality(component_df: pd.DataFrame, energy_df: pd.DataFrame) -> None:
    """数据质量页面"""
    st.subheader("📋 数据质量")
    
    if component_df.empty and energy_df.empty:
        st.info("暂无数据。")
        return

    # 数据完整性
    st.markdown("### 📊 数据完整性")
    
    def build_quality_table(data: pd.DataFrame, label: str) -> pd.DataFrame:
        if data.empty:
            return pd.DataFrame()
        table = data.groupby("component_id").agg({
            "timestamp": ["min", "max", "count"],
            "value": ["mean", "std"]
        }).reset_index()
        table.columns = ["设备", "开始时间", "结束时间", "记录数", "平均值", "标准差"]
        table["数据类型"] = label
        return table

    tables = [
        build_quality_table(component_df, "组件数据"),
        build_quality_table(energy_df, "能耗数据")
    ]
    tables = [t for t in tables if not t.empty]
    
    if tables:
        combined = pd.concat(tables, ignore_index=True)
        st.dataframe(combined, width="stretch")

    # 缺失目录检查
    st.markdown("### 📁 目录检查")
    for key, rel_path in DATA_DIRS.items():
        abs_path = resolve_path(rel_path)
        if abs_path.exists():
            file_count = len(list(abs_path.rglob("*.xlsx")))
            st.success(f"✅ {key}: {rel_path} ({file_count} 个文件)")
        else:
            st.error(f"❌ {key}: {rel_path} (目录不存在)")


# ============================================================================
# 主函数
# ============================================================================


def main() -> None:
    st.set_page_config(page_title="冷却系统监控", layout="wide")
    st.title("🏭 冷却系统运行监控")

    with st.sidebar:
        st.markdown("### 📁 数据目录")
        for key, rel_path in DATA_DIRS.items():
            abs_path = resolve_path(rel_path)
            status = "✅" if abs_path.exists() else "❌"
            st.text(f"{status} {key}")

        st.divider()
        st.markdown("### 🔄 缓存管理")
        rebuild_cache = st.button("重建缓存")

        st.divider()
        page = st.radio("📄 页面", [
            "系统总览",
            "组件分析", 
            "效率分析",
            "稳定性分析",
            "异常检测",
            "数据质量"
        ], index=0)

    # 加载数据
    if rebuild_cache:
        read_component_file_cached.clear()
        read_energy_file_cached.clear()
    if "component_df" not in st.session_state or rebuild_cache:
        st.session_state.load_progress = st.progress(0, text="正在准备加载...")
        component_df, energy_df = load_all_data(True)
        st.session_state.load_progress.progress(100, text="加载完成")
        st.session_state.load_progress.empty()
        st.session_state.component_df = component_df
        st.session_state.energy_df = energy_df
    
    component_df = st.session_state.component_df
    energy_df = st.session_state.energy_df

    # 时间筛选
    all_timestamps = []
    if not component_df.empty:
        all_timestamps.extend([component_df["timestamp"].min(), component_df["timestamp"].max()])
    if not energy_df.empty:
        all_timestamps.extend([energy_df["timestamp"].min(), energy_df["timestamp"].max()])

    if all_timestamps:
        start = min(all_timestamps)
        end = max(all_timestamps)
        
        with st.sidebar:
            st.divider()
            st.markdown("### 📅 时间范围")
            date_range = st.date_input("选择日期", value=(start.date(), end.date()))
        
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date = end_date = date_range if isinstance(date_range, dt_date) else start.date()
        
        start_dt = datetime.combine(start_date, datetime.min.time())
        end_dt = datetime.combine(end_date, datetime.max.time())
        component_df = filter_by_time(component_df, start_dt, end_dt)
        energy_df = filter_by_time(energy_df, start_dt, end_dt)

    # 渲染页面
    if page == "系统总览":
        render_system_overview(component_df, energy_df)
    elif page == "组件分析":
        render_component_analysis(component_df, energy_df)
    elif page == "效率分析":
        render_efficiency_analysis(component_df, energy_df)
    elif page == "稳定性分析":
        render_stability_analysis(component_df)
    elif page == "异常检测":
        render_anomaly_detection(component_df)
    else:
        render_data_quality(component_df, energy_df)


if __name__ == "__main__":
    main()
