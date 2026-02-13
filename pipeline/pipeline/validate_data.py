#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据校验脚本 - 验证数据库导入数据与原始Excel文件的一致性

用法:
    python validate_data.py [--config CONFIG_FILE] [--output OUTPUT_DIR]

功能:
    1. 检查数据文件数量
    2. 检查设备注册数量
    3. 检查时间范围
    4. 检查数据分布
    5. 生成校验报告
"""

import os
import sys
import json
import argparse
import random
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

import pymysql
import pandas as pd
from pymysql.cursors import DictCursor


# ============================================================================
# 配置
# ============================================================================

DEFAULT_CONFIG = {
    "db": {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "3306")),
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASSWORD", ""),
        "database": os.getenv("DB_NAME", "cooling_system_v2"),
        "charset": "utf8mb4"
    },
    "paths": {
        "excel_tree": "excel_tree.txt",
        "chiller_params": "设备参数/冷机编号型号额定参数.xlsx",
        "pump_params": "设备参数/水泵编号型号额定参数.xlsx",
        "tower_params": "设备参数/冷却塔编号型号额定参数.xlsx"
    },
    "output_dir": "../../../sql"
}


# ============================================================================
# 数据库连接
# ============================================================================

def get_connection(config: Dict) -> pymysql.Connection:
    """创建数据库连接"""
    return pymysql.connect(
        host=config["db"]["host"],
        port=config["db"]["port"],
        user=config["db"]["user"],
        password=config["db"]["password"],
        database=config["db"]["database"],
        charset=config["db"]["charset"],
        cursorclass=DictCursor
    )


# ============================================================================
# 校验函数
# ============================================================================

def check_file_count(conn: pymysql.Connection, excel_tree_path: str) -> Dict:
    """检查数据文件数量"""
    result = {
        "name": "数据文件数量检查",
        "status": "unknown",
        "details": {}
    }

    # 读取excel_tree.txt
    if not os.path.exists(excel_tree_path):
        result["status"] = "error"
        result["details"]["error"] = f"文件不存在: {excel_tree_path}"
        return result

    with open(excel_tree_path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    # 统计数据文件和参数文件
    data_files = [l for l in lines if not l.startswith("设备参数/")]
    param_files = [l for l in lines if l.startswith("设备参数/")]

    result["details"]["excel_total"] = len(lines)
    result["details"]["excel_data_files"] = len(data_files)
    result["details"]["excel_param_files"] = len(param_files)

    # 查询数据库中的文件数量
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(DISTINCT source_file) AS cnt
            FROM raw_measurement
        """)
        row = cursor.fetchone()
        db_file_count = row["cnt"] if row else 0

    result["details"]["db_file_count"] = db_file_count
    result["details"]["difference"] = len(data_files) - db_file_count

    # 判断状态
    if db_file_count == len(data_files):
        result["status"] = "pass"
        result["message"] = f"文件数量匹配: {db_file_count}/{len(data_files)}"
    elif db_file_count > 0:
        result["status"] = "warning"
        result["message"] = f"文件数量差异: DB={db_file_count}, Excel数据文件={len(data_files)}"
    else:
        result["status"] = "fail"
        result["message"] = "数据库中没有数据文件"

    return result


def check_equipment_count(conn: pymysql.Connection, config: Dict) -> Dict:
    """检查设备注册数量"""
    result = {
        "name": "设备注册数量检查",
        "status": "unknown",
        "details": {}
    }

    # 查询数据库中的设备数量
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT equipment_type, COUNT(*) AS cnt
            FROM equipment_registry
            GROUP BY equipment_type
            ORDER BY equipment_type
        """)
        db_counts = {row["equipment_type"]: row["cnt"] for row in cursor.fetchall()}

    result["details"]["db_counts"] = db_counts
    result["details"]["db_total"] = sum(db_counts.values())

    # 读取Excel参数文件
    excel_counts = {}
    base_path = os.path.dirname(config["paths"]["excel_tree"])

    # 冷机
    chiller_path = os.path.join(base_path, config["paths"]["chiller_params"])
    if os.path.exists(chiller_path):
        df = pd.read_excel(chiller_path)
        excel_counts["chiller"] = len(df)

    # 水泵
    pump_path = os.path.join(base_path, config["paths"]["pump_params"])
    if os.path.exists(pump_path):
        df = pd.read_excel(pump_path)
        excel_counts["pump_total"] = len(df)

    # 冷却塔
    tower_path = os.path.join(base_path, config["paths"]["tower_params"])
    if os.path.exists(tower_path):
        df = pd.read_excel(tower_path)
        # 排除合计行和空行
        df_clean = df.dropna(subset=[df.columns[2]])  # 冷塔编号列
        df_clean = df_clean[~df_clean.iloc[:, 2].astype(str).str.contains("合计|冷塔编号", na=False)]
        excel_counts["cooling_tower_total"] = len(df_clean)

    result["details"]["excel_counts"] = excel_counts

    # 比较
    comparisons = []

    # 冷机比较
    if "chiller" in excel_counts:
        db_chiller = db_counts.get("chiller", 0)
        match = db_chiller == excel_counts["chiller"]
        comparisons.append({
            "type": "冷机",
            "excel": excel_counts["chiller"],
            "db": db_chiller,
            "match": match
        })

    # 水泵比较 (所有泵类型之和)
    if "pump_total" in excel_counts:
        pump_types = ["chilled_pump", "cooling_pump", "closed_tower_pump",
                      "user_side_pump", "source_side_pump",
                      "heat_recovery_primary_pump", "heat_recovery_secondary_pump",
                      "fire_pump", "unknown_pump"]
        db_pump_total = sum(db_counts.get(t, 0) for t in pump_types)
        match = db_pump_total == excel_counts["pump_total"]
        comparisons.append({
            "type": "水泵",
            "excel": excel_counts["pump_total"],
            "db": db_pump_total,
            "match": match
        })

    # 冷却塔比较 (注意: 只有有数据的冷却塔会被注册)
    if "cooling_tower_total" in excel_counts:
        tower_types = ["cooling_tower", "cooling_tower_closed"]
        db_tower_total = sum(db_counts.get(t, 0) for t in tower_types)
        comparisons.append({
            "type": "冷却塔",
            "excel": excel_counts["cooling_tower_total"],
            "db": db_tower_total,
            "match": False,  # 冷却塔通常不完全匹配
            "note": "只有有监测数据的冷却塔会被注册"
        })

    result["details"]["comparisons"] = comparisons

    # 检查是否有unknown_pump
    unknown_count = db_counts.get("unknown_pump", 0)
    result["details"]["unknown_pump_count"] = unknown_count

    # 判断状态
    all_match = all(c["match"] for c in comparisons if "note" not in c)
    if all_match and unknown_count == 0:
        result["status"] = "pass"
        result["message"] = "设备数量匹配，无未知类型"
    elif all_match and unknown_count > 0:
        result["status"] = "warning"
        result["message"] = f"设备数量匹配，但有 {unknown_count} 个未知泵类型需人工分类"
    else:
        result["status"] = "warning"
        result["message"] = "部分设备数量不匹配（可能是正常的）"

    return result


def check_time_range(conn: pymysql.Connection) -> Dict:
    """检查数据时间范围"""
    result = {
        "name": "数据时间范围检查",
        "status": "unknown",
        "details": {}
    }

    with conn.cursor() as cursor:
        # raw_measurement 时间范围
        cursor.execute("""
            SELECT
                MIN(record_time) AS earliest,
                MAX(record_time) AS latest,
                COUNT(*) AS total_records
            FROM raw_measurement
        """)
        raw_row = cursor.fetchone()

        # agg_hour 时间范围
        cursor.execute("""
            SELECT
                MIN(bucket_time) AS earliest,
                MAX(bucket_time) AS latest,
                COUNT(*) AS total_records
            FROM agg_hour
        """)
        hour_row = cursor.fetchone()

        # agg_day 时间范围
        cursor.execute("""
            SELECT
                MIN(bucket_time) AS earliest,
                MAX(bucket_time) AS latest,
                COUNT(*) AS total_records
            FROM agg_day
        """)
        day_row = cursor.fetchone()

    result["details"]["raw_measurement"] = {
        "earliest": str(raw_row["earliest"]) if raw_row["earliest"] else None,
        "latest": str(raw_row["latest"]) if raw_row["latest"] else None,
        "total_records": raw_row["total_records"]
    }

    result["details"]["agg_hour"] = {
        "earliest": str(hour_row["earliest"]) if hour_row["earliest"] else None,
        "latest": str(hour_row["latest"]) if hour_row["latest"] else None,
        "total_records": hour_row["total_records"]
    }

    result["details"]["agg_day"] = {
        "earliest": str(day_row["earliest"]) if day_row["earliest"] else None,
        "latest": str(day_row["latest"]) if day_row["latest"] else None,
        "total_records": day_row["total_records"]
    }

    # 判断状态
    if raw_row["total_records"] > 0 and hour_row["total_records"] > 0:
        result["status"] = "pass"
        result["message"] = (
            f"数据范围: {raw_row['earliest']} ~ {raw_row['latest']}, "
            f"共 {raw_row['total_records']:,} 条原始记录"
        )
    elif raw_row["total_records"] > 0:
        result["status"] = "warning"
        result["message"] = "有原始数据但聚合数据为空"
    else:
        result["status"] = "fail"
        result["message"] = "没有数据"

    return result


def check_data_distribution(conn: pymysql.Connection) -> Dict:
    """检查数据分布"""
    result = {
        "name": "数据分布检查",
        "status": "unknown",
        "details": {}
    }

    with conn.cursor() as cursor:
        # 按系统前缀统计
        cursor.execute("""
            SELECT
                SUBSTRING_INDEX(source_file, '/', 1) AS category,
                COUNT(*) AS record_count
            FROM raw_measurement
            GROUP BY category
            ORDER BY record_count DESC
        """)
        by_category = [dict(row) for row in cursor.fetchall()]

        # 按设备类型统计
        cursor.execute("""
            SELECT
                equipment_type,
                COUNT(*) AS record_count
            FROM agg_hour
            GROUP BY equipment_type
            ORDER BY record_count DESC
        """)
        by_equipment = [dict(row) for row in cursor.fetchall()]

        # 按指标类型统计
        cursor.execute("""
            SELECT
                metric_name,
                COUNT(*) AS record_count
            FROM agg_hour
            GROUP BY metric_name
            ORDER BY record_count DESC
        """)
        by_metric = [dict(row) for row in cursor.fetchall()]

    result["details"]["by_category"] = by_category
    result["details"]["by_equipment_type"] = by_equipment
    result["details"]["by_metric"] = by_metric

    # 判断状态
    if len(by_category) > 0 and len(by_equipment) > 0:
        result["status"] = "pass"
        result["message"] = f"数据分布正常: {len(by_category)} 个类别, {len(by_equipment)} 种设备类型"
    else:
        result["status"] = "warning"
        result["message"] = "数据分布异常"

    return result


def check_data_quality(conn: pymysql.Connection) -> Dict:
    """检查数据质量"""
    result = {
        "name": "数据质量检查",
        "status": "unknown",
        "details": {}
    }

    with conn.cursor() as cursor:
        # 检查空值比例
        cursor.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS null_count,
                SUM(CASE WHEN value = 0 THEN 1 ELSE 0 END) AS zero_count
            FROM raw_measurement
        """)
        quality_row = cursor.fetchone()

        # 检查异常值 (负数)
        cursor.execute("""
            SELECT COUNT(*) AS negative_count
            FROM raw_measurement
            WHERE value < 0
        """)
        negative_row = cursor.fetchone()

    total = quality_row["total"]
    null_count = quality_row["null_count"]
    zero_count = quality_row["zero_count"]
    negative_count = negative_row["negative_count"]

    result["details"]["total_records"] = total
    result["details"]["null_count"] = null_count
    result["details"]["null_ratio"] = f"{null_count/total*100:.2f}%" if total > 0 else "N/A"
    result["details"]["zero_count"] = zero_count
    result["details"]["zero_ratio"] = f"{zero_count/total*100:.2f}%" if total > 0 else "N/A"
    result["details"]["negative_count"] = negative_count

    # 判断状态
    null_ratio = null_count / total if total > 0 else 0
    if null_ratio < 0.01 and negative_count == 0:
        result["status"] = "pass"
        result["message"] = f"数据质量良好: 空值率 {null_ratio*100:.2f}%, 无负数"
    elif null_ratio < 0.05:
        result["status"] = "warning"
        result["message"] = f"数据质量一般: 空值率 {null_ratio*100:.2f}%"
    else:
        result["status"] = "fail"
        result["message"] = f"数据质量较差: 空值率 {null_ratio*100:.2f}%"

    return result


# ============================================================================
# 报告生成
# ============================================================================

def generate_report(results: List[Dict], output_dir: str) -> str:
    """生成校验报告"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(output_dir, f"validation_report_{timestamp}.md")

    os.makedirs(output_dir, exist_ok=True)

    lines = []
    lines.append("# 数据校验报告")
    lines.append("")
    lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    # 汇总
    lines.append("## 校验汇总")
    lines.append("")
    lines.append("| 检查项 | 状态 | 说明 |")
    lines.append("|--------|------|------|")

    status_icons = {
        "pass": "✅",
        "warning": "⚠️",
        "fail": "❌",
        "error": "🔴",
        "unknown": "❓"
    }

    for r in results:
        icon = status_icons.get(r["status"], "❓")
        msg = r.get("message", "")
        lines.append(f"| {r['name']} | {icon} {r['status']} | {msg} |")

    lines.append("")

    # 详细信息
    lines.append("## 详细信息")
    lines.append("")

    for r in results:
        lines.append(f"### {r['name']}")
        lines.append("")
        lines.append(f"**状态**: {status_icons.get(r['status'], '❓')} {r['status']}")
        lines.append("")

        if "message" in r:
            lines.append(f"**说明**: {r['message']}")
            lines.append("")

        if "details" in r:
            lines.append("**详情**:")
            lines.append("")
            lines.append("```json")
            lines.append(json.dumps(r["details"], ensure_ascii=False, indent=2, default=str))
            lines.append("```")
            lines.append("")

    # 写入文件
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    return report_path


def save_results_json(results: List[Dict], output_dir: str) -> str:
    """保存JSON格式结果"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = os.path.join(output_dir, f"validation_results_{timestamp}.json")

    os.makedirs(output_dir, exist_ok=True)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)

    return json_path


def _apply_db_env(config: Dict[str, Any]) -> None:
    """Apply DB config to environment for tools that read DB_* vars."""
    db_cfg = config.get("db", {})
    os.environ["DB_HOST"] = str(db_cfg.get("host", "localhost"))
    os.environ["DB_PORT"] = str(db_cfg.get("port", 3306))
    os.environ["DB_USER"] = str(db_cfg.get("user", "root"))
    os.environ["DB_PASSWORD"] = str(db_cfg.get("password", ""))
    os.environ["DB_NAME"] = str(db_cfg.get("database", "cooling_system_v2"))


def _resolve_runtime_path(path_value: str) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return (Path.cwd() / path).resolve()


def _parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _count_tag_rows_spotcheck(file_path: Path) -> int:
    df = pd.read_excel(file_path)
    columns = [str(col).strip() for col in df.columns]
    df.columns = columns

    aliases = {
        "tag_name": ["点名", "tag_name"],
        "ts": ["采集时间", "ts", "collect_time"],
        "value": ["采集值", "value", "collect_value"],
    }

    def pick_column(candidates: List[str]) -> Optional[str]:
        for name in candidates:
            if name in columns:
                return name
        return None

    tag_col = pick_column(aliases["tag_name"])
    ts_col = pick_column(aliases["ts"])
    value_col = pick_column(aliases["value"])

    if not all([tag_col, ts_col, value_col]):
        if len(columns) < 3:
            return 0
        tag_col, ts_col, value_col = columns[:3]

    tag_df = df[[tag_col, ts_col, value_col]].copy()
    tag_df.columns = ["tag_name", "ts", "value"]
    tag_df["ts"] = pd.to_datetime(tag_df["ts"], errors="coerce")
    tag_df["value"] = pd.to_numeric(tag_df["value"], errors="coerce")
    tag_df = tag_df.dropna(subset=["ts", "value", "tag_name"])
    return int(len(tag_df))


def _count_device_rows_spotcheck(file_path: Path) -> int:
    df = pd.read_excel(file_path, header=None)
    if df.shape[0] < 4:
        return 0
    data_df = df.iloc[3:].copy()
    if data_df.shape[1] < 3:
        return 0
    data_df = data_df.iloc[:, [1, 2]]
    data_df.columns = ["ts", "value"]
    data_df = data_df.dropna(subset=["ts"])
    data_df["ts"] = pd.to_datetime(data_df["ts"], errors="coerce")
    data_df["value"] = pd.to_numeric(data_df["value"], errors="coerce")
    data_df = data_df.dropna(subset=["ts", "value"])
    return int(len(data_df))


def run_metric_spotcheck(
    config: Dict[str, Any],
    time_start: datetime,
    time_end: datetime,
    metric_names: List[str],
) -> Dict[str, Any]:
    """Spot-check selected metrics via backend metric calculator."""
    _apply_db_env(config)

    src_root = (Path(__file__).resolve().parents[2] / "src").resolve()
    if str(src_root) not in sys.path:
        sys.path.insert(0, str(src_root))

    from carbon_metrics.backend.services.metric_calculator import MetricCalculator

    calculator = MetricCalculator()
    rows: List[Dict[str, Any]] = []
    status_counter: Counter = Counter()

    print("metric_name,status,value,unit,input_records,valid_records,issue_types")
    for metric_name in metric_names:
        result = calculator.calculate(
            metric_name=metric_name,
            time_start=time_start,
            time_end=time_end,
        )
        issue_types = sorted({
            issue.get("type")
            for issue in (result.quality_issues or [])
            if isinstance(issue, dict) and issue.get("type")
        })
        status_counter[result.status] += 1
        row = {
            "metric_name": metric_name,
            "status": result.status,
            "value": result.value,
            "unit": result.unit,
            "input_records": int(result.input_records or 0),
            "valid_records": int(result.valid_records or 0),
            "issue_types": issue_types,
        }
        rows.append(row)
        print(
            f"{metric_name},{result.status},{result.value},{result.unit},"
            f"{row['input_records']},{row['valid_records']},{'|'.join(issue_types)}"
        )

    print("\nsummary_status_count")
    for status in ("success", "partial", "no_data", "failed"):
        print(f"{status},{status_counter.get(status, 0)}")

    if status_counter.get("failed", 0) > 0:
        status = "fail"
        message = "metric spotcheck has failed metrics"
    elif status_counter.get("no_data", 0) > 0:
        status = "warning"
        message = "metric spotcheck has no_data metrics"
    else:
        status = "pass"
        message = "metric spotcheck passed"

    return {
        "name": "metric_spotcheck",
        "status": status,
        "message": message,
        "details": {
            "time_start": str(time_start),
            "time_end": str(time_end),
            "metric_names": metric_names,
            "status_count": dict(status_counter),
            "items": rows,
        },
    }


def run_backfill_alignment_spotcheck(
    conn: pymysql.Connection,
    energy_dir: Path,
    params_dir: Path,
    sample_files_per_type: int,
    random_seed: int,
) -> Dict[str, Any]:
    """Spot-check alignment between local Excel rows and raw_measurement."""
    from pipeline.ingest import load_source_config, match_source_files

    rng = random.Random(random_seed)
    candidates: Dict[str, Dict[str, List[Path]]] = {
        "tag": defaultdict(list),
        "device": defaultdict(list),
    }

    configs = load_source_config(conn)
    for config in configs:
        if config.schema_type not in {"tag", "device"}:
            continue
        matched_files = match_source_files(config, energy_dir=energy_dir, params_dir=params_dir)
        for file_path in matched_files:
            candidates[config.schema_type][file_path.name].append(file_path)

    sampled_rows: List[Dict[str, Any]] = []
    mismatch_count = 0
    sample_plan: Dict[str, int] = {}

    print("source_type,source_file,expected_rows,db_rows,diff")
    with conn.cursor() as cursor:
        for source_type in ("tag", "device"):
            source_files = list(candidates[source_type].keys())
            if not source_files:
                sample_plan[source_type] = 0
                continue

            pick_count = min(max(sample_files_per_type, 0), len(source_files))
            sample_plan[source_type] = pick_count
            picked_files = rng.sample(source_files, pick_count)

            for file_name in picked_files:
                expected_rows = 0
                for file_path in candidates[source_type][file_name]:
                    if source_type == "tag":
                        expected_rows += _count_tag_rows_spotcheck(file_path)
                    else:
                        expected_rows += _count_device_rows_spotcheck(file_path)

                cursor.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM raw_measurement
                    WHERE source_type = %s AND source_file = %s
                    """,
                    (source_type, file_name),
                )
                db_rows = int((cursor.fetchone() or {}).get("cnt", 0))
                diff = db_rows - expected_rows
                if diff != 0:
                    mismatch_count += 1

                sampled_rows.append({
                    "source_type": source_type,
                    "source_file": file_name,
                    "expected_rows": expected_rows,
                    "db_rows": db_rows,
                    "diff": diff,
                })
                print(f"{source_type},{file_name},{expected_rows},{db_rows},{diff}")

    print(f"\nspotcheck_mismatch_count={mismatch_count}")

    if mismatch_count > 0:
        status = "fail"
        message = "backfill alignment spotcheck found mismatches"
    else:
        status = "pass"
        message = "backfill alignment spotcheck passed"

    return {
        "name": "backfill_alignment_spotcheck",
        "status": status,
        "message": message,
        "details": {
            "energy_dir": str(energy_dir),
            "params_dir": str(params_dir),
            "sample_files_per_type": sample_files_per_type,
            "random_seed": random_seed,
            "sample_plan": sample_plan,
            "mismatch_count": mismatch_count,
            "items": sampled_rows,
        },
    }


# ============================================================================
# 主函数
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="数据校验脚本")
    parser.add_argument("--config", type=str, help="配置文件路径")
    parser.add_argument("--output", type=str, help="输出目录")
    parser.add_argument("--json", action="store_true", help="同时输出JSON格式")
    parser.add_argument("--spotcheck", action="store_true", help="Run metric/data alignment spotcheck")
    parser.add_argument("--spotcheck-only", action="store_true", help="Run only spotcheck and skip full validation")
    parser.add_argument(
        "--spotcheck-metrics",
        type=str,
        default="冷冻水回水温度,冷却水供水温度,冷冻水流量,冷却水流量,冷却水温差,制冷量",
        help="Comma-separated metric names for metric spotcheck",
    )
    parser.add_argument(
        "--spotcheck-time-start",
        type=str,
        default="2025-07-01T00:00:00",
        help="Start time for metric spotcheck in ISO format",
    )
    parser.add_argument(
        "--spotcheck-time-end",
        type=str,
        default="2026-01-21T00:00:00",
        help="End time for metric spotcheck in ISO format",
    )
    parser.add_argument(
        "--spotcheck-energy-dir",
        type=str,
        default="data1",
        help="Energy source directory for alignment spotcheck",
    )
    parser.add_argument(
        "--spotcheck-params-dir",
        type=str,
        default="data1",
        help="Params directory for alignment spotcheck",
    )
    parser.add_argument(
        "--spotcheck-files-per-type",
        type=int,
        default=12,
        help="Sample file count per source_type(tag/device) for alignment spotcheck",
    )
    parser.add_argument(
        "--spotcheck-seed",
        type=int,
        default=42,
        help="Random seed for alignment spotcheck sampling",
    )
    args = parser.parse_args()

    # 加载配置
    config = DEFAULT_CONFIG.copy()
    if args.config and os.path.exists(args.config):
        with open(args.config, "r", encoding="utf-8") as f:
            user_config = json.load(f)
            config.update(user_config)

    # 设置输出目录
    output_dir = args.output or config["output_dir"]

    metric_names = [name.strip() for name in args.spotcheck_metrics.split(",") if name.strip()]
    if not metric_names:
        metric_names = ["冷冻水回水温度", "冷却水供水温度", "冷冻水流量", "冷却水流量", "冷却水温差", "制冷量"]
    try:
        spotcheck_time_start = _parse_iso_datetime(args.spotcheck_time_start)
        spotcheck_time_end = _parse_iso_datetime(args.spotcheck_time_end)
    except ValueError as exc:
        print(f"spotcheck time format invalid: {exc}")
        sys.exit(2)
    if spotcheck_time_start >= spotcheck_time_end:
        print("spotcheck_time_start must be earlier than spotcheck_time_end")
        sys.exit(2)

    spotcheck_energy_dir = _resolve_runtime_path(args.spotcheck_energy_dir)
    spotcheck_params_dir = _resolve_runtime_path(args.spotcheck_params_dir)

    # 设置路径
    script_dir = os.path.dirname(os.path.abspath(__file__))
    excel_tree_path = os.path.join(script_dir, config["paths"]["excel_tree"])

    print("=" * 60)
    print("数据校验开始")
    print("=" * 60)
    print()

    # 连接数据库
    try:
        conn = get_connection(config)
        print(f"✓ 数据库连接成功: {config['db']['database']}")
    except Exception as e:
        print(f"✗ 数据库连接失败: {e}")
        sys.exit(1)

    print()

    # 执行校验
    results = []

    if args.spotcheck_only:
        print("=== metric_spotcheck ===")
        metric_spotcheck_result = run_metric_spotcheck(
            config=config,
            time_start=spotcheck_time_start,
            time_end=spotcheck_time_end,
            metric_names=metric_names,
        )
        results.append(metric_spotcheck_result)

        print("\n=== data_alignment_spotcheck ===")
        data_spotcheck_result = run_backfill_alignment_spotcheck(
            conn=conn,
            energy_dir=spotcheck_energy_dir,
            params_dir=spotcheck_params_dir,
            sample_files_per_type=args.spotcheck_files_per_type,
            random_seed=args.spotcheck_seed,
        )
        results.append(data_spotcheck_result)

        conn.close()
        fail_count = sum(1 for r in results if r["status"] == "fail")
        warn_count = sum(1 for r in results if r["status"] == "warning")
        if fail_count > 0:
            sys.exit(1)
        if warn_count > 0:
            sys.exit(2)
        return

    # 1. 文件数量检查
    print("1. 检查数据文件数量...")
    r = check_file_count(conn, excel_tree_path)
    results.append(r)
    print(f"   {r['status']}: {r.get('message', '')}")

    # 2. 设备数量检查
    print("2. 检查设备注册数量...")
    config["paths"]["excel_tree"] = excel_tree_path
    r = check_equipment_count(conn, config)
    results.append(r)
    print(f"   {r['status']}: {r.get('message', '')}")

    # 3. 时间范围检查
    print("3. 检查数据时间范围...")
    r = check_time_range(conn)
    results.append(r)
    print(f"   {r['status']}: {r.get('message', '')}")

    # 4. 数据分布检查
    print("4. 检查数据分布...")
    r = check_data_distribution(conn)
    results.append(r)
    print(f"   {r['status']}: {r.get('message', '')}")

    # 5. 数据质量检查
    print("5. 检查数据质量...")
    r = check_data_quality(conn)
    results.append(r)
    print(f"   {r['status']}: {r.get('message', '')}")

    # 关闭连接
    if args.spotcheck:
        print("\n=== metric_spotcheck ===")
        metric_spotcheck_result = run_metric_spotcheck(
            config=config,
            time_start=spotcheck_time_start,
            time_end=spotcheck_time_end,
            metric_names=metric_names,
        )
        results.append(metric_spotcheck_result)

        print("\n=== data_alignment_spotcheck ===")
        data_spotcheck_result = run_backfill_alignment_spotcheck(
            conn=conn,
            energy_dir=spotcheck_energy_dir,
            params_dir=spotcheck_params_dir,
            sample_files_per_type=args.spotcheck_files_per_type,
            random_seed=args.spotcheck_seed,
        )
        results.append(data_spotcheck_result)

    conn.close()

    print()
    print("=" * 60)
    print("生成报告")
    print("=" * 60)

    # 生成报告
    report_path = generate_report(results, output_dir)
    print(f"✓ Markdown报告: {report_path}")

    if args.json:
        json_path = save_results_json(results, output_dir)
        print(f"✓ JSON结果: {json_path}")

    print()

    # 汇总
    pass_count = sum(1 for r in results if r["status"] == "pass")
    warn_count = sum(1 for r in results if r["status"] == "warning")
    fail_count = sum(1 for r in results if r["status"] == "fail")

    print("=" * 60)
    print("校验汇总")
    print("=" * 60)
    print(f"  ✅ 通过: {pass_count}")
    print(f"  ⚠️ 警告: {warn_count}")
    print(f"  ❌ 失败: {fail_count}")
    print()

    if fail_count > 0:
        print("⚠️ 存在失败项，请检查报告详情")
        sys.exit(1)
    elif warn_count > 0:
        print("✓ 校验完成，存在警告项")
    else:
        print("✓ 校验全部通过")


if __name__ == "__main__":
    main()
