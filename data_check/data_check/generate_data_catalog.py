from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DATA_ROOT = ROOT_DIR / "data1"
OUTPUT_JSON = ROOT_DIR / "docs" / "data_catalog.json"
OUTPUT_MD = ROOT_DIR / "docs" / "data_catalog.md"
OUTPUT_JSONL = ROOT_DIR / "docs" / "data_catalog_files.jsonl"


def is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def normalize_header(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value).strip()
    return text


def count_non_empty(values: list[Any]) -> int:
    return sum(1 for value in values if not is_blank(value))


def count_text_like(values: list[Any]) -> int:
    count = 0
    for value in values:
        if isinstance(value, str) and value.strip() != "":
            count += 1
    return count


def select_header_row(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    row0 = df.iloc[0].tolist()
    row1 = df.iloc[1].tolist() if len(df) > 1 else []
    row0_non_empty = count_non_empty(row0)
    row1_text = count_text_like(row1)
    if row0_non_empty <= 1 and row1_text >= 2:
        return 1
    return 0


def is_datetime_string(value: str) -> bool:
    text = value.strip()
    if not text:
        return False
    if any(token in text for token in ["-", "/", ":"]):
        try:
            pd.to_datetime(text, errors="raise")
            return True
        except Exception:
            return False
    return False


def normalize_cell(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return float(value)
    return value


def infer_first_column_type(name: str, values: list[Any]) -> str:
    if "序号" in name:
        return "index"
    non_empty = [value for value in values if value is not None]
    if not non_empty:
        return "unknown"
    if all(isinstance(value, (int, float)) for value in non_empty):
        return "index"
    for value in non_empty:
        if isinstance(value, (datetime, pd.Timestamp)):
            return "time"
        if isinstance(value, str) and is_datetime_string(value):
            return "time"
    return "text"


def infer_column_type(name: str, values: list[Any]) -> str:
    if "时间" in name:
        return "time"
    non_empty = [value for value in values if value is not None]
    if not non_empty:
        return "unknown"
    if all(isinstance(value, (int, float)) for value in non_empty):
        return "number"
    for value in non_empty:
        if isinstance(value, (datetime, pd.Timestamp)):
            return "time"
        if isinstance(value, str) and is_datetime_string(value):
            return "time"
    return "text"


def detect_purpose(text: str) -> str:
    keywords = text
    if any(key in keywords for key in ["功率", "电量", "电能"]):
        return "功率 / 电量"
    if "流量" in keywords:
        return "流量"
    if any(key in keywords for key in ["温度", "供水", "回水", "上塔", "下塔"]):
        return "温度"
    if any(key in keywords for key in ["运行", "负载", "状态"]):
        return "运行状态 / 运行时间 / 负载率"
    return "未识别"


def build_sheet_entry(df: pd.DataFrame, sheet_name: str, context_text: str) -> dict[str, Any]:
    if df.empty:
        return {
            "sheet": sheet_name,
            "columns": [],
            "first_column": {"name": "", "type": "unknown", "distinct_values": [], "truncated": False},
            "column_roles": [],
            "unit_column": None,
            "sample_rows": [],
            "purpose": detect_purpose(context_text),
        }

    header_row = select_header_row(df)
    header_values = df.iloc[header_row].tolist()
    columns: list[str] = []
    for idx, value in enumerate(header_values, start=1):
        name = normalize_header(value)
        if name == "":
            name = f"col{idx}"
        columns.append(name)

    column_entries = [{"name": name} for name in columns]

    sample_df = df.iloc[header_row + 1 : header_row + 11]
    sample_rows: list[list[Any]] = []
    for _, row in sample_df.iterrows():
        row_values = [normalize_cell(value) for value in row.tolist()]
        sample_rows.append(row_values)

    first_col_values = [row[0] for row in sample_rows if row]
    first_col_type = infer_first_column_type(columns[0] if columns else "", first_col_values)
    distinct_values: list[str] = []
    truncated = False
    if first_col_type == "text":
        seen = set()
        for value in first_col_values:
            if value is None:
                continue
            text = str(value)
            if text in seen:
                continue
            seen.add(text)
            distinct_values.append(text)
            if len(distinct_values) >= 5:
                break
        if len(seen) > len(distinct_values):
            truncated = True

    column_roles: list[dict[str, Any]] = []
    for idx, name in enumerate(columns[:4], start=1):
        values = [row[idx - 1] for row in sample_rows if len(row) >= idx]
        column_roles.append({"index": idx, "name": name, "type": infer_column_type(name, values)})

    unit_column = None
    if columns:
        last_name = columns[-1]
        if "单位" in last_name:
            unit_column = last_name

    return {
        "sheet": sheet_name,
        "columns": column_entries,
        "first_column": {
            "name": columns[0] if columns else "",
            "type": first_col_type,
            "distinct_values": distinct_values,
            "truncated": truncated,
        },
        "column_roles": column_roles,
        "unit_column": unit_column,
        "sample_rows": sample_rows,
        "purpose": detect_purpose(context_text),
    }


def build_catalog(data_root: Path) -> dict[str, Any]:
    folder_map: dict[str, list[dict[str, Any]]] = {}
    for file_path in sorted(data_root.rglob("*.xlsx")):
        rel_path = file_path.relative_to(data_root)
        folder = str(rel_path.parent)
        context_text = f"{folder} {file_path.name}"
        file_entry: dict[str, Any] = {
            "folder": folder,
            "file": file_path.name,
            "relative_path": str(rel_path),
        }

        if file_path.name.startswith("~$"):
            file_entry["sheets"] = []
            file_entry["error"] = "temporary Excel file"
        else:
            try:
                xls = pd.ExcelFile(file_path)
                sheets = []
                for sheet_name in xls.sheet_names:
                    try:
                        df = pd.read_excel(
                            xls,
                            sheet_name=sheet_name,
                            header=None,
                            nrows=12,
                        )
                        sheets.append(build_sheet_entry(df, sheet_name, context_text))
                    except Exception as exc:
                        sheets.append(
                            {
                                "sheet": sheet_name,
                                "columns": [],
                                "first_column": {
                                    "name": "",
                                    "type": "unknown",
                                    "distinct_values": [],
                                    "truncated": False,
                                },
                                "column_roles": [],
                                "unit_column": None,
                                "sample_rows": [],
                                "purpose": detect_purpose(context_text),
                                "error": str(exc),
                            }
                        )
                file_entry["sheets"] = sheets
            except Exception as exc:
                file_entry["sheets"] = []
                file_entry["error"] = str(exc)

        folder_map.setdefault(folder, []).append(file_entry)

    folders = []
    for folder, files in sorted(folder_map.items()):
        folders.append({"path": folder, "files": files})

    return {
        "root": str(data_root),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "folders": folders,
    }


def write_jsonl(catalog: dict[str, Any]) -> None:
    lines = []
    for folder in catalog["folders"]:
        for file_entry in folder["files"]:
            lines.append(json.dumps(file_entry, ensure_ascii=False))
    OUTPUT_JSONL.write_text("\n".join(lines), encoding="utf-8")


def write_markdown(catalog: dict[str, Any]) -> None:
    lines = [
        "# data1 数据目录字典",
        "",
        f"生成时间: {catalog['generated_at']}",
        "",
    ]
    for folder in catalog["folders"]:
        lines.append(f"## {folder['path']}")
        for file_entry in folder["files"]:
            lines.append(f"- {file_entry['file']}")
            if "error" in file_entry:
                lines.append(f"  - 错误: {file_entry['error']}")
            for sheet in file_entry.get("sheets", []):
                lines.append(f"  - Sheet: {sheet['sheet']}")
                lines.append(f"    - 用途: {sheet['purpose']}")
                if sheet.get("column_roles"):
                    first = sheet["column_roles"][0]
                    lines.append(f"    - 列1: {first['name']} ({first['type']})")
                lines.append(f"    - 第一列类型: {sheet['first_column']['type']}")
                if sheet.get("columns"):
                    column_names = ", ".join(col["name"] for col in sheet["columns"])
                    lines.append(f"    - 列: {column_names}")
                sample_rows = sheet.get("sample_rows", [])
                if sample_rows:
                    lines.append("    - 示例行(前5):")
                    for row in sample_rows[:5]:
                        row_text = ", ".join("" if value is None else str(value) for value in row)
                        lines.append(f"      - {row_text}")
            lines.append("")
        lines.append("")
    OUTPUT_MD.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def main() -> None:
    data_root = DEFAULT_DATA_ROOT
    if not data_root.exists():
        raise SystemExit(f"Data root not found: {data_root}")
    catalog = build_catalog(data_root)
    OUTPUT_JSON.write_text(json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8")
    write_jsonl(catalog)
    write_markdown(catalog)


if __name__ == "__main__":
    main()
