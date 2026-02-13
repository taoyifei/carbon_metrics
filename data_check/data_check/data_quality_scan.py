from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


try:
    import pymysql
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: pymysql. Install with: pip install pymysql"
    ) from exc


ROOT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_CSV = ROOT_DIR / "docs" / "data_quality_report.csv"


@dataclass
class TableScanConfig:
    name: str
    time_col: str
    value_col: str
    key_cols: list[str]


def get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise SystemExit(f"Missing environment variable: {name}")
    return value


def connect() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=get_env("DB_HOST"),
        port=int(get_env("DB_PORT", "3306")),
        user=get_env("DB_USER"),
        password=get_env("DB_PASSWORD"),
        database=get_env("DB_NAME"),
        charset="utf8mb4",
        autocommit=True,
    )


def fetch_all(cur: pymysql.cursors.Cursor, sql: str, params: Iterable[Any] | None = None) -> list[dict[str, Any]]:
    cur.execute(sql, params or ())
    return list(cur.fetchall())


def list_tables(cur: pymysql.cursors.Cursor, schema: str) -> list[str]:
    rows = fetch_all(
        cur,
        "SELECT TABLE_NAME AS name FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s ORDER BY TABLE_NAME",
        (schema,),
    )
    return [row["name"] for row in rows]


def list_columns(cur: pymysql.cursors.Cursor, schema: str, table: str) -> list[str]:
    rows = fetch_all(
        cur,
        "SELECT COLUMN_NAME AS name FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s",
        (schema, table),
    )
    return [row["name"] for row in rows]


def resolve_scan_config(table: str, columns: list[str]) -> TableScanConfig | None:
    if "collect_time" in columns and "collect_value" in columns:
        keys = ["tag_name"] if "tag_name" in columns else []
        return TableScanConfig(table, "collect_time", "collect_value", keys)
    if "record_time" in columns and "record_value" in columns:
        keys: list[str] = []
        for key in ("device_path", "metric_name"):
            if key in columns:
                keys.append(key)
        return TableScanConfig(table, "record_time", "record_value", keys)
    return None


def infer_range(table: str) -> tuple[float | None, float | None]:
    name = table.lower()
    if "temp" in name:
        return -20.0, 80.0
    if "frequency" in name:
        return 0.0, 100.0
    return None, None


def run_scalar(cur: pymysql.cursors.Cursor, sql: str, params: Iterable[Any]) -> Any:
    cur.execute(sql, params)
    row = cur.fetchone()
    return next(iter(row.values())) if row else None


def build_report_row(
    cur: pymysql.cursors.Cursor,
    config: TableScanConfig,
) -> dict[str, Any]:
    table = config.name
    time_col = config.time_col
    value_col = config.value_col
    key_cols = config.key_cols

    total = run_scalar(cur, f"SELECT COUNT(*) AS cnt FROM `{table}`", ())
    null_time = run_scalar(
        cur,
        f"SELECT COUNT(*) AS cnt FROM `{table}` WHERE `{time_col}` IS NULL",
        (),
    )
    null_value = run_scalar(
        cur,
        f"SELECT COUNT(*) AS cnt FROM `{table}` WHERE `{value_col}` IS NULL",
        (),
    )
    min_time = run_scalar(
        cur,
        f"SELECT MIN(`{time_col}`) AS v FROM `{table}`",
        (),
    )
    max_time = run_scalar(
        cur,
        f"SELECT MAX(`{time_col}`) AS v FROM `{table}`",
        (),
    )
    min_value = run_scalar(
        cur,
        f"SELECT MIN(`{value_col}`) AS v FROM `{table}`",
        (),
    )
    max_value = run_scalar(
        cur,
        f"SELECT MAX(`{value_col}`) AS v FROM `{table}`",
        (),
    )
    negative_count = run_scalar(
        cur,
        f"SELECT COUNT(*) AS cnt FROM `{table}` WHERE `{value_col}` < 0",
        (),
    )

    range_low, range_high = infer_range(table)
    range_low_count = None
    range_high_count = None
    if range_low is not None:
        range_low_count = run_scalar(
            cur,
            f"SELECT COUNT(*) AS cnt FROM `{table}` WHERE `{value_col}` < %s",
            (range_low,),
        )
    if range_high is not None:
        range_high_count = run_scalar(
            cur,
            f"SELECT COUNT(*) AS cnt FROM `{table}` WHERE `{value_col}` > %s",
            (range_high,),
        )

    duplicate_count = None
    if key_cols:
        group_cols = ", ".join([f"`{col}`" for col in key_cols] + [f"`{time_col}`"])
        duplicate_count = run_scalar(
            cur,
            (
                "SELECT COALESCE(SUM(cnt - 1), 0) AS dup FROM ("
                f"SELECT COUNT(*) AS cnt FROM `{table}` GROUP BY {group_cols} HAVING cnt > 1"
                ") t"
            ),
            (),
        )

    return {
        "table": table,
        "time_column": time_col,
        "value_column": value_col,
        "key_columns": ",".join(key_cols),
        "total_rows": total,
        "null_time": null_time,
        "null_value": null_value,
        "min_time": min_time,
        "max_time": max_time,
        "min_value": min_value,
        "max_value": max_value,
        "negative_values": negative_count,
        "range_low": range_low,
        "range_high": range_high,
        "below_range": range_low_count,
        "above_range": range_high_count,
        "duplicate_rows": duplicate_count,
    }


def main() -> None:
    schema = get_env("DB_NAME")
    with connect() as conn:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            tables = list_tables(cur, schema)
            rows: list[dict[str, Any]] = []
            for table in tables:
                columns = list_columns(cur, schema, table)
                config = resolve_scan_config(table, columns)
                if not config:
                    continue
                rows.append(build_report_row(cur, config))

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
