from __future__ import annotations

import csv
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


try:
    import pymysql
except ImportError as exc: 
    raise SystemExit(
        "Missing pymysql. Install with: pip install pymysql ASAP"
    ) from exc


ROOT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_REPORT = ROOT_DIR / "docs" / "data_quality_deep_report.csv"
OUTPUT_EXAMPLES = ROOT_DIR / "docs" / "data_quality_deep_examples.csv"


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


def get_env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


def get_env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value else default


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


def fetch_all(cur: pymysql.cursors.Cursor, sql: str, params: Iterable[Any] | None = None) -> list[Any]:
    cur.execute(sql, params or ())
    return list(cur.fetchall())


def get_row_value(row: Any) -> Any:
    if row is None:
        return None
    if isinstance(row, dict):
        return next(iter(row.values()))
    if isinstance(row, (list, tuple)):
        return row[0] if row else None
    return row


def get_row_field(row: Any, key: str) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    return None


def run_scalar(cur: pymysql.cursors.Cursor, sql: str, params: Iterable[Any] | None = None) -> Any:
    cur.execute(sql, params or ())
    row = cur.fetchone()
    return get_row_value(row)


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


def build_key_expr(key_cols: list[str]) -> str:
    if not key_cols:
        return "'__all__'"
    escaped = [f"`{col}`" for col in key_cols]
    return f"CONCAT_WS('||', {', '.join(escaped)})"


def build_time_filter(time_col: str, start_time: str | None, end_time: str | None) -> tuple[str, list[Any]]:
    clauses = [f"`{time_col}` IS NOT NULL"]
    params: list[Any] = []
    if start_time:
        clauses.append(f"`{time_col}` >= %s")
        params.append(start_time)
    if end_time:
        clauses.append(f"`{time_col}` <= %s")
        params.append(end_time)
    return " AND ".join(clauses), params


def infer_run_status_rule(cur: pymysql.cursors.Cursor, table: str, value_col: str, time_col: str, time_filter: str, params: list[Any], threshold: float) -> tuple[str, int]:
    total = run_scalar(
        cur,
        f"SELECT COUNT(*) AS cnt FROM `{table}` WHERE {time_filter} AND `{value_col}` IS NOT NULL",
        params,
    )
    if not total:
        return "unknown", 0
    count01 = run_scalar(
        cur,
        (
            f"SELECT COUNT(*) AS cnt FROM `{table}` "
            f"WHERE {time_filter} AND `{value_col}` IN (0, 1)"
        ),
        params,
    )
    count0100 = run_scalar(
        cur,
        (
            f"SELECT COUNT(*) AS cnt FROM `{table}` "
            f"WHERE {time_filter} AND `{value_col}` >= 0 AND `{value_col}` <= 100"
        ),
        params,
    )
    ratio01 = float(count01 or 0) / float(total)
    ratio0100 = float(count0100 or 0) / float(total)
    if ratio01 >= threshold:
        return "binary", total
    if ratio0100 >= threshold:
        return "percent", total
    return "unknown", total


def select_examples(cur: pymysql.cursors.Cursor, sql: str, params: list[Any], limit: int) -> list[Any]:
    cur.execute(sql, params + [limit])
    return list(cur.fetchall())


def main() -> None:
    schema = get_env("DB_NAME")
    start_time = os.getenv("START_TIME")
    end_time = os.getenv("END_TIME")
    sample_limit = get_env_int("SAMPLE_LIMIT", 50)
    gap_factor = get_env_float("GAP_FACTOR", 3.0)
    jump_sigma = get_env_float("JUMP_SIGMA", 5.0)
    min_key_rows = get_env_int("MIN_KEY_ROWS", 100)
    status_threshold = get_env_float("STATUS_THRESHOLD", 0.95)

    report_rows: list[dict[str, Any]] = []
    example_rows: list[dict[str, Any]] = []

    start_ts = time.time()
    with connect() as conn:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            tables = list_tables(cur, schema)
            total_tables = len(tables)
            for idx, table in enumerate(tables, start=1):
                elapsed = time.time() - start_ts
                print(f"[{idx}/{total_tables}] Scanning {table} (elapsed {elapsed:.1f}s)")
                columns = list_columns(cur, schema, table)
                config = resolve_scan_config(table, columns)
                if not config:
                    print(f"  - skipped (no time/value columns)")
                    continue

                time_filter, params = build_time_filter(config.time_col, start_time, end_time)
                key_expr = build_key_expr(config.key_cols)
                key_cols_str = ",".join(config.key_cols)

                total_rows = run_scalar(cur, f"SELECT COUNT(*) AS cnt FROM `{table}`", [])
                key_count = None
                if config.key_cols:
                    key_count = run_scalar(
                        cur,
                        f"SELECT COUNT(DISTINCT {key_expr}) AS cnt FROM `{table}` WHERE {time_filter}",
                        params,
                    )

                min_time = run_scalar(
                    cur,
                    f"SELECT MIN(`{config.time_col}`) AS v FROM `{table}` WHERE {time_filter}",
                    params,
                )
                max_time = run_scalar(
                    cur,
                    f"SELECT MAX(`{config.time_col}`) AS v FROM `{table}` WHERE {time_filter}",
                    params,
                )
                min_value = run_scalar(
                    cur,
                    f"SELECT MIN(`{config.value_col}`) AS v FROM `{table}` WHERE {time_filter}",
                    params,
                )
                max_value = run_scalar(
                    cur,
                    f"SELECT MAX(`{config.value_col}`) AS v FROM `{table}` WHERE {time_filter}",
                    params,
                )
                negative_values = run_scalar(
                    cur,
                    (
                        f"SELECT COUNT(*) AS cnt FROM `{table}` "
                        f"WHERE {time_filter} AND `{config.value_col}` < 0"
                    ),
                    params,
                )

                # Duplicate rows (key + time)
                duplicate_rows = None
                if config.key_cols:
                    duplicate_rows = run_scalar(
                        cur,
                        (
                            "SELECT COALESCE(SUM(cnt - 1), 0) AS dup FROM ("
                            f"SELECT COUNT(*) AS cnt FROM `{table}` "
                            f"WHERE {time_filter} GROUP BY {key_expr}, `{config.time_col}` HAVING cnt > 1"
                            ") t"
                        ),
                        params,
                    )

                    dup_examples = select_examples(
                        cur,
                        (
                            f"SELECT {key_expr} AS key_value, `{config.time_col}` AS time_value, COUNT(*) AS cnt "
                            f"FROM `{table}` WHERE {time_filter} "
                            f"GROUP BY {key_expr}, `{config.time_col}` HAVING cnt > 1 "
                            f"ORDER BY cnt DESC LIMIT %s"
                        ),
                        params,
                        sample_limit,
                    )
                    for row in dup_examples:
                        example_rows.append(
                            {
                                "table": table,
                                "key": get_row_field(row, "key_value"),
                                "time": get_row_field(row, "time_value"),
                                "value": None,
                                "anomaly_type": "duplicate_rows",
                                "detail": f"count={get_row_field(row, 'cnt')}",
                            }
                        )

                # Interval stats
                interval_subquery = (
                    f"SELECT {key_expr} AS key_value, `{config.time_col}` AS time_value, "
                    f"TIMESTAMPDIFF(SECOND, LAG(`{config.time_col}`) OVER (PARTITION BY {key_expr} ORDER BY `{config.time_col}`), `{config.time_col}`) "
                    f"AS delta_seconds FROM `{table}` WHERE {time_filter}"
                )
                mode_interval = None
                interval_count = run_scalar(
                    cur,
                    f"SELECT COUNT(*) AS cnt FROM ({interval_subquery}) t WHERE delta_seconds IS NOT NULL AND delta_seconds > 0",
                    params,
                )
                if interval_count:
                    mode_row = fetch_all(
                        cur,
                        (
                            "SELECT delta_seconds, COUNT(*) AS cnt FROM ("
                            f"{interval_subquery}"
                            ") t WHERE delta_seconds IS NOT NULL AND delta_seconds > 0 "
                            "GROUP BY delta_seconds ORDER BY cnt DESC LIMIT 1"
                        ),
                        params,
                    )
                    if mode_row:
                        mode_interval = mode_row[0]["delta_seconds"]

                irregular_rate = None
                max_gap = None
                gap_count = None
                if mode_interval:
                    irregular_count = run_scalar(
                        cur,
                        (
                            "SELECT COUNT(*) AS cnt FROM ("
                            f"{interval_subquery}"
                            ") t WHERE delta_seconds IS NOT NULL AND delta_seconds > 0 AND delta_seconds <> %s"
                        ),
                        params + [mode_interval],
                    )
                    irregular_rate = float(irregular_count or 0) / float(interval_count)
                    max_gap = run_scalar(
                        cur,
                        (
                            "SELECT MAX(delta_seconds) AS v FROM ("
                            f"{interval_subquery}"
                            ") t WHERE delta_seconds IS NOT NULL AND delta_seconds > 0"
                        ),
                        params,
                    )
                    gap_threshold = float(mode_interval) * gap_factor
                    gap_count = run_scalar(
                        cur,
                        (
                            "SELECT COUNT(*) AS cnt FROM ("
                            f"{interval_subquery}"
                            ") t WHERE delta_seconds IS NOT NULL AND delta_seconds >= %s"
                        ),
                        params + [gap_threshold],
                    )

                    gap_examples = select_examples(
                        cur,
                        (
                            "SELECT key_value, time_value, delta_seconds FROM ("
                            f"{interval_subquery}"
                            ") t WHERE delta_seconds IS NOT NULL AND delta_seconds >= %s "
                            "ORDER BY delta_seconds DESC LIMIT %s"
                        ),
                        params + [gap_threshold],
                        sample_limit,
                    )
                    for row in gap_examples:
                        example_rows.append(
                            {
                                "table": table,
                                "key": get_row_field(row, "key_value"),
                                "time": get_row_field(row, "time_value"),
                                "value": get_row_field(row, "delta_seconds"),
                                "anomaly_type": "time_gap",
                                "detail": f"threshold={gap_threshold}",
                            }
                        )

                # Jump detection
                diff_subquery = (
                    f"SELECT {key_expr} AS key_value, `{config.time_col}` AS time_value, `{config.value_col}` AS value_value, "
                    f"ABS(`{config.value_col}` - LAG(`{config.value_col}`) OVER (PARTITION BY {key_expr} ORDER BY `{config.time_col}`)) "
                    f"AS diff_value, LAG(`{config.value_col}`) OVER (PARTITION BY {key_expr} ORDER BY `{config.time_col}`) AS prev_value "
                    f"FROM `{table}` WHERE {time_filter} AND `{config.value_col}` IS NOT NULL"
                )
                diff_stats = fetch_all(
                    cur,
                    (
                        "SELECT AVG(diff_value) AS avg_diff, STDDEV_POP(diff_value) AS std_diff FROM ("
                        f"{diff_subquery}"
                        ") t WHERE diff_value IS NOT NULL"
                    ),
                    params,
                )
                jump_anomaly_count = None
                if diff_stats and diff_stats[0]["avg_diff"] is not None:
                    avg_diff = float(diff_stats[0]["avg_diff"] or 0)
                    std_diff = float(diff_stats[0]["std_diff"] or 0)
                    threshold = avg_diff + jump_sigma * std_diff
                    if std_diff > 0:
                        jump_anomaly_count = run_scalar(
                            cur,
                            (
                                "SELECT COUNT(*) AS cnt FROM ("
                                f"{diff_subquery}"
                                ") t WHERE diff_value IS NOT NULL AND diff_value > %s"
                            ),
                            params + [threshold],
                        )
                        jump_examples = select_examples(
                            cur,
                            (
                                "SELECT key_value, time_value, value_value, prev_value, diff_value FROM ("
                                f"{diff_subquery}"
                                ") t WHERE diff_value IS NOT NULL AND diff_value > %s "
                                "ORDER BY diff_value DESC LIMIT %s"
                            ),
                            params + [threshold],
                            sample_limit,
                        )
                        for row in jump_examples:
                            example_rows.append(
                                {
                                    "table": table,
                                    "key": get_row_field(row, "key_value"),
                                    "time": get_row_field(row, "time_value"),
                                    "value": get_row_field(row, "value_value"),
                                    "anomaly_type": "value_jump",
                                    "detail": (
                                        f"prev={get_row_field(row, 'prev_value')}, "
                                        f"diff={get_row_field(row, 'diff_value')}, "
                                        f"threshold={threshold}"
                                    ),
                                }
                            )

                # Range checks
                temp_anomalies = None
                freq_anomalies = None
                load_ratio_anomalies = None
                status_anomalies = None
                status_rule = None

                if "temp" in table.lower():
                    temp_anomalies = run_scalar(
                        cur,
                        (
                            f"SELECT COUNT(*) AS cnt FROM `{table}` WHERE {time_filter} "
                            f"AND (`{config.value_col}` < -20 OR `{config.value_col}` > 80)"
                        ),
                        params,
                    )
                    temp_examples = select_examples(
                        cur,
                        (
                            f"SELECT {key_expr} AS key_value, `{config.time_col}` AS time_value, `{config.value_col}` AS value_value "
                            f"FROM `{table}` WHERE {time_filter} AND (`{config.value_col}` < -20 OR `{config.value_col}` > 80) "
                            "ORDER BY value_value DESC LIMIT %s"
                        ),
                        params,
                        sample_limit,
                    )
                    for row in temp_examples:
                        example_rows.append(
                            {
                                "table": table,
                                "key": get_row_field(row, "key_value"),
                                "time": get_row_field(row, "time_value"),
                                "value": get_row_field(row, "value_value"),
                                "anomaly_type": "temp_out_of_range",
                                "detail": "range=-20..80",
                            }
                        )

                if "frequency" in table.lower():
                    freq_anomalies = run_scalar(
                        cur,
                        (
                            f"SELECT COUNT(*) AS cnt FROM `{table}` WHERE {time_filter} "
                            f"AND (`{config.value_col}` < 0 OR `{config.value_col}` > 100)"
                        ),
                        params,
                    )
                    freq_examples = select_examples(
                        cur,
                        (
                            f"SELECT {key_expr} AS key_value, `{config.time_col}` AS time_value, `{config.value_col}` AS value_value "
                            f"FROM `{table}` WHERE {time_filter} AND (`{config.value_col}` < 0 OR `{config.value_col}` > 100) "
                            "ORDER BY value_value DESC LIMIT %s"
                        ),
                        params,
                        sample_limit,
                    )
                    for row in freq_examples:
                        example_rows.append(
                            {
                                "table": table,
                                "key": get_row_field(row, "key_value"),
                                "time": get_row_field(row, "time_value"),
                                "value": get_row_field(row, "value_value"),
                                "anomaly_type": "frequency_out_of_range",
                                "detail": "range=0..100",
                            }
                        )

                if "load_ratio" in table.lower():
                    load_ratio_anomalies = run_scalar(
                        cur,
                        (
                            f"SELECT COUNT(*) AS cnt FROM `{table}` WHERE {time_filter} "
                            f"AND (`{config.value_col}` < 0 OR `{config.value_col}` > 100)"
                        ),
                        params,
                    )
                    load_examples = select_examples(
                        cur,
                        (
                            f"SELECT {key_expr} AS key_value, `{config.time_col}` AS time_value, `{config.value_col}` AS value_value "
                            f"FROM `{table}` WHERE {time_filter} AND (`{config.value_col}` < 0 OR `{config.value_col}` > 100) "
                            "ORDER BY value_value DESC LIMIT %s"
                        ),
                        params,
                        sample_limit,
                    )
                    for row in load_examples:
                        example_rows.append(
                            {
                                "table": table,
                                "key": get_row_field(row, "key_value"),
                                "time": get_row_field(row, "time_value"),
                                "value": get_row_field(row, "value_value"),
                                "anomaly_type": "load_ratio_out_of_range",
                                "detail": "range=0..100",
                            }
                        )

                if "run_status" in table.lower():
                    status_rule, status_total = infer_run_status_rule(
                        cur,
                        table,
                        config.value_col,
                        config.time_col,
                        time_filter,
                        params,
                        status_threshold,
                    )
                    if status_rule == "binary":
                        status_anomalies = run_scalar(
                            cur,
                            (
                                f"SELECT COUNT(*) AS cnt FROM `{table}` WHERE {time_filter} "
                                f"AND `{config.value_col}` IS NOT NULL AND `{config.value_col}` NOT IN (0, 1)"
                            ),
                            params,
                        )
                    elif status_rule == "percent":
                        status_anomalies = run_scalar(
                            cur,
                            (
                                f"SELECT COUNT(*) AS cnt FROM `{table}` WHERE {time_filter} "
                                f"AND `{config.value_col}` IS NOT NULL AND (`{config.value_col}` < 0 OR `{config.value_col}` > 100)"
                            ),
                            params,
                        )
                    else:
                        status_anomalies = None

                    if status_anomalies:
                        status_examples = select_examples(
                            cur,
                            (
                                f"SELECT {key_expr} AS key_value, `{config.time_col}` AS time_value, `{config.value_col}` AS value_value "
                                f"FROM `{table}` WHERE {time_filter} AND `{config.value_col}` IS NOT NULL "
                                f"AND (`{config.value_col}` < 0 OR `{config.value_col}` > 100) "
                                "ORDER BY value_value DESC LIMIT %s"
                            ),
                            params,
                            sample_limit,
                        )
                        for row in status_examples:
                            example_rows.append(
                                {
                                    "table": table,
                                    "key": get_row_field(row, "key_value"),
                                    "time": get_row_field(row, "time_value"),
                                    "value": get_row_field(row, "value_value"),
                                    "anomaly_type": "run_status_out_of_range",
                                    "detail": f"rule={status_rule}",
                                }
                            )

                # Unit consistency (collect tables)
                unit_inconsistent_keys = None
                if "unit" in columns and "tag_name" in columns:
                    unit_inconsistent_keys = run_scalar(
                        cur,
                        (
                            "SELECT COUNT(*) AS cnt FROM ("
                            f"SELECT `tag_name`, COUNT(DISTINCT `unit`) AS c FROM `{table}` "
                            f"WHERE {time_filter} AND `unit` IS NOT NULL GROUP BY `tag_name` HAVING c > 1"
                            ") t"
                        ),
                        params,
                    )
                    unit_examples = select_examples(
                        cur,
                        (
                            "SELECT tag_name, GROUP_CONCAT(DISTINCT unit ORDER BY unit SEPARATOR ',') AS units "
                            f"FROM `{table}` WHERE {time_filter} AND unit IS NOT NULL "
                            "GROUP BY tag_name HAVING COUNT(DISTINCT unit) > 1 LIMIT %s"
                        ),
                        params,
                        sample_limit,
                    )
                    for row in unit_examples:
                        example_rows.append(
                            {
                                "table": table,
                                "key": get_row_field(row, "tag_name"),
                                "time": None,
                                "value": None,
                                "anomaly_type": "unit_inconsistent",
                                "detail": get_row_field(row, "units"),
                            }
                        )

                # Sparse keys
                sparse_keys = None
                if config.key_cols:
                    sparse_keys = run_scalar(
                        cur,
                        (
                            "SELECT COUNT(*) AS cnt FROM ("
                            f"SELECT {key_expr} AS key_value, COUNT(*) AS c FROM `{table}` "
                            f"WHERE {time_filter} GROUP BY {key_expr} HAVING c < %s"
                            ") t"
                        ),
                        params + [min_key_rows],
                    )
                    sparse_examples = select_examples(
                        cur,
                        (
                            f"SELECT {key_expr} AS key_value, COUNT(*) AS c FROM `{table}` "
                            f"WHERE {time_filter} GROUP BY {key_expr} HAVING c < %s "
                            "ORDER BY c ASC LIMIT %s"
                        ),
                        params + [min_key_rows],
                        sample_limit,
                    )
                    for row in sparse_examples:
                        example_rows.append(
                            {
                                "table": table,
                                "key": get_row_field(row, "key_value"),
                                "time": None,
                                "value": get_row_field(row, "c"),
                                "anomaly_type": "sparse_key",
                                "detail": f"min_rows={min_key_rows}",
                            }
                        )

                report_rows.append(
                    {
                        "table": table,
                        "time_column": config.time_col,
                        "value_column": config.value_col,
                        "key_columns": key_cols_str,
                        "total_rows": total_rows,
                        "key_count": key_count,
                        "time_start": min_time,
                        "time_end": max_time,
                        "min_value": min_value,
                        "max_value": max_value,
                        "negative_values": negative_values,
                        "mode_interval_seconds": mode_interval,
                        "interval_irregular_rate": irregular_rate,
                        "max_gap_seconds": max_gap,
                        "gap_count": gap_count,
                        "duplicate_rows": duplicate_rows,
                        "jump_anomaly_count": jump_anomaly_count,
                        "run_status_rule": status_rule,
                        "run_status_anomaly_count": status_anomalies,
                        "load_ratio_anomaly_count": load_ratio_anomalies,
                        "temp_anomaly_count": temp_anomalies,
                        "frequency_anomaly_count": freq_anomalies,
                        "unit_inconsistent_keys": unit_inconsistent_keys,
                        "sparse_keys": sparse_keys,
                    }
                )
                elapsed = time.time() - start_ts
                print(f"  - done in {elapsed:.1f}s (table complete)")

    OUTPUT_REPORT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_REPORT.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(report_rows[0].keys()) if report_rows else [])
        writer.writeheader()
        writer.writerows(report_rows)

    with OUTPUT_EXAMPLES.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["table", "key", "time", "value", "anomaly_type", "detail"],
        )
        writer.writeheader()
        writer.writerows(example_rows)

    total_elapsed = time.time() - start_ts
    print(f"Wrote report: {OUTPUT_REPORT}")
    print(f"Wrote examples: {OUTPUT_EXAMPLES}")
    print(f"Total elapsed: {total_elapsed:.1f}s")


if __name__ == "__main__":
    main()
