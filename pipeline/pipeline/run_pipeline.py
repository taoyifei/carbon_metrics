#!/usr/bin/env python
"""
Pipeline entry script.

Examples:
    python run_pipeline.py --help
    python run_pipeline.py --init --ingest --map --canonical --agg
    python run_pipeline.py --quality --metrics
"""
from __future__ import annotations

import argparse

from pipeline import run_pipeline, setup_logging


def build_parser() -> argparse.ArgumentParser:
    """Build command line argument parser."""
    parser = argparse.ArgumentParser(description="cooling_system_v2 pipeline")
    parser.add_argument("--init", action="store_true", help="Execute database_v2.sql and database_v2_1.sql")
    parser.add_argument("--ingest", action="store_true", help="Ingest params and raw data")
    parser.add_argument("--map", action="store_true", help="Build point_mapping from raw data")
    parser.add_argument("--canonical", action="store_true", help="Build canonical_measurement")
    parser.add_argument("--agg", action="store_true", help="Compute agg_hour and agg_day")
    parser.add_argument("--quality", action="store_true", help="Compute data quality metrics")
    parser.add_argument("--metrics", action="store_true", help="Compute metrics and write to metric_result")
    parser.add_argument(
        "--canonical-batch-size",
        type=int,
        default=200000,
        help="Canonical rebuild batch size by raw_measurement ID range (default: 200000)",
    )
    parser.add_argument(
        "--agg-chunk-hours",
        type=int,
        default=24,
        help="Hourly aggregation chunk size in hours (default: 24)",
    )
    parser.add_argument(
        "--bucket-type",
        choices=["hour", "day"],
        default="hour",
        help="Time bucket type for metric computation (default: hour)",
    )
    parser.add_argument("--start-time", help="Start time for metric computation (YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--end-time", help="End time for metric computation (YYYY-MM-DD HH:MM:SS)")
    parser.add_argument(
        "--energy-dir",
        help="Input root directory for device/tag excel files (defaults to norm/create_sql/energy data folder)",
    )
    parser.add_argument(
        "--params-dir",
        help="Input directory for parameter excel files (defaults to norm/create_sql/params folder)",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=15,
        help="Console progress heartbeat interval in seconds (default: 15)",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable console stage progress output",
    )
    return parser


def main() -> None:
    """Main entrypoint."""
    setup_logging()
    parser = build_parser()
    args = parser.parse_args()

    if not any([args.init, args.ingest, args.map, args.canonical, args.agg, args.quality, args.metrics]):
        parser.print_help()
        return

    run_pipeline(args)


if __name__ == "__main__":
    main()
