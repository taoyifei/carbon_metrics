"""
Pipeline module for cooling_system_v2 data processing.
"""
from __future__ import annotations

import argparse
import threading
import time
from pathlib import Path
from typing import Callable

from .aggregation import compute_agg_day, compute_agg_hour
from .canonical import build_canonical
from .db import LOGGER, execute_sql_file, get_connection, setup_logging
from .ingest import ingest_sources
from .mapping import build_point_mapping
from .metrics import compute_equipment_metrics, compute_metrics
from .quality import compute_agg_day_quality, compute_agg_hour_quality


class PipelineProgress:
    """Simple stage progress printer with heartbeat output."""

    BAR_WIDTH = 24

    def __init__(self, total_steps: int, enabled: bool = True, heartbeat_seconds: int = 15) -> None:
        self.total_steps = max(total_steps, 1)
        self.enabled = enabled and total_steps > 0
        self.heartbeat_seconds = max(1, heartbeat_seconds)
        self.completed_steps = 0
        self._current_stage = ""
        self._stage_started_at = 0.0
        self._stop_event: threading.Event | None = None
        self._heartbeat_thread: threading.Thread | None = None

    @staticmethod
    def _format_duration(seconds: float) -> str:
        total_seconds = max(int(seconds), 0)
        hours, remainder = divmod(total_seconds, 3600)
        minutes, secs = divmod(remainder, 60)
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        return f"{minutes:02d}:{secs:02d}"

    def _build_bar(self, completed_steps: int) -> str:
        filled = int(self.BAR_WIDTH * completed_steps / self.total_steps)
        return f"[{'#' * filled}{'.' * (self.BAR_WIDTH - filled)}]"

    def _emit(self, status: str, stage_name: str, elapsed_seconds: float) -> None:
        if not self.enabled:
            return

        if status == "DONE":
            stage_index = self.completed_steps
        else:
            stage_index = min(self.completed_steps + 1, self.total_steps)

        bar = self._build_bar(self.completed_steps)
        elapsed = self._format_duration(elapsed_seconds)
        print(
            f"{bar} {stage_index}/{self.total_steps} {status:<7} {stage_name} elapsed={elapsed}",
            flush=True,
        )

    def _heartbeat_loop(self) -> None:
        if not self._stop_event:
            return

        while not self._stop_event.wait(self.heartbeat_seconds):
            elapsed = time.perf_counter() - self._stage_started_at
            self._emit("RUNNING", self._current_stage, elapsed)

    def start_stage(self, stage_name: str) -> None:
        if not self.enabled:
            return

        self._current_stage = stage_name
        self._stage_started_at = time.perf_counter()
        self._emit("RUNNING", stage_name, 0.0)

        self._stop_event = threading.Event()
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()

    def finish_stage(self, stage_name: str, success: bool) -> None:
        if not self.enabled:
            return

        if self._stop_event:
            self._stop_event.set()
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=0.2)

        elapsed = time.perf_counter() - self._stage_started_at
        if success:
            self.completed_steps += 1
            self._emit("DONE", stage_name, elapsed)
            return

        self._emit("FAILED", stage_name, elapsed)

    def finish_pipeline(self, total_elapsed: float) -> None:
        if not self.enabled:
            return

        final_bar = self._build_bar(self.total_steps)
        elapsed = self._format_duration(total_elapsed)
        print(
            f"{final_bar} {self.total_steps}/{self.total_steps} COMPLETE pipeline elapsed={elapsed}",
            flush=True,
        )


def _run_stage(progress: PipelineProgress, stage_name: str, runner: Callable[[], None]) -> None:
    progress.start_stage(stage_name)
    try:
        runner()
    except Exception:
        progress.finish_stage(stage_name, success=False)
        raise
    progress.finish_stage(stage_name, success=True)


def run_pipeline(args: argparse.Namespace) -> None:
    """Run selected pipeline stages."""
    base_dir = Path(__file__).resolve().parent.parent
    base_sql = base_dir / "database_v2.sql"
    update_sql = base_dir / "database_v2_1.sql"

    if args.init and not base_sql.exists():
        LOGGER.error("Base SQL file not found: %s", base_sql)
        return

    total_steps = sum([
        bool(args.init),
        bool(args.ingest),
        bool(args.map),
        bool(args.canonical),
        bool(args.agg),
        bool(args.quality),
        bool(args.metrics),
    ])
    progress = PipelineProgress(
        total_steps=total_steps,
        enabled=not getattr(args, "no_progress", False),
        heartbeat_seconds=getattr(args, "progress_interval", 15),
    )
    pipeline_started_at = time.perf_counter()

    if args.init:
        def _run_init() -> None:
            LOGGER.info("Initializing database with %s", base_sql)
            execute_sql_file(base_sql)
            if update_sql.exists():
                LOGGER.info("Applying database update %s", update_sql)
                execute_sql_file(update_sql)

        _run_stage(progress, "init", _run_init)

    with get_connection() as conn:
        if args.ingest:
            def _run_ingest() -> None:
                LOGGER.info("Ingesting sources from %s", base_dir)
                ingest_sources(
                    base_dir,
                    conn,
                    energy_dir=args.energy_dir,
                    params_dir=args.params_dir,
                )

            _run_stage(progress, "ingest", _run_ingest)

        if args.map:
            _run_stage(progress, "map", lambda: build_point_mapping(conn))

        if args.canonical:
            _run_stage(
                progress,
                "canonical",
                lambda: build_canonical(
                    conn,
                    batch_size=getattr(args, "canonical_batch_size", 200000),
                ),
            )

        if args.agg:
            def _run_agg() -> None:
                compute_agg_hour(
                    conn,
                    chunk_hours=getattr(args, "agg_chunk_hours", 24),
                )
                compute_agg_day(conn)

            _run_stage(progress, "agg", _run_agg)

        if args.quality:
            def _run_quality() -> None:
                compute_agg_hour_quality(conn)
                compute_agg_day_quality(conn)

            _run_stage(progress, "quality", _run_quality)

        if args.metrics:
            def _run_metrics() -> None:
                bucket_type = args.bucket_type or "hour"
                compute_metrics(conn, bucket_type, args.start_time, args.end_time)
                compute_equipment_metrics(conn, bucket_type, args.start_time, args.end_time)

            _run_stage(progress, "metrics", _run_metrics)

    progress.finish_pipeline(time.perf_counter() - pipeline_started_at)
