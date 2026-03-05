from datetime import datetime

from carbon_metrics.backend.metrics.base import MetricContext
from carbon_metrics.backend.metrics.energy import (
    _query_energy_by_bucket_type,
    _query_energy_by_type,
)


class _FakeCursor:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self.rows


class _FakeDb:
    def __init__(self, rows=None):
        self.cursor_obj = _FakeCursor(rows=rows)

    def cursor(self):
        return self.cursor_obj


def _build_ctx() -> MetricContext:
    return MetricContext(
        time_start=datetime(2025, 1, 1, 0, 0, 0),
        time_end=datetime(2025, 1, 2, 0, 0, 0),
        building_id="B1",
        system_id=None,
        equipment_type=None,
        equipment_id=None,
        sub_equipment_id=None,
    )


def test_query_energy_by_type_uses_window_function_single_scope():
    db = _FakeDb(rows=[])
    ctx = _build_ctx()

    rows, sql, error = _query_energy_by_type(
        db,
        ctx,
        clamp_threshold=0.1,
        positive_clamp_threshold=1000.0,
    )

    assert error is None
    assert rows == []
    assert "AVG(ABS(agg_last)) OVER (PARTITION BY equipment_type)" in sql
    assert "energy_type_mode" not in sql
    assert "SELECT * FROM agg_hour" not in sql
    assert db.cursor_obj.executed[1][1] == [
        ctx.time_start,
        ctx.time_end,
        ctx.building_id,
    ]


def test_query_energy_by_bucket_type_uses_window_function_single_scope():
    db = _FakeDb(rows=[])
    ctx = _build_ctx()

    rows, sql, error = _query_energy_by_bucket_type(
        db,
        ctx,
        clamp_threshold=0.1,
        positive_clamp_threshold=1000.0,
    )

    assert error is None
    assert rows == []
    assert "AVG(ABS(agg_last)) OVER (PARTITION BY equipment_type)" in sql
    assert "energy_type_mode" not in sql
    assert "SELECT * FROM agg_hour" not in sql
    assert "bucket_time," in sql
    assert db.cursor_obj.executed[1][1] == [
        ctx.time_start,
        ctx.time_end,
        ctx.building_id,
    ]
