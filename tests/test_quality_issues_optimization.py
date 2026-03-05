from datetime import datetime
import sys
import types

pymysql_stub = types.ModuleType("pymysql")
pymysql_stub.Connection = object
pymysql_stub.connect = lambda **kwargs: None
pymysql_cursors_stub = types.ModuleType("pymysql.cursors")
pymysql_cursors_stub.DictCursor = object
pymysql_stub.cursors = pymysql_cursors_stub
sys.modules.setdefault("pymysql", pymysql_stub)
sys.modules.setdefault("pymysql.cursors", pymysql_cursors_stub)

from carbon_metrics.backend.services.quality_service import QualityService


class _FakeCursor:
    def __init__(self, *, fetchall_results=None, fetchone_results=None):
        self.fetchall_results = list(fetchall_results or [])
        self.fetchone_results = list(fetchone_results or [])
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        if self.fetchall_results:
            return self.fetchall_results.pop(0)
        return []

    def fetchone(self):
        if self.fetchone_results:
            return self.fetchone_results.pop(0)
        return {}


class _FakeDb:
    def __init__(self, cursor_obj):
        self.cursor_obj = cursor_obj

    def cursor(self):
        return self.cursor_obj


def _build_service(cursor_obj) -> QualityService:
    service = QualityService.__new__(QualityService)
    service.db = _FakeDb(cursor_obj)
    return service


def _build_time_range():
    return (
        datetime(2025, 1, 1, 0, 0, 0),
        datetime(2025, 1, 2, 0, 0, 0),
    )


def test_quality_issues_uses_single_window_query_when_rows_present():
    time_start, time_end = _build_time_range()
    cursor = _FakeCursor(
        fetchall_results=[[
            {
                "bucket_time": time_start,
                "building_id": "B1",
                "system_id": "S1",
                "equipment_type": "chiller",
                "equipment_id": "EQ1",
                "sub_equipment_id": None,
                "metric_name": "energy",
                "issue_type": "gap",
                "issue_count": 6,
                "max_gap_seconds": 3600,
                "severity": "high",
                "quality_score": 80,
                "total_count": 7,
            }
        ]]
    )
    service = _build_service(cursor)

    result = service.get_issues(
        time_start=time_start,
        time_end=time_end,
        issue_type="gap",
        severity="high",
        page=1,
        page_size=20,
    )

    assert len(cursor.executed) == 1
    sql, params = cursor.executed[0]
    assert "COUNT(*) OVER() AS total_count" in sql
    assert "WHERE severity = %s" not in sql
    assert "gap_count >= 5" in sql
    assert params == [time_start, time_end, 20, 0]
    assert result["total"] == 7
    assert result["total_pages"] == 1
    assert result["items"][0]["issue_type"] == "gap"


def test_quality_issues_falls_back_to_count_when_page_is_empty():
    time_start, time_end = _build_time_range()
    cursor = _FakeCursor(
        fetchall_results=[[]],
        fetchone_results=[{"total": 3}],
    )
    service = _build_service(cursor)

    result = service.get_issues(
        time_start=time_start,
        time_end=time_end,
        issue_type="gap",
        page=2,
        page_size=2,
    )

    assert len(cursor.executed) == 2
    assert "COUNT(*) OVER() AS total_count" in cursor.executed[0][0]
    assert "SELECT COUNT(*) AS total" in cursor.executed[1][0]
    assert result["items"] == []
    assert result["total"] == 3
    assert result["total_pages"] == 2
