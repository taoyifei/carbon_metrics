"""Tests for thread-safe shared cache in parallel batch calculation."""
import threading
import os
import re
import textwrap

import pytest


def _get_src_path(relative: str) -> str:
    return os.path.join(os.path.dirname(__file__), '..', *relative.split('/'))


def _load_thread_safe_cache_class():
    """Extract ThreadSafeCache class from source without triggering pymysql import."""
    src_path = _get_src_path('carbon_metrics/backend/services/metric_calculator.py')
    with open(src_path, 'r', encoding='utf-8') as f:
        source = f.read()

    # Extract the class definition block
    match = re.search(
        r'^(class ThreadSafeCache:.*?)(?=\n\nclass |\n\n\n|\Z)',
        source,
        re.MULTILINE | re.DOTALL,
    )
    assert match, "ThreadSafeCache class not found in metric_calculator.py"

    ns = {'threading': threading, 'Dict': dict, 'Any': object}
    exec(textwrap.dedent(match.group(1)), ns)
    return ns['ThreadSafeCache']


class TestThreadSafeCache:
    """Tests for ThreadSafeCache wrapper class."""

    def test_cache_basic_operations(self):
        """ThreadSafeCache supports dict-like get/set/contains."""
        TSC = _load_thread_safe_cache_class()
        cache = TSC()
        cache["key1"] = "value1"
        assert "key1" in cache
        assert cache["key1"] == "value1"
        assert "key2" not in cache

    def test_cache_has_lock(self):
        """ThreadSafeCache exposes a threading.Lock via .lock property."""
        TSC = _load_thread_safe_cache_class()
        cache = TSC()
        assert hasattr(cache, 'lock')
        assert isinstance(cache.lock, type(threading.Lock()))

    def test_cache_thread_safety(self):
        """Multiple threads writing to ThreadSafeCache don't corrupt data."""
        TSC = _load_thread_safe_cache_class()
        cache = TSC()
        errors = []

        def writer(thread_id):
            try:
                for i in range(100):
                    key = f"thread_{thread_id}_item_{i}"
                    with cache.lock:
                        cache[key] = thread_id * 1000 + i
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(t,)) for t in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        for t_id in range(4):
            for i in range(100):
                key = f"thread_{t_id}_item_{i}"
                assert key in cache
                assert cache[key] == t_id * 1000 + i

    def test_parallel_path_uses_shared_cache(self):
        """Parallel path in calculate_batch passes cache (not None) to run_single."""
        src_path = _get_src_path('carbon_metrics/backend/services/metric_calculator.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            source = f.read()

        lines = source.split('\n')
        in_run_single = False
        for line in lines:
            stripped = line.strip()
            if 'def run_single' in stripped:
                in_run_single = True
            if in_run_single and 'query_cache=None' in stripped:
                pytest.fail("Found query_cache=None in run_single — thundering herd bug still present")
            if in_run_single and stripped.startswith('return ') and 'self.calculate' not in stripped:
                in_run_single = False


class TestCachedFetchLocking:
    """Tests for lock-aware _cached_fetchone/_cached_fetchall in base.py."""

    def test_cached_fetchone_with_lock_cache_hit(self):
        """_cached_fetchone uses lock detection pattern."""
        src_path = _get_src_path('carbon_metrics/backend/metrics/base.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            source = f.read()

        assert "getattr(self._query_cache, 'lock', None)" in source, \
            "_cached_fetchone should detect lock via getattr"

    def test_cached_fetchall_with_lock_pattern(self):
        """_cached_fetchall uses lock detection pattern."""
        src_path = _get_src_path('carbon_metrics/backend/metrics/base.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            source = f.read()

        lock_count = source.count("getattr(self._query_cache, 'lock', None)")
        assert lock_count >= 2, \
            f"Expected at least 2 lock detections (fetchone + fetchall), found {lock_count}"
