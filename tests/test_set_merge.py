"""Tests verifying SET statement merging across metric files."""
import os

import pytest


def _count_set_statements(filepath: str) -> int:
    """Count lines containing 'SET @' in a Python source file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return sum(1 for line in f if 'SET @' in line)


def _get_metric_path(filename: str) -> str:
    """Get absolute path to a metric module file."""
    return os.path.join(
        os.path.dirname(__file__), '..',
        'carbon_metrics', 'backend', 'metrics', filename
    )


class TestSetStatementMerge:
    """Verify SET statements are merged to minimize DB round-trips."""

    def test_energy_set_count(self):
        """energy.py should have exactly 2 SET @ lines (one per query function)."""
        path = _get_metric_path('energy.py')
        count = _count_set_statements(path)
        assert count == 2, f"energy.py: expected 2 SET @ lines, found {count}"

    def test_pump_set_count(self):
        """pump.py should have exactly 1 SET @ line."""
        path = _get_metric_path('pump.py')
        count = _count_set_statements(path)
        assert count == 1, f"pump.py: expected 1 SET @ line, found {count}"

    def test_tower_set_count(self):
        """tower.py should have exactly 1 SET @ line."""
        path = _get_metric_path('tower.py')
        count = _count_set_statements(path)
        assert count == 1, f"tower.py: expected 1 SET @ line, found {count}"

    def test_stability_set_count(self):
        """stability.py should have exactly 1 SET @ line."""
        path = _get_metric_path('stability.py')
        count = _count_set_statements(path)
        assert count == 1, f"stability.py: expected 1 SET @ line, found {count}"

    def test_total_set_count(self):
        """Total SET @ across all metric files should be 5 (was 11)."""
        files = ['energy.py', 'pump.py', 'tower.py', 'stability.py']
        total = sum(_count_set_statements(_get_metric_path(f)) for f in files)
        assert total == 5, f"Total SET @ lines: expected 5, found {total}"
