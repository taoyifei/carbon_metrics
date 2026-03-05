"""Tests for coverage_overview performance-oriented logic."""
import os
import re


def _get_src_path(relative: str) -> str:
    return os.path.join(os.path.dirname(__file__), '..', *relative.split('/'))


def _load_source(relative: str) -> str:
    with open(_get_src_path(relative), 'r', encoding='utf-8') as f:
        return f.read()


def test_coverage_overview_uses_batch_calculation():
    """coverage_overview should reuse calculate_batch instead of serial calculate()."""
    source = _load_source('carbon_metrics/backend/services/metric_calculator.py')
    match = re.search(
        r'def coverage_overview\(.*?return \{',
        source,
        re.MULTILINE | re.DOTALL,
    )
    assert match, "coverage_overview not found in metric_calculator.py"
    block = match.group(0)

    assert 'self.calculate_batch(' in block, (
        "coverage_overview should reuse calculate_batch for shared cache / "
        "parallel execution benefits"
    )
    assert 'self.calculate(' not in block, (
        "coverage_overview should not call self.calculate() serially anymore"
    )


def test_available_metric_counts_no_longer_sorts_unused_counts():
    """_query_available_metric_counts should avoid ORDER BY cnt DESC."""
    source = _load_source('carbon_metrics/backend/services/metric_calculator.py')
    match = re.search(
        r'def _query_available_metric_counts\(.*?return \{',
        source,
        re.MULTILINE | re.DOTALL,
    )
    assert match, "_query_available_metric_counts not found in metric_calculator.py"
    block = match.group(0)

    assert 'ORDER BY cnt DESC' not in block, (
        "_query_available_metric_counts returns a dict, so ORDER BY cnt DESC "
        "adds avoidable sort work"
    )
