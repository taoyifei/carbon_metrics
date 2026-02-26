"""Shared pytest fixtures for performance optimization tests."""
import threading
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_cursor():
    """MagicMock cursor with configurable fetchone/fetchall returns."""
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = []
    cursor.execute.return_value = None
    return cursor


@pytest.fixture
def mock_connection(mock_cursor):
    """MagicMock connection returning mock_cursor from context manager."""
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn


@pytest.fixture
def sample_cache():
    """Empty dict for query cache testing."""
    return {}


@pytest.fixture
def sample_lock():
    """threading.Lock instance for thread-safety testing."""
    return threading.Lock()
