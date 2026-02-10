"""
数据库连接模块
提供数据库连接池和上下文管理
"""
import pymysql
import os
import logging
import threading
from queue import Queue, Empty
from pymysql.cursors import DictCursor
from contextlib import contextmanager
from typing import Generator, Any, Dict, Optional

from .config import get_db_config, DatabaseConfig

logger = logging.getLogger(__name__)


class Database:
    """数据库连接管理类"""

    def __init__(self, config: DatabaseConfig = None, pool_size: int = 4):
        self.config = config or get_db_config()
        self._pool: Queue[pymysql.Connection] = Queue(maxsize=pool_size)
        self._pool_size = pool_size
        self._lock = threading.Lock()

    def _get_connection_params(self) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "host": self.config.host,
            "port": self.config.port,
            "user": self.config.user,
            "password": self.config.password,
            "database": self.config.database,
            "charset": self.config.charset,
            "cursorclass": DictCursor,
            "autocommit": True,
        }
        for env_key, param_key in (
            ("DB_CONNECT_TIMEOUT", "connect_timeout"),
            ("DB_READ_TIMEOUT", "read_timeout"),
            ("DB_WRITE_TIMEOUT", "write_timeout"),
        ):
            timeout_value = self._parse_optional_positive_int(env_key)
            if timeout_value is not None:
                params[param_key] = timeout_value
        return params

    @staticmethod
    def _parse_optional_positive_int(env_key: str) -> Optional[int]:
        raw_value = os.getenv(env_key)
        if raw_value is None or raw_value == "":
            return None
        try:
            value = int(raw_value)
            if value <= 0:
                logger.warning("Invalid %s=%r, expected positive integer", env_key, raw_value)
                return None
            return value
        except (TypeError, ValueError):
            logger.warning("Invalid %s=%r, expected integer", env_key, raw_value)
            return None

    def _create_connection(self) -> pymysql.Connection:
        """创建新的数据库连接"""
        return pymysql.connect(**self._get_connection_params())

    def get_connection(self) -> pymysql.Connection:
        """从池中获取连接，池空则新建"""
        try:
            conn = self._pool.get_nowait()
            try:
                conn.ping(reconnect=False)
                return conn
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
        except Empty:
            pass
        return self._create_connection()

    def _return_connection(self, conn: pymysql.Connection) -> None:
        """归还连接到池中，池满则关闭"""
        try:
            self._pool.put_nowait(conn)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass

    def close(self):
        """关闭池中所有连接"""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except Exception:
                pass

    @contextmanager
    def cursor(self) -> Generator[DictCursor, None, None]:
        """获取游标的上下文管理器（连接用完归还池中）"""
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            yield cur
        finally:
            cur.close()
            self._return_connection(conn)


# 全局数据库实例
_db_instance: Database = None


def get_db() -> Database:
    """获取全局数据库实例"""
    global _db_instance
    if _db_instance is None:
        _db_instance = Database()
    return _db_instance
