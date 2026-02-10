"""
配置管理模块
从环境变量读取配置
"""
import logging
import os
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    """数据库配置"""
    host: str = "localhost"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = "cooling_system_v2"
    charset: str = "utf8mb4"


@dataclass
class AppConfig:
    """应用配置"""
    debug: bool = False
    api_prefix: str = "/api"
    cors_origins: list = None

    def __post_init__(self):
        if self.cors_origins is None:
            self.cors_origins = ["http://localhost:5173", "http://127.0.0.1:5173"]


def _parse_db_port() -> int:
    db_port = os.getenv("DB_PORT", "3306")
    try:
        return int(db_port)
    except (TypeError, ValueError):
        logger.warning("Invalid DB_PORT=%r, fallback to 3306", db_port)
        return 3306


def get_db_config() -> DatabaseConfig:
    """从环境变量获取数据库配置"""
    return DatabaseConfig(
        host=os.getenv("DB_HOST", "localhost"),
        port=_parse_db_port(),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "cooling_system_v2"),
    )


def get_app_config() -> AppConfig:
    """从环境变量获取应用配置"""
    return AppConfig(
        debug=os.getenv("DEBUG", "false").lower() == "true",
    )
