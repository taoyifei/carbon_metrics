"""
数据库配置和工具模块
"""
from __future__ import annotations

import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict

import pymysql

LOGGER = logging.getLogger("pipeline_v2")


def setup_logging() -> None:
    """配置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def get_db_config() -> Dict[str, Any]:
    """获取数据库配置"""
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "3306")),
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASSWORD", ""),
        "database": os.getenv("DB_NAME", "cooling_system_v2"),
        "charset": "utf8mb4",
        "autocommit": False,
        "cursorclass": pymysql.cursors.DictCursor,
    }


def get_connection() -> pymysql.Connection:
    """获取数据库连接"""
    return pymysql.connect(**get_db_config())


def execute_sql_file(sql_file: Path) -> None:
    """执行SQL文件"""
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")

    sql_content = sql_file.read_text(encoding="utf-8")

    # 提取 DELIMITER // ... DELIMITER ; 块（触发器定义）
    delimiter_pattern = re.compile(
        r'DELIMITER\s+//\s*(.*?)\s*DELIMITER\s*;',
        re.DOTALL | re.IGNORECASE
    )
    trigger_blocks = delimiter_pattern.findall(sql_content)
    sql_content = delimiter_pattern.sub('', sql_content)

    # 分割普通语句
    statements = [s.strip() for s in sql_content.split(";") if s.strip()]

    config = get_db_config()
    config["database"] = None
    with pymysql.connect(**config) as conn:
        with conn.cursor() as cursor:
            for statement in statements:
                if not statement or statement.upper().startswith('DELIMITER'):
                    continue
                try:
                    cursor.execute(statement)
                except Exception as exc:
                    LOGGER.warning("SQL failed: %s", exc)

            # 处理触发器块
            for block in trigger_blocks:
                # 提取每个 CREATE TRIGGER ... END// 语句
                trigger_pattern = re.compile(
                    r'(CREATE\s+TRIGGER\s+\w+.*?END)\s*//',
                    re.DOTALL | re.IGNORECASE
                )
                for match in trigger_pattern.finditer(block):
                    trigger_sql = match.group(1)
                    try:
                        cursor.execute(trigger_sql)
                    except Exception as exc:
                        LOGGER.warning("Trigger failed: %s", exc)
        conn.commit()
