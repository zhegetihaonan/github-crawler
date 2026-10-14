"""Idempotent MySQL persistence for crawler result messages."""

import hashlib
import json
import re
from typing import Any, Dict

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from common.config import (
    MYSQL_DB,
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_PWD,
    MYSQL_USER,
    RESULTS_DB_ENABLED,
    RESULTS_TABLE,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RESULT_FIELDS = (
    "run_id",
    "result_id",
    "task_id",
    "crawler_id",
    "source",
    "keyword",
    "name",
    "matched_field",
    "repo_url",
    "file_path",
    "file_minio_url",
    "file_name",
    "found_at",
)
_engine = None


def _table_name() -> str:
    if not _IDENTIFIER.fullmatch(RESULTS_TABLE):
        raise ValueError("RESULTS_TABLE must be a simple SQL identifier")
    return RESULTS_TABLE


def _get_engine():
    global _engine
    if _engine is None:
        missing = [
            name
            for name, value in (
                ("MYSQL_HOST", MYSQL_HOST),
                ("MYSQL_USER", MYSQL_USER),
                ("MYSQL_DB", MYSQL_DB),
            )
            if not value
        ]
        if missing:
            raise RuntimeError("Missing MySQL settings: " + ", ".join(missing))
        _engine = create_engine(
            URL.create(
                "mysql+pymysql",
                username=MYSQL_USER,
                password=MYSQL_PWD,
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                database=MYSQL_DB,
                query={"charset": "utf8mb4"},
            ),
            pool_pre_ping=True,
            pool_recycle=1800,
        )
    return _engine


def ensure_results_table() -> None:
    """Create the result table when database persistence is enabled."""
    if not RESULTS_DB_ENABLED:
        return
    table = _table_name()
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{table}` (
        record_hash CHAR(64) NOT NULL PRIMARY KEY,
        run_id CHAR(36) NULL,
        result_id CHAR(64) NULL,
        task_id VARCHAR(255) NULL,
        crawler_id VARCHAR(255) NULL,
        source VARCHAR(64) NULL,
        keyword TEXT NULL,
        name TEXT NULL,
        matched_field VARCHAR(255) NULL,
        repo_url TEXT NULL,
        file_path TEXT NULL,
        file_minio_url TEXT NULL,
        file_name TEXT NULL,
        found_at VARCHAR(64) NULL,
        payload_json LONGTEXT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        KEY idx_task_id (task_id),
        KEY idx_source (source)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    with _get_engine().begin() as connection:
        connection.execute(text(ddl))
        existing_columns = set(
            connection.execute(
                text("""
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA=:database AND TABLE_NAME=:table
                """),
                {"database": MYSQL_DB, "table": table},
            ).scalars()
        )
        if "run_id" not in existing_columns:
            connection.execute(text(f"ALTER TABLE `{table}` ADD COLUMN run_id CHAR(36) NULL"))
        if "result_id" not in existing_columns:
            connection.execute(text(f"ALTER TABLE `{table}` ADD COLUMN result_id CHAR(64) NULL"))


def save_result(data: Dict[str, Any]) -> bool:
    """Insert a result once. Returns False if DB persistence is disabled."""
    if not RESULTS_DB_ENABLED:
        return False

    table = _table_name()
    payload_json = json.dumps(data, ensure_ascii=False, sort_keys=True, default=str)
    identity = {field: data.get(field) for field in (
        "task_id", "source", "keyword", "name", "matched_field", "file_path", "repo_url"
    )}
    if data.get("run_id"):
        identity["run_id"] = data["run_id"]
    record_hash = hashlib.sha256(
        json.dumps(identity, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    values = {field: data.get(field) for field in _RESULT_FIELDS}
    values["result_id"] = data.get("result_id") or record_hash
    values.update(record_hash=record_hash, payload_json=payload_json)
    columns = ", ".join(f"`{key}`" for key in values)
    placeholders = ", ".join(f":{key}" for key in values)
    query = text(f"INSERT IGNORE INTO `{table}` ({columns}) VALUES ({placeholders})")
    with _get_engine().begin() as connection:
        connection.execute(query, values)
    return True
