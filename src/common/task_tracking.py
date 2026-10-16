"""Durable task-run state and a database-backed RabbitMQ outbox."""

import hashlib
import json
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from common.config import MAX_TASK_ATTEMPTS, MYSQL_DB, MYSQL_HOST, MYSQL_PORT, MYSQL_PWD, MYSQL_USER
from common.logger import setup_logger

logger = setup_logger("task_tracking")

engine = create_engine(
    URL.create(
        "mysql+pymysql",
        username=MYSQL_USER,
        password=MYSQL_PWD,
        host=MYSQL_HOST or None,
        port=MYSQL_PORT,
        database=MYSQL_DB or None,
        query={"charset": "utf8mb4"},
    ),
    pool_pre_ping=True,
    pool_recycle=1800,
)


def ensure_tracking_schema() -> None:
    statements = (
        """
        CREATE TABLE IF NOT EXISTS task_runs (
            run_id CHAR(36) NOT NULL PRIMARY KEY,
            task_id VARCHAR(255) NOT NULL,
            task_type TINYINT NOT NULL,
            status VARCHAR(24) NOT NULL,
            idempotency_key VARCHAR(255) NULL,
            request_hash CHAR(64) NULL,
            total_items INT NOT NULL DEFAULT 0,
            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            started_at DATETIME(6) NULL,
            heartbeat_at DATETIME(6) NULL,
            finished_at DATETIME(6) NULL,
            UNIQUE KEY uq_task_run_idempotency (task_id, idempotency_key),
            KEY idx_task_runs_task_created (task_id, created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS task_run_items (
            item_id CHAR(36) NOT NULL PRIMARY KEY,
            run_id CHAR(36) NOT NULL,
            task_id VARCHAR(255) NOT NULL,
            source VARCHAR(64) NOT NULL,
            keyword VARCHAR(200) NOT NULL,
            status VARCHAR(24) NOT NULL,
            attempt_count INT NOT NULL DEFAULT 0,
            max_attempts INT NOT NULL DEFAULT 5,
            progress INT NOT NULL DEFAULT 0,
            current_stage VARCHAR(128) NULL,
            error_message TEXT NULL,
            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            queued_at DATETIME(6) NULL,
            started_at DATETIME(6) NULL,
            heartbeat_at DATETIME(6) NULL,
            finished_at DATETIME(6) NULL,
            UNIQUE KEY uq_run_source_keyword (run_id, source, keyword),
            KEY idx_task_items_task_created (task_id, created_at),
            KEY idx_task_items_run_status (run_id, status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS dispatch_outbox (
            outbox_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            item_id CHAR(36) NOT NULL,
            exchange_name VARCHAR(255) NOT NULL,
            routing_key VARCHAR(255) NOT NULL,
            payload LONGTEXT NOT NULL,
            attempts INT NOT NULL DEFAULT 0,
            next_attempt_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            published_at DATETIME(6) NULL,
            last_error TEXT NULL,
            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            UNIQUE KEY uq_outbox_item (item_id),
            KEY idx_outbox_pending (published_at, next_attempt_at, outbox_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
    )
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))
        item_columns = set(
            connection.execute(
                text("""
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA=:database AND TABLE_NAME='task_run_items'
                """),
                {"database": MYSQL_DB},
            ).scalars()
        )
        if "heartbeat_at" not in item_columns:
            connection.execute(text("ALTER TABLE task_run_items ADD COLUMN heartbeat_at DATETIME(6) NULL"))
        run_columns = set(
            connection.execute(
                text("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=:database AND TABLE_NAME='task_runs'"),
                {"database": MYSQL_DB},
            ).scalars()
        )
        if "request_hash" not in run_columns:
            connection.execute(text("ALTER TABLE task_runs ADD COLUMN request_hash CHAR(64) NULL"))


class IdempotencyConflict(ValueError):
    """An idempotency key was reused with a different request body."""


def create_task_run(
    task: Dict[str, Any],
    sources: Iterable[tuple[str, str]],
    exchange: str,
    idempotency_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Atomically record a run, its child items, and messages to publish."""
    source_list = list(sources)
    request_payload = {"task": task, "sources": source_list, "exchange": exchange}
    request_hash = hashlib.sha256(
        json.dumps(request_payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    if idempotency_key:
        with engine.connect() as connection:
            existing = connection.execute(
                text("SELECT run_id, request_hash FROM task_runs WHERE task_id=:task_id AND idempotency_key=:key"),
                {"task_id": task["task_id"], "key": idempotency_key},
            ).first()
        if existing:
            if existing.request_hash != request_hash:
                raise IdempotencyConflict("Idempotency-Key already belongs to a different request")
            return get_run_status(str(existing.run_id))

    run_id = str(uuid.uuid4())
    items = []
    for keyword in task["keywords"]:
        for source, routing_key in source_list:
            items.append(
                {
                    "item_id": str(uuid.uuid4()),
                    "run_id": run_id,
                    "task_id": task["task_id"],
                    "source": source,
                    "keyword": keyword,
                    "routing_key": routing_key,
                }
            )

    with engine.begin() as connection:
        connection.execute(
            text("""
                INSERT INTO task_runs (run_id, task_id, task_type, status, idempotency_key, request_hash, total_items)
                VALUES (:run_id, :task_id, :task_type, 'pending', :idempotency_key, :request_hash, :total_items)
            """),
            {
                "run_id": run_id,
                "task_id": task["task_id"],
                "task_type": task["task_type"],
                "idempotency_key": idempotency_key,
                "request_hash": request_hash,
                "total_items": len(items),
            },
        )
        for item in items:
            payload = {
                **task,
                "run_id": item["run_id"],
                "item_id": item["item_id"],
                "source": item["source"],
                "keyword": item["keyword"]
            }
            connection.execute(
                text("""
                    INSERT INTO task_run_items
                        (item_id, run_id, task_id, source, keyword, status, max_attempts)
                    VALUES
                        (:item_id, :run_id, :task_id, :source, :keyword, 'pending', :max_attempts)
                """),
                {**item, "max_attempts": MAX_TASK_ATTEMPTS},
            )
            connection.execute(
                text("""
                    INSERT INTO dispatch_outbox (item_id, exchange_name, routing_key, payload)
                    VALUES (:item_id, :exchange_name, :routing_key, :payload)
                """),
                {
                    "item_id": item["item_id"],
                    "exchange_name": exchange,
                    "routing_key": item["routing_key"],
                    "payload": json.dumps(payload, ensure_ascii=False),
                },
            )
    logger.info("Created task run task_id=%s run_id=%s child_items=%s", task["task_id"], run_id, len(items))
    return get_run_status(run_id)


def dispatch_outbox(publish, batch_size: int = 50) -> int:
    """Publish due outbox rows. Failed rows stay durable and retry with backoff."""
    with engine.connect() as connection:
        rows = connection.execute(
            text("""
                SELECT outbox_id, item_id, exchange_name, routing_key, payload, attempts
                FROM dispatch_outbox
                WHERE published_at IS NULL AND next_attempt_at <= CURRENT_TIMESTAMP(6)
                ORDER BY outbox_id LIMIT :batch_size
            """),
            {"batch_size": batch_size},
        ).mappings().all()

    sent = 0
    for row in rows:
        try:
            publish(row["exchange_name"], row["routing_key"], json.loads(row["payload"]))
            with engine.begin() as connection:
                connection.execute(
                    text("UPDATE dispatch_outbox SET published_at=CURRENT_TIMESTAMP(6), last_error=NULL WHERE outbox_id=:id"),
                    {"id": row["outbox_id"]},
                )
                connection.execute(
                    text("UPDATE task_run_items SET status='queued', queued_at=CURRENT_TIMESTAMP(6) WHERE item_id=:item_id AND status='pending'"),
                    {"item_id": row["item_id"]},
                )
                _refresh_run(connection, row["item_id"])
            sent += 1
        except Exception as exc:
            attempts = int(row["attempts"]) + 1
            delay = min(300, 2 ** min(attempts, 8))
            with engine.begin() as connection:
                connection.execute(
                    text("""
                        UPDATE dispatch_outbox
                        SET attempts=:attempts, next_attempt_at=:next_attempt_at, last_error=:error
                        WHERE outbox_id=:id AND published_at IS NULL
                    """),
                    {
                        "attempts": attempts,
                        "next_attempt_at": datetime.now() + timedelta(seconds=delay),
                        "error": str(exc)[:2000],
                        "id": row["outbox_id"],
                    },
                )
            logger.warning(
                "Outbox publish failed item_id=%s attempt=%s retry_in=%ss error_type=%s",
                row["item_id"], attempts, delay, type(exc).__name__
            )
    return sent


def mark_item_started(item_id: str, redelivered: bool = False) -> bool:
    with engine.begin() as connection:
        result = connection.execute(
            text("""
                UPDATE task_run_items
                SET status='running', attempt_count=attempt_count+1,
                    started_at=COALESCE(started_at, CURRENT_TIMESTAMP(6)),
                    heartbeat_at=CURRENT_TIMESTAMP(6), current_stage='starting'
                WHERE item_id=:item_id
                  AND (status IN ('pending','queued','retrying')
                       OR (status='running' AND (:redelivered=1 OR heartbeat_at < DATE_SUB(CURRENT_TIMESTAMP(6), INTERVAL 10 MINUTE))))
            """),
            {"item_id": item_id, "redelivered": int(redelivered)},
        )
        if result.rowcount != 1:
            return False
        connection.execute(
            text("UPDATE task_runs SET status='running', started_at=COALESCE(started_at, CURRENT_TIMESTAMP(6)) WHERE run_id=(SELECT run_id FROM task_run_items WHERE item_id=:item_id) AND status NOT IN ('succeeded','failed','cancelled','partial')"),
            {"item_id": item_id},
        )
        return True


def mark_item_progress(item_id: str, progress: int, stage: str) -> None:
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE task_run_items SET progress=:progress, current_stage=:stage, heartbeat_at=CURRENT_TIMESTAMP(6) WHERE item_id=:item_id AND status='running'"),
            {"item_id": item_id, "progress": max(0, min(100, progress)), "stage": stage[:128]},
        )


def mark_item_retry(item_id: str, error: str) -> None:
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE task_run_items SET status='retrying', error_message=:error, current_stage='retrying' WHERE item_id=:item_id AND status NOT IN ('succeeded','failed','cancelled')"),
            {"item_id": item_id, "error": error[:4000]},
        )
        _refresh_run(connection, item_id)


def finish_item(item_id: str, status: str, error: Optional[str] = None) -> None:
    if status not in {"succeeded", "failed", "cancelled"}:
        raise ValueError("terminal status must be succeeded, failed, or cancelled")
    with engine.begin() as connection:
        connection.execute(
            text("""
                UPDATE task_run_items
                SET status=:status, progress=IF(:status='succeeded',100,progress),
                    current_stage=:status, error_message=:error, finished_at=CURRENT_TIMESTAMP(6)
                WHERE item_id=:item_id AND status NOT IN ('succeeded','failed','cancelled')
            """),
            {"item_id": item_id, "status": status, "error": error[:4000] if error else None},
        )
        _refresh_run(connection, item_id)


def cancel_task(task_id: str) -> None:
    with engine.begin() as connection:
        connection.execute(
            text("""
                UPDATE task_run_items SET status='cancelled', current_stage='cancelled',
                    finished_at=CURRENT_TIMESTAMP(6)
                WHERE task_id=:task_id AND status IN ('pending','queued','running','retrying')
            """),
            {"task_id": task_id},
        )
        connection.execute(
            text("""
                UPDATE dispatch_outbox o JOIN task_run_items i ON i.item_id=o.item_id
                SET o.published_at=COALESCE(o.published_at,CURRENT_TIMESTAMP(6)), o.last_error='cancelled'
                WHERE i.task_id=:task_id AND i.status='cancelled'
            """),
            {"task_id": task_id},
        )
        run_ids = connection.execute(
            text("SELECT DISTINCT run_id FROM task_run_items WHERE task_id=:task_id"),
            {"task_id": task_id},
        ).scalars().all()
        for run_id in run_ids:
            _refresh_run_by_id(connection, run_id)


def _refresh_run(connection, item_id: str) -> None:
    run_id = connection.execute(
        text("SELECT run_id FROM task_run_items WHERE item_id=:item_id"), {"item_id": item_id}
    ).scalar()
    if run_id:
        _refresh_run_by_id(connection, run_id)


def _refresh_run_by_id(connection, run_id: str) -> None:
    statuses = connection.execute(
        text("SELECT status FROM task_run_items WHERE run_id=:run_id"), {"run_id": run_id}
    ).scalars().all()
    if not statuses:
        status = "succeeded"
    elif any(value in {"pending", "queued", "running", "retrying"} for value in statuses):
        status = "running" if "running" in statuses or "retrying" in statuses else "queued"
    elif all(value == "succeeded" for value in statuses):
        status = "succeeded"
    elif all(value == "cancelled" for value in statuses):
        status = "cancelled"
    elif all(value == "failed" for value in statuses):
        status = "failed"
    else:
        status = "partial"
    terminal = status in {"succeeded", "failed", "cancelled", "partial"}
    connection.execute(
        text("""
            UPDATE task_runs SET status=:status,
                finished_at=IF(:terminal, CURRENT_TIMESTAMP(6), NULL)
            WHERE run_id=:run_id
        """),
        {"run_id": run_id, "status": status, "terminal": terminal},
    )


def get_run_status(run_id: str) -> Dict[str, Any]:
    with engine.connect() as connection:
        run = connection.execute(
            text("SELECT * FROM task_runs WHERE run_id=:run_id"), {"run_id": run_id}
        ).mappings().first()
        if not run:
            return {}
        items = connection.execute(
            text("""
                SELECT item_id, source, keyword, status, attempt_count, max_attempts,
                       progress, current_stage, error_message, created_at, queued_at,
                       started_at, heartbeat_at, finished_at
                FROM task_run_items WHERE run_id=:run_id ORDER BY created_at, item_id
            """),
            {"run_id": run_id},
        ).mappings().all()
        return {**dict(run), "items": [dict(item) for item in items]}


def get_task_status(task_id: str, runs_limit: int = 10) -> Optional[Dict[str, Any]]:
    with engine.connect() as connection:
        task = connection.execute(
            text("SELECT task_id, task_type, keywords, priority, max_page, request_interval, status FROM tasks WHERE task_id=:task_id"),
            {"task_id": task_id},
        ).mappings().first()
        if not task:
            return None
        runs = connection.execute(
            text("SELECT run_id FROM task_runs WHERE task_id=:task_id ORDER BY created_at DESC LIMIT :limit"),
            {"task_id": task_id, "limit": runs_limit},
        ).scalars().all()
        return {"task": dict(task), "runs": [get_run_status(run_id) for run_id in runs]}


def get_active_items(source: str, limit: int = 20) -> list[Dict[str, Any]]:
    with engine.connect() as connection:
        rows = connection.execute(
            text("""
                SELECT item_id, task_id, run_id, source, keyword, status, progress, current_stage
                FROM task_run_items
                WHERE source=:source AND status='running'
                ORDER BY started_at LIMIT :limit
            """),
            {"source": source, "limit": limit},
        ).mappings().all()
        return [dict(row) for row in rows]


def is_item_cancelled(item_id: str) -> bool:
    with engine.connect() as connection:
        cancelled = connection.execute(
            text("SELECT status='cancelled' FROM task_run_items WHERE item_id=:item_id"),
            {"item_id": item_id},
        ).scalar()
        return bool(cancelled)
