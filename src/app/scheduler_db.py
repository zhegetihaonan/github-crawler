import json

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from common.config import MYSQL_DB, MYSQL_HOST, MYSQL_PORT, MYSQL_PWD, MYSQL_USER

DB_URL = URL.create(
    "mysql+pymysql",
    username=MYSQL_USER,
    password=MYSQL_PWD,
    host=MYSQL_HOST or None,
    port=MYSQL_PORT,
    database=MYSQL_DB or None,
    query={"charset": "utf8mb4"},
)
engine = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=1800)


def save_cycle_task(task):
    """新增或更新任务；重试同一 task_id 不会因唯一键冲突失败。"""
    values = task if isinstance(task, dict) else task.model_dump()
    sql = """
    INSERT INTO tasks
    (task_id,task_type,keywords,priority,max_page,request_interval,status)
    VALUES
    (:task_id,:task_type,:keywords,:priority,:max_page,:request_interval,1)
    ON DUPLICATE KEY UPDATE
        task_type=VALUES(task_type), keywords=VALUES(keywords),
        priority=VALUES(priority), max_page=VALUES(max_page),
        request_interval=VALUES(request_interval), status=1
    """
    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "task_id": values["task_id"],
                "task_type": values["task_type"],
                "keywords": json.dumps(values["keywords"], ensure_ascii=False),
                "priority": values["priority"],
                "max_page": values["max_page"],
                "request_interval": values["request_interval"],
            }
        )


def get_active_cycle_tasks():
    """从数据库获取所有启用的周期任务"""
    sql = """
    SELECT task_id, task_type, keywords, priority, max_page, request_interval
    FROM tasks
    WHERE task_type = 1 AND status = 1 order by priority desc
    """
    with engine.connect() as conn:
        return [
            {
                "task_id": row["task_id"],
                "task_type": row["task_type"],
                "keywords": json.loads(row["keywords"]),
                "priority": row["priority"],
                "max_page": row["max_page"],
                "request_interval": row["request_interval"],
            }
            for row in conn.execute(text(sql)).mappings()
        ]
