from sqlalchemy import create_engine, text
import json
from common.config import *

# MySQL连接
DB_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PWD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
engine = create_engine(DB_URL, pool_pre_ping=True)


def save_cycle_task(task):
    """新增周期任务"""
    sql = """
    INSERT INTO tasks
    (task_id,task_type,keywords,priority,max_page,request_interval)
    VALUES
    (:task_id,:task_type,:keywords,:priority,:max_page,:request_interval)
    """
    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "task_id": task.task_id,
                "task_type": task.task_type,
                "keywords": json.dumps(task.keywords),
                "priority": task.priority,
                "max_page": task.max_page,
                "request_interval": task.request_interval
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
        result = conn.execute(text(sql))
        tasks = []
        for row in result:
            tasks.append({
                "task_id": row.task_id,
                "task_type": row.task_type,
                "keywords": json.loads(row.keywords),
                "priority": row.priority,
                "max_page": row.max_page,
                "request_interval": row.request_interval
            })
        return tasks
