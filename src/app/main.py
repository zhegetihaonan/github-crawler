from datetime import datetime
import hmac
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from common.redis_client import redis_client
from schemas import TaskRequest
from common.logger import setup_logger
from common.config import (
    API_KEY,
    ENABLE_WENKU_CRAWLER,
    PROXY_URL,
    QUEUE_GITHUB,
    QUEUE_WENKU,
    TASK_EXCHANGE,
)
from common.rabbitmq import publish_task
from common.task_tracking import (
    IdempotencyConflict,
    cancel_task as cancel_tracked_task,
    create_task_run,
    dispatch_outbox,
    ensure_tracking_schema,
    get_run_status,
    get_task_status,
)
from sqlalchemy import text
import threading
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from scheduler_db import engine, save_cycle_task, get_active_cycle_tasks

try:
    from proxy.manager import get_proxy, fail
    from proxy.fetcher import run as fetcher_run
    from proxy.checker import run as checker_run
except ModuleNotFoundError:
    get_proxy = fail = fetcher_run = checker_run = None

logger = setup_logger(__name__)


def require_api_key(x_api_key: Optional[str] = Header(None, alias="X-API-Key")):
    if API_KEY and (not x_api_key or not hmac.compare_digest(x_api_key, API_KEY)):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


app = FastAPI(title="Surface Web Task Dispatcher", dependencies=[Depends(require_api_key)])
scheduler = BackgroundScheduler(
    job_defaults={
        "max_instances": 3,
        "misfire_grace_time": 86400,
        "coalesce": True
    }
)


# 用来记录已经注册的周期任务 id
registered_tasks = set()


# 启动时加载数据库周期任务
def load_tasks_from_db():
    tasks = get_active_cycle_tasks()
    logger.info(f"加载周期任务： {len(tasks)}")
    for task in tasks:
        if task["task_id"] not in registered_tasks:
            scheduler.add_job(
                schedule_task_run,
                trigger=IntervalTrigger(seconds=task["request_interval"]),
                args=[task],
                id=task["task_id"],
                next_run_time=datetime.now(),
                replace_existing=True
            )
            registered_tasks.add(task["task_id"])
            # logger.info(f"已注册周期任务: {task['task_id']}")
    for job in scheduler.get_jobs():
        logger.info(
            f"周期任务 {job.id} 下次执行时间: {job.next_run_time}"
        )


def dispatch_task(task: dict):
    """Create a durable run; the outbox publisher delivers its child tasks."""
    if redis_client.sismember("crawler:task:stopped", task["task_id"]):
        logger.info("Skipped scheduled run for stopped task task_id=%s", task["task_id"])
        return None
    return create_task_run(task, task_sources(), TASK_EXCHANGE)


def task_sources():
    sources = [("github", QUEUE_GITHUB)]
    if ENABLE_WENKU_CRAWLER:
        sources.append(("wenku", QUEUE_WENKU))
    return sources


def publish_pending_tasks():
    try:
        def publish_with_priority(exchange, routing_key, payload):
            publish_task(exchange, routing_key, payload, priority=int(payload.get("priority", 0)))

        count = dispatch_outbox(publish_with_priority)
        if count:
            logger.info("Published %s task messages from outbox", count)
    except Exception:
        logger.exception("Outbox dispatch cycle failed")


def schedule_task_run(task: dict):
    run = dispatch_task(task)
    if run:
        logger.info("Created scheduled task run task_id=%s run_id=%s", task["task_id"], run["run_id"])
    return run


@app.on_event("startup")
def startup_event():
    if not API_KEY:
        logger.warning("API_KEY is not configured; task API endpoints are unauthenticated")
    ensure_tracking_schema()
    scheduler.start()
    scheduler.add_job(
        lambda: logger.info("scheduler alive"),
        "interval",
        seconds=300
    )
    scheduler.add_job(publish_pending_tasks, "interval", seconds=1, id="task-outbox-publisher", max_instances=1)
    logger.info("启动 scheduler")
    load_tasks_from_db()
    logger.info("Scheduler 启动完成")
    # 订阅更新线程
    if fetcher_run and checker_run:
        threading.Thread(target=fetcher_run, daemon=True, name="proxy-fetcher").start()
        threading.Thread(target=checker_run, daemon=True, name="proxy-checker").start()


@app.post("/crawler/task")
def dispatch(task: TaskRequest, idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key", max_length=255)):
    try:
        if task.task_type == 0:
            save_cycle_task(task)
            redis_client.srem("crawler:task:stopped", task.task_id)
            existing_job = scheduler.get_job(task.task_id)
            if existing_job:
                scheduler.remove_job(task.task_id)
                registered_tasks.discard(task.task_id)
            run = create_task_run(task.model_dump(), task_sources(), TASK_EXCHANGE, idempotency_key)
            logger.info("Accepted one-time task task_id=%s run_id=%s", task.task_id, run["run_id"])
            return {"code": 202, "message": "任务已接受", "run_id": run["run_id"], "status": run["status"]}

        elif task.task_type == 1:
            save_cycle_task(task)
            redis_client.srem("crawler:task:stopped", task.task_id)
            run = schedule_task_run(task.model_dump())
            scheduler.add_job(
                schedule_task_run,
                trigger=IntervalTrigger(seconds=task.request_interval),
                args=[task.model_dump()],
                id=task.task_id,
                replace_existing=True,
            )
            registered_tasks.add(task.task_id)
            logger.info("Registered periodic task task_id=%s run_id=%s", task.task_id, run["run_id"])
            return {"code": 202, "message": "周期任务已接受", "run_id": run["run_id"], "status": run["status"]}
    except IdempotencyConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except Exception as e:
        logger.exception("Task submission failed task_id=%s error_type=%s", task.task_id, type(e).__name__)
        raise HTTPException(status_code=500, detail="任务下发失败")


@app.get("/crawler/task/{task_id}")
def task_status(task_id: str):
    result = get_task_status(task_id)
    if result is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    return result


@app.get("/crawler/task/{task_id}/runs/{run_id}")
def task_run_status(task_id: str, run_id: str):
    result = get_run_status(run_id)
    if not result or result["task_id"] != task_id:
        raise HTTPException(status_code=404, detail="任务运行记录不存在")
    return result


@app.delete("/crawler/task")
def delete_task(task_id: str = Query(...)):
    try:
        # 在缓存标记任务停止
        redis_client.sadd("crawler:task:stopped", task_id)
        redis_client.expire("crawler:task:stopped", 7 * 24 * 3600)
        logger.info(f"任务已在缓存标记停止: {task_id}")

        # 更新数据库 status=0
        sql = "UPDATE tasks SET status=0 WHERE task_id=:task_id"
        with engine.begin() as conn:
            conn.execute(text(sql), {"task_id": task_id})
        cancel_tracked_task(task_id)
        logger.info(f"任务已在数据库标记停止: {task_id}")

        # 如果是周期任务需要从 scheduler 移除
        job = scheduler.get_job(task_id)
        if job:
            scheduler.remove_job(task_id)
            registered_tasks.discard(task_id)
            logger.info(f"周期任务已从scheduler删除: {task_id}")
            return {"code": 200, "message": f"周期任务 {task_id} 删除成功"}
        else:
            logger.info(f"周期任务在scheduler中未找到: {task_id}")
            return {"code": 200, "message": f"单次任务 {task_id} 删除成功"}

    except Exception as e:
        logger.error(f"任务删除失败: {e}")
        raise HTTPException(status_code=500, detail="任务删除失败")


# mock 上报接口（为了链路先跑通）
@app.post("/mock/report")
def mock_report(data: dict):
    logger.info("Mock result received source=%s task_id=%s", data.get("source"), data.get("task_id"))
    return {"code": 200, "message": "ok"}


@app.get("/proxy")
def proxy():
    return {"proxy": get_proxy() if get_proxy else PROXY_URL}


@app.post("/proxy/fail")
def proxy_fail(proxy: str):
    if not fail:
        raise HTTPException(status_code=503, detail="Proxy manager is not installed")
    fail(proxy)
    return {"ok": True}
