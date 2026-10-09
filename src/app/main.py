from fastapi import FastAPI, HTTPException, Query
from common.rabbitmq import publish_task
from common.redis_client import redis_client
from schemas import TaskRequest
from common.logger import setup_logger
from common.config import *
import pika
from sqlalchemy import create_engine, text
import json
import threading
from proxy.manager import get_proxy, fail
from proxy.fetcher import run as fetcher_run
from proxy.checker import run as checker_run
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from scheduler_db import engine, save_cycle_task, get_active_cycle_tasks

logger = setup_logger(__name__)

app = FastAPI(title="Surface Web Task Dispatcher")
scheduler = BackgroundScheduler(
    job_defaults={
        "max_instances": 3,
        "misfire_grace_time": 86400,
        "coalesce": True
    }
)


# 用来记录已经注册的周期任务id
registered_tasks = set()


# 启动时加载数据库周期任务
def load_tasks_from_db():
    tasks = get_active_cycle_tasks()
    logger.info(f"加载周期任务： {len(tasks)}")
    for task in tasks:
        if task["task_id"] not in registered_tasks:
            scheduler.add_job(
                dispatch_task,
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
    task_id = task["task_id"]
    if redis_client.sismember("crawler:task:stopped", task_id):
        logger.info(f"任务已停止下发: {task_id}")
        return
    for keyword in task["keywords"]:
        dedup_key = f"task:dedup:{task_id}"

        if redis_client.sismember(dedup_key, keyword):
            continue

        redis_client.sadd(dedup_key, keyword)
        redis_client.expire(dedup_key, DEDUP_TIMEOUT)

        sub_task = {
            "task_id": task_id,
            "task_type": task["task_type"],
            "keyword": keyword,
            "priority": task["priority"],
            "max_page": task["max_page"],
            "request_interval": task["request_interval"],
        }

        for queue in [QUEUE_GITHUB, QUEUE_WENKU]:
            publish_task(TASK_EXCHANGE, queue, sub_task, priority=task["priority"])

            logger.info(f"下发子任务: {sub_task} ------> {queue}")


@app.on_event("startup")
def startup_event():
    scheduler.start()
    scheduler.add_job(
        lambda: logger.info("scheduler alive"),
        "interval",
        seconds=300
    )
    logger.info("启动 scheduler")
    load_tasks_from_db()
    logger.info("Scheduler 启动完成")
    # 订阅更新线程
    threading.Thread(target=fetcher_run, daemon=True).start()

    # 健康检测线程
    threading.Thread(target=checker_run, daemon=True).start()


@app.post("/crawler/task")
def dispatch(task: TaskRequest):
    try:
        if task.task_type == 0:
            save_cycle_task(task)
            dispatch_task(task.dict())
            logger.info(f"新增单次任务: {task.task_id}")
            return {"code": 201, "message": f"单次任务 {task.task_id} 创建成功"}

        elif task.task_type == 1:
            # 周期任务
            save_cycle_task(task)
            # 注册到 scheduler
            if task.task_id not in registered_tasks:
                # 立即执行一次
                dispatch_task(task.dict())
                scheduler.add_job(
                    dispatch_task,
                    trigger=IntervalTrigger(seconds=task.request_interval),
                    args=[task.dict()],
                    id=task.task_id,
                    # next_run_time=datetime.now(),
                    replace_existing=True
                )
                registered_tasks.add(task.task_id)
                logger.info(f"新增周期任务: {task.task_id}")
                return {"code": 201, "message": f"周期任务 {task.task_id} 创建成功"}

        logger.info(f'任务下发成功: {task}')
        return {
            "code": 201,
            "message": "任务创建成功"
        }
    except Exception as e:
        logger.error(f"任务下发失败: {e}")
        raise HTTPException(status_code=500, detail="任务下发失败")


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
    logger.info(f"收到结果上报: {data}")
    return {"code": 200, "message": "ok"}


@app.get("/proxy")
def proxy():

    p = get_proxy()

    return {"proxy": p}


@app.post("/proxy/fail")
def proxy_fail(proxy: str):

    fail(proxy)

    return {"ok": True}
