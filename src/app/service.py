from common.rabbitmq import publish_task
from common.redis_client import redis_client
from common.logger import setup_logger
from common.config import *


logger = setup_logger(__name__)


def dispatch_task(ch, task: dict):
    task_id = task["task_id"]
    task_type = task["task_type"]
    for keyword in task["keywords"]:
        dedup_key = f"task:dedup:{task_id}"

        if task_type == 0 and redis_client.sismember(dedup_key, keyword):
            continue

        redis_client.sadd(dedup_key, keyword)
        redis_client.expire(dedup_key, 7 * 24 * 3600)

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
