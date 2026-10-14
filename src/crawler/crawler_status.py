import time
import requests
import socket
from urllib.parse import urlparse
from common.rabbitmq import get_channel
from common.logger import setup_logger
from common.config import *
from common.task_tracking import get_active_items

logger = setup_logger('crawler_status')

node_id = socket.gethostname()


def get_host_ip():
    targets = []
    status_host = urlparse(STATUS_URL).hostname if STATUS_URL else None
    if status_host:
        targets.append((status_host, 80))
    if RABBITMQ_HOST:
        targets.append((RABBITMQ_HOST, RABBITMQ_PORT))
    targets.append(("8.8.8.8", 80))

    for host, port in targets:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.connect((host, int(port)))
                ip_addr = sock.getsockname()[0]
                if ip_addr and not ip_addr.startswith("127."):
                    return ip_addr
        except Exception:
            continue

    try:
        ip_addr = socket.gethostbyname(socket.gethostname())
        if ip_addr and not ip_addr.startswith("127."):
            return ip_addr
    except Exception:
        pass

    return IP


ip = get_host_ip()


def get_status(channel, queue):
    q = channel.queue_declare(queue=queue, durable=True, arguments={"x-max-priority": 10})
    count = q.method.message_count
    return "running" if count else "idle", count


def report_status():
    connection, channel = get_channel()
    while True:
        for queue in [QUEUE_GITHUB, QUEUE_WENKU]:
            _, queued_count = get_status(channel, queue)
            source = queue.split('.')[-1]
            try:
                active_items = get_active_items(source)
            except Exception:
                logger.exception("failed reading active task state source=%s", source)
                active_items = []
            task_status = [
                {
                    "current_task_id": item["task_id"],
                    "current_task_progress": item["progress"],
                }
                for item in active_items
            ]
            if not task_status:
                task_status = [{"current_task_id": "idle", "current_task_progress": 0}]
            payload = {
                "node_id": node_id,
                "engine_type": f"surface_web_{source}",
                "source_name": source,
                "ip_addres": ip,
                "port": 8008,
                "task_count": queued_count + len(active_items),
                "task_status": task_status,
            }

            try:
                if not STATUS_URL:
                    logger.debug("STATUS_URL is not configured; skipping heartbeat")
                    continue
                requests.put(STATUS_URL, json=payload, timeout=5)
                logger.info(f'send heartbeat: ---> {payload}')
            except requests.RequestException:
                logger.exception("heartbeat delivery failed queue=%s", queue)
        time.sleep(60)


if __name__ == '__main__':
    report_status()
