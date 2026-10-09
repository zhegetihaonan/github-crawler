import threading
import time
import requests
import socket
from urllib.parse import urlparse
from common.rabbitmq import get_channel
from common.logger import setup_logger
from common.config import *

logger = setup_logger('crawler_status')

node_id = socket.gethostname()


def get_host_ip():
    targets = []
    status_host = urlparse(STATUS_URL).hostname
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


def current_task_id():
    return 'test'


def get_status(channel, queue):
    q = channel.queue_declare(queue=queue, durable=True, arguments={"x-max-priority": 10})
    count = q.method.message_count
    return "running" if count else "idle", count


def report_status():
    connection, channel = get_channel()
    while True:
        for queue in [QUEUE_GITHUB, QUEUE_WENKU]:
            status, task_count = get_status(channel, queue)
            payload = {
                "node_id": node_id,
                "engine_type": f"surface_web_{queue.split('.')[-1]}",
                "source_name": queue.split('.')[-1],
                "ip_addres": ip,
                "port": 8008,
                "task_count": task_count,
                "task_status": [
                    {
                        "current_task_id": status,
                        "current_task_progress": 0
                    }
                ]
            }

            try:
                requests.put(STATUS_URL, json=payload, timeout=5)
                logger.info(f'send heartbeat: ---> {payload}')
            except Exception:
                pass
        time.sleep(60)


if __name__ == '__main__':
    report_status()
