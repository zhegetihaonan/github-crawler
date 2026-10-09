import json
import random
import re
import threading
import time
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import boto3
import pika
import requests
import yaml
from botocore.exceptions import ClientError

from common.config import *
from common.logger import setup_logger
from common.redis_client import redis_client


logger = setup_logger("crawler_wenku_optimized")

PROXY_CONFIG_PATH = Path(__file__).with_name("proxies.yaml")
STOPPED_TASK_SET = "crawler:task:stopped"
DEDUP_KEY_PREFIX = "crawler:dedup:wenku"
HTTP_RETRY_STATUS = {403, 408, 429, 500, 502, 503, 504}
DEFAULT_MAX_PAGE = 10
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
)

s3 = None
if MINIO_ENDPOINT and MINIO_ACCESS_KEY and MINIO_SECRET_KEY:
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


class Crawler(threading.Thread):
    def __init__(self, cid: str):
        super().__init__(name=f"wenku-crawler-{cid}")
        self.cid = cid
        self._running = threading.Event()
        self._running.set()
        self.connection = None
        self.ch_consume = None
        self.ch_publish = None
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.proxies = self.load_proxies()
        self.ensure_minio_bucket(MINIO_BUCKET_WENKU)
        logger.info("crawler=%s proxy_count=%s", self.cid, len(self.proxies))

    def create_connection(self):
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300,
                socket_timeout=300,
                connection_attempts=5,
                retry_delay=3,
            )
        )

        self.ch_consume = self.connection.channel()
        self.ch_publish = self.connection.channel()

        self.ch_consume.exchange_declare(exchange=TASK_EXCHANGE, exchange_type="topic", durable=True)
        self.ch_consume.queue_declare(queue=QUEUE_WENKU, durable=True, arguments={"x-max-priority": 10})
        self.ch_consume.queue_bind(exchange=TASK_EXCHANGE, queue=QUEUE_WENKU, routing_key=QUEUE_WENKU)
        self.ch_consume.basic_qos(prefetch_count=1)

        self.ch_publish.exchange_declare(exchange=RESULT_EXCHANGE, exchange_type="topic", durable=True)
        self.ch_publish.queue_declare(queue=QUEUE_RESULT, durable=True, arguments={"x-max-priority": 10})
        self.ch_publish.queue_bind(exchange=RESULT_EXCHANGE, queue=QUEUE_RESULT, routing_key=QUEUE_RESULT)

    def disconnect_from_rabbitmq(self):
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception:
            logger.exception("crawler=%s failed closing RabbitMQ connection", self.cid)

    def publish_result(self, records: List[Dict]):
        if not records:
            return
        for record in records:
            record.setdefault("crawler_id", str(self.cid))
            self.ch_publish.basic_publish(
                exchange=RESULT_EXCHANGE,
                routing_key=QUEUE_RESULT,
                body=json.dumps(record, ensure_ascii=False),
                properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
            )
        logger.info("crawler=%s published %s results", self.cid, len(records))

    @staticmethod
    def ensure_minio_bucket(bucket_name: Optional[str]):
        if not s3 or not bucket_name:
            logger.warning("MinIO is not fully configured; file uploads will be skipped")
            return
        try:
            s3.head_bucket(Bucket=bucket_name)
        except ClientError:
            s3.create_bucket(Bucket=bucket_name)
            logger.info("created MinIO bucket: %s", bucket_name)

    @staticmethod
    def upload_to_minio(bucket_name: Optional[str], object_name: str, content: bytes) -> str:
        if not s3 or not bucket_name:
            return ""
        for attempt in range(1, 4):
            try:
                s3.put_object(Bucket=bucket_name, Key=object_name, Body=content)
                return f"{MINIO_ENDPOINT.rstrip('/')}/{bucket_name}/{object_name}"
            except ClientError as exc:
                logger.warning("MinIO upload failed attempt=%s object=%s error=%s", attempt, object_name, exc)
                time.sleep(2)
        return ""

    def crawl(self, task: Dict):
        pass

    def run(self):
        time.sleep(random.randint(1, 5))
        try:
            self.create_connection()
            logger.info("crawler=%s started, waiting for wenku tasks", self.cid)
            while self._running.is_set():
                method, properties, body = self.ch_consume.basic_get(queue=QUEUE_WENKU, auto_ack=False)
                if method is None:
                    time.sleep(1)
                    continue
                task = {}
                try:
                    task = json.loads(body)
                    logger.info("crawler=%s processing task=%s", self.cid, task)
                    self.crawl(task)
                    logger.info("crawler=%s task done task=%s", self.cid, task)
                except Exception:
                    logger.exception("crawler=%s task failed task=%s error=%s", self.cid, task, traceback.format_exc())
                finally:
                    self.ch_consume.basic_ack(method.delivery_tag)
        except Exception:
            logger.exception("crawler=%s crashed", self.cid)
        finally:
            self.disconnect_from_rabbitmq()
            self.session.close()

    def stop(self):
        self._running.clear()


def main():
    threads: List[Crawler] = []
    for _ in range(THREAD_NUM):
        thread = Crawler(str(uuid.uuid4()))
        threads.append(thread)
        thread.start()
        time.sleep(1)

    while True:
        time.sleep(5)
        for thread in threads[:]:
            if thread.is_alive():
                continue
            logger.info("crawler thread %s stopped; restarting", thread.name)
            thread.stop()
            thread.join(timeout=5)
            new_thread = Crawler(str(uuid.uuid4()))
            threads.remove(thread)
            threads.append(new_thread)
            new_thread.start()


if __name__ == "__main__":
    main()  # 涉密省略关键代码
