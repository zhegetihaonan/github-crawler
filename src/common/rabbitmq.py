import pika
import json
import threading
from common.config import *
from common.logger import setup_logger
import time

logger = setup_logger(__name__)
_local = threading.local()


def _connect():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=60,
        socket_timeout=60,
        connection_attempts=3,
        retry_delay=2,
    )

    conn = pika.BlockingConnection(parameters)
    ch = conn.channel()
    ch.basic_qos(prefetch_count=1)

    # 声明交换机和队列（幂等）
    ch.exchange_declare(exchange=TASK_EXCHANGE, exchange_type="topic", durable=True)

    ch.queue_declare(queue=QUEUE_GITHUB, durable=True, arguments={"x-max-priority": 10})
    ch.queue_bind(exchange=TASK_EXCHANGE, queue=QUEUE_GITHUB, routing_key=QUEUE_GITHUB)

    ch.queue_declare(queue=QUEUE_WENKU, durable=True, arguments={"x-max-priority": 10})
    ch.queue_bind(exchange=TASK_EXCHANGE, queue=QUEUE_WENKU, routing_key=QUEUE_WENKU)

    ch.exchange_declare(exchange=RESULT_EXCHANGE, exchange_type="topic", durable=True)
    ch.queue_declare(queue=QUEUE_RESULT, durable=True, arguments={"x-max-priority": 10})
    ch.queue_bind(exchange=RESULT_EXCHANGE, queue=QUEUE_RESULT, routing_key=QUEUE_RESULT)

    return conn, ch


def get_channel():
    if not hasattr(_local, "conn"):
        _local.conn, _local.ch = _connect()
    else:
        # 这里非常关键
        if _local.conn.is_closed or _local.ch.is_closed:
            try:
                _local.conn.close()
            except:
                pass
            _local.conn, _local.ch = _connect()

    return _local.conn, _local.ch


def publish_task(exchange, routing_key, message: dict, priority=0, retry=3):
    for i in range(retry):
        try:
            conn, ch = get_channel()
            ch.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=json.dumps(message, ensure_ascii=False),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    priority=priority
                ),
            )
            return

        except pika.exceptions.AMQPError as e:
            logger.error(f"RabbitMQ publish failed, retry {i+1}/{retry}: {e}")

            try:
                _local.conn.close()
            except:
                pass

            time.sleep(1)

    raise RuntimeError("RabbitMQ publish failed after retries")


class Publisher:
    def __init__(self):
        self.conn = None
        self.ch = None
        self.connect()

    def connect(self):
        while True:
            try:
                self.conn = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=RABBITMQ_HOST,
                        port=RABBITMQ_PORT,
                        credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD),
                        heartbeat=600,
                        blocked_connection_timeout=300,
                    )
                )
                self.ch = self.conn.channel()
                self.ch.exchange_declare(exchange=RESULT_EXCHANGE, exchange_type="topic", durable=True)
                return
            except Exception as e:
                logger.error(f"Publisher connect failed: {e}")
                time.sleep(5)

    def publish(self, routing_key, body):
        while True:
            try:
                self.ch.basic_publish(
                    exchange=RESULT_EXCHANGE,
                    routing_key=routing_key,
                    body=body,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                return
            except Exception as e:
                logger.error(f"Publish failed, reconnecting: {e}")
                self.safe_close()
                self.connect()

    def safe_close(self):
        try:
            if self.conn:
                self.conn.close()
        except:
            pass