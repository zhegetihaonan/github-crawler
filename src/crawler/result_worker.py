import json
import time

import requests
import pika

from common.config import (
    MAX_RESULT_ATTEMPTS,
    QUEUE_RESULT,
    REPORT_URL_GITHUB,
    REPORT_URL_WENKU,
    RESULTS_DB_ENABLED,
)
from common.logger import setup_logger
from common.rabbitmq import get_channel, publish_dead_letter
from result_store import ensure_results_table, save_result

logger = setup_logger("result_worker")


def report_url_for(data):
    source = str(data.get("source", "")).lower()
    if source == "github":
        return REPORT_URL_GITHUB
    if source in {"wenku", "gitlab"}:
        return REPORT_URL_WENKU
    raise ValueError(f"Unsupported result source: {source!r}")


def start():
    if not RESULTS_DB_ENABLED and not (REPORT_URL_GITHUB or REPORT_URL_WENKU):
        raise RuntimeError(
            "Configure RESULTS_DB_ENABLED=1 or at least one result report URL"
        )
    ensure_results_table()
    conn, ch = get_channel()

    def callback(channel, method, properties, body):
        try:
            data = json.loads(body)
            if not isinstance(data, dict):
                raise ValueError("Result message must be a JSON object")
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
            logger.error("Discarding invalid result message: %s", exc)
            publish_dead_letter(channel, QUEUE_RESULT, body, str(exc))
            channel.basic_ack(method.delivery_tag)
            return

        try:
            url = report_url_for(data)
        except ValueError as exc:
            logger.error("Discarding result with unsupported source: %s", exc)
            publish_dead_letter(channel, QUEUE_RESULT, body, str(exc))
            channel.basic_ack(method.delivery_tag)
            return

        if not url and not RESULTS_DB_ENABLED:
            logger.error(
                "No database or report URL for result source=%s; discarding message",
                data.get("source"),
            )
            publish_dead_letter(channel, QUEUE_RESULT, body, "no sink configured")
            channel.basic_ack(method.delivery_tag)
            return

        try:
            # Persist first. INSERT IGNORE and a stable record hash make redelivery safe.
            save_result(data)

            if url:
                headers = {"Idempotency-Key": str(data["result_id"])} if data.get("result_id") else None
                response = requests.post(url, json=data, headers=headers, timeout=15)
                response.raise_for_status()
                logger.info(
                    "Result delivered source=%s task_id=%s status=%s",
                    data.get("source"),
                    data.get("task_id"),
                    response.status_code,
                )
            elif RESULTS_DB_ENABLED:
                logger.info(
                    "Result stored in MySQL source=%s task_id=%s",
                    data.get("source"),
                    data.get("task_id"),
                )
            else:
                raise RuntimeError("No report URL configured for this result source")

            channel.basic_ack(method.delivery_tag)
        except Exception:
            headers = dict(getattr(properties, "headers", None) or {})
            retry_count = int(headers.get("x-retry-count", 0))
            logger.exception(
                "Result processing failed source=%s task_id=%s; message will be retried",
                data.get("source"),
                data.get("task_id"),
            )
            if retry_count + 1 < MAX_RESULT_ATTEMPTS:
                next_count = retry_count + 1
                headers["x-retry-count"] = next_count
                time.sleep(min(2 ** next_count, 60))
                channel.basic_publish(
                    exchange="",
                    routing_key=QUEUE_RESULT,
                    body=body,
                    properties=requests_to_amqp_properties(properties, headers),
                    mandatory=True,
                )
                channel.basic_ack(method.delivery_tag)
            else:
                publish_dead_letter(channel, QUEUE_RESULT, body, "result retry limit reached", headers)
                channel.basic_ack(method.delivery_tag)

    ch.basic_consume(queue=QUEUE_RESULT, on_message_callback=callback)
    logger.info("Result worker consuming queue=%s", QUEUE_RESULT)
    try:
        ch.start_consuming()
    finally:
        if conn and conn.is_open:
            conn.close()


def requests_to_amqp_properties(original, headers):
    return pika.BasicProperties(
        content_type=getattr(original, "content_type", None) or "application/json",
        delivery_mode=2,
        priority=getattr(original, "priority", 0),
        headers=headers,
        correlation_id=getattr(original, "correlation_id", None),
    )

if __name__ == "__main__":
    start()
