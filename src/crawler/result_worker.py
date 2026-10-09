import json
import requests
from common.rabbitmq import get_channel
from common.config import QUEUE_RESULT, REPORT_URL_GITHUB, REPORT_URL_WENKU
from common.logger import setup_logger
import time

logger = setup_logger('result_worker')


def start():
    conn, ch = get_channel()

    def callback(ch, method, properties, body):
        data = json.loads(body)
        url = REPORT_URL_GITHUB if data.get('source') == 'github' else REPORT_URL_WENKU

        try:
            resp = requests.post(
                url,
                json=data,
                timeout=10
            )

            if resp.status_code == 200:
                ch.basic_ack(method.delivery_tag)
                logger.info(f"结果上报成功: {data} ----> {resp.text}")

            else:
                logger.error(f"结果上报失败 status={resp.status_code} ----> {data}")
                ch.basic_nack(method.delivery_tag, requeue=True)

        except Exception:
            logger.exception("结果上报失败")
            time.sleep(10)
            ch.basic_nack(method.delivery_tag, requeue=True)

    ch.basic_consume(queue=QUEUE_RESULT, on_message_callback=callback)
    ch.start_consuming()


if __name__ == "__main__":
    start()
