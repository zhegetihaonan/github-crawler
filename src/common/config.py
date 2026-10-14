"""Environment based application configuration."""

import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

load_dotenv()


def _int(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None or value.strip() == "" else int(value)


def _float(name: str, default: float) -> float:
    value = os.getenv(name)
    return default if value is None or value.strip() == "" else float(value)


def _bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = _int("RABBITMQ_PORT", 5672)
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = _int("REDIS_PORT", 6379)
REDIS_PASS = os.getenv("REDIS_PASS", "")

TASK_QUEUE = os.getenv("TASK_QUEUE", "mw_task_queue")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
TASK_EXCHANGE = os.getenv("TASK_EXCHANGE", "task.exchange")
RESULT_EXCHANGE = os.getenv("RESULT_EXCHANGE", "result.exchange")
QUEUE_GITHUB = os.getenv("QUEUE_GITHUB", "crawler.task.github")
QUEUE_WENKU = os.getenv("QUEUE_WENKU", "crawler.task.wenku")
QUEUE_RESULT = os.getenv("QUEUE_RESULT", "crawler.result")

REPORT_URL = os.getenv("REPORT_URL")
REPORT_URL_GITHUB = os.getenv("REPORT_URL_GITHUB") or REPORT_URL
REPORT_URL_WENKU = os.getenv("REPORT_URL_WENKU") or REPORT_URL
STATUS_URL = os.getenv("STATUS_URL")

GITHUB_TOKENS = [token.strip() for token in os.getenv("GITHUB_TOKEN", "").split(",") if token.strip()]
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
GITLAB_URL = os.getenv("GITLAB_URL", "https://gitlab.com")
GITHUB_URL = os.getenv("GITHUB_URL", "https://api.github.com")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET_GITHUB = os.getenv("MINIO_BUCKET_GITHUB")
MINIO_BUCKET_WENKU = os.getenv("MINIO_BUCKET_WENKU")

SINCE_DATE_DAYS = max(1, _int("SINCE_DATE_DAYS", 180))


def get_since_date() -> str:
    """Return a rolling UTC cutoff, recalculated for every task run."""
    return (datetime.now(timezone.utc) - timedelta(days=SINCE_DATE_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
GITHUB_SLEEP = _float("GITHUB_SLEEP", 2.0)
GITLAB_SLEEP = _float("GITLAB_SLEEP", 2.0)
SFTP_FLAG = _int("SFTP_FLAG", 0)

MYSQL_USER = os.getenv("MYSQL_USER", "")
MYSQL_PWD = os.getenv("MYSQL_PWD", "")
MYSQL_HOST = os.getenv("MYSQL_HOST", "")
MYSQL_PORT = _int("MYSQL_PORT", 3306)
MYSQL_DB = os.getenv("MYSQL_DB", "")

RESULTS_DB_ENABLED = _bool("RESULTS_DB_ENABLED", False)
RESULTS_TABLE = os.getenv("RESULTS_TABLE", "crawler_results")
ENABLE_WENKU_CRAWLER = _bool("ENABLE_WENKU_CRAWLER", False)
MAX_TASK_ATTEMPTS = max(1, _int("MAX_TASK_ATTEMPTS", 5))
MAX_RESULT_ATTEMPTS = max(1, _int("MAX_RESULT_ATTEMPTS", 8))
API_KEY = os.getenv("API_KEY", "")

IP = os.getenv("IP", "")
THREAD_NUM = max(1, _int("THREAD_NUM", 1))
PROXY_URL = os.getenv("PROXY_URL")
