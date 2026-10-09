import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASS = os.getenv("REDIS_PASS", "")

TASK_QUEUE = os.getenv("TASK_QUEUE", "mw_task_queue")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

TASK_EXCHANGE = os.getenv("TASK_EXCHANGE", "task.exchange")
RESULT_EXCHANGE = os.getenv("RESULT_EXCHANGE", "result.exchange")

QUEUE_GITHUB = os.getenv("QUEUE_GITHUB", "crawler.task.github")
QUEUE_WENKU = os.getenv("QUEUE_WENKU", "crawler.task.wenku")
QUEUE_RESULT = os.getenv("QUEUE_RESULT", "crawler.result")

REPORT_URL = os.getenv("REPORT_URL")
REPORT_URL_GITHUB = os.getenv("REPORT_URL_GITHUB")
REPORT_URL_WENKU = os.getenv("REPORT_URL_WENKU")

STATUS_URL = os.getenv("STATUS_URL")

# ====== CONFIG from ENV ======
GITHUB_TOKENS = os.getenv("GITHUB_TOKEN").split(",")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
GITLAB_URL = os.getenv("GITLAB_URL", "https://gitlab.com")
GITHUB_URL = os.getenv("GITHUB_URL", "https://api.github.com")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET_GITHUB = os.getenv("MINIO_BUCKET_GITHUB")
MINIO_BUCKET_WENKU = os.getenv("MINIO_BUCKET_WENKU")

SINCE_DATE = (datetime.utcnow() - timedelta(days=180)).strftime("%Y-%m-%dT%H:%M:%SZ")
GITHUB_SLEEP = float(os.getenv("GITHUB_SLEEP", "2.0"))
GITLAB_SLEEP = float(os.getenv("GITLAB_SLEEP", "2.0"))
SFTP_FLAG = int(os.getenv("SFTP_FLAG", 0))

MYSQL_USER = os.getenv("MYSQL_USER", "")
MYSQL_PWD = os.getenv("MYSQL_PWD", "")
MYSQL_HOST = os.getenv("MYSQL_HOST", "")
MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
MYSQL_DB = os.getenv("MYSQL_DB", "")

IP = os.getenv("IP", "")
THREAD_NUM = int(os.getenv("THREAD_NUM", 1))

DEDUP_TIMEOUT = int(os.getenv("DEDUP_TIMEOUT", 86400))

PROXY_URL = os.getenv("PROXY_URL")
