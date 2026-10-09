import base64
import json
import random
import re
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone
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


logger = setup_logger("crawler_github_optimized")

PROXY_CONFIG_PATH = Path(__file__).with_name("proxies.yaml")
HTTP_RETRY_STATUS = {403, 408, 429, 500, 502, 503, 504}
STOPPED_TASK_SET = "crawler:task:stopped"
DEFAULT_PER_PAGE = 50
GITHUB_TOKEN_POOL = [token.strip() for token in GITHUB_TOKENS if token and token.strip()]

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
        super().__init__(name=f"github-crawler-{cid}")
        self.cid = cid
        self._running = threading.Event()
        self._running.set()
        self.connection = None
        self.ch_consume = None
        self.ch_publish = None
        self.session = requests.Session()
        self.proxies = self.load_proxies()
        self.github_tokens = GITHUB_TOKEN_POOL[:]
        self.bad_github_tokens = set()
        self.commit_author_cache: Dict[str, tuple[Any, Any, Any]] = {}
        self.ensure_minio_bucket(MINIO_BUCKET_GITHUB)
        logger.info("crawler=%s token_count=%s proxy_count=%s", self.cid, len(self.github_tokens), len(self.proxies))

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
        self.ch_consume.queue_declare(queue=QUEUE_GITHUB, durable=True, arguments={"x-max-priority": 10})
        self.ch_consume.queue_bind(exchange=TASK_EXCHANGE, queue=QUEUE_GITHUB, routing_key=QUEUE_GITHUB)
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

    def pick_github_token(self) -> str:
        available_tokens = [token for token in self.github_tokens if token not in self.bad_github_tokens]
        if not available_tokens:
            logger.error("crawler=%s no valid GitHub tokens left", self.cid)
            return ""
        return random.choice(available_tokens)

    def github_headers(self) -> Dict[str, str]:
        headers = {
            "Accept": "application/vnd.github+json",
            # "X-GitHub-Api-Version": "2022-11-28",
        }
        token = self.pick_github_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def refresh_github_auth_header(self, headers: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:
        if not headers or "Authorization" not in headers:
            return None
        auth_header = headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return None

        bad_token = auth_header.removeprefix("Bearer ").strip()
        if bad_token:
            self.bad_github_tokens.add(bad_token)
            logger.warning(
                "crawler=%s disabled bad GitHub token=%s remaining_tokens=%s",
                self.cid,
                self.mask_token(bad_token),
                len(self.github_tokens) - len(self.bad_github_tokens),
            )

        next_token = self.pick_github_token()
        if not next_token:
            return None
        refreshed_headers = headers.copy()
        refreshed_headers["Authorization"] = f"Bearer {next_token}"
        return refreshed_headers

    @staticmethod
    def mask_token(token: str) -> str:
        if len(token) <= 8:
            return "***"
        return f"{token[:4]}...{token[-4:]}"

    @staticmethod
    def load_proxies(config_path: Path = PROXY_CONFIG_PATH) -> List[Optional[Dict[str, str]]]:
        if not config_path.exists():
            logger.warning("proxy config not found: %s; requests will run without proxy", config_path)
            return [None]
        with config_path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        proxies = data.get("proxies") or []
        return proxies or [None]

    def proxy_pool(self) -> List[Optional[Dict[str, str]]]:
        proxies = self.proxies[:]
        random.shuffle(proxies)
        return proxies or [None]

    def safe_request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        data: Any = None,
        json_body: Any = None,
        retries: int = 3,
        timeout: int = 60,
        acceptable_status: tuple[int, ...] = (200,),
    ) -> Optional[requests.Response]:
        for attempt in range(1, retries + 1):
            for proxy in self.proxy_pool():
                try:
                    resp = self.session.request(
                        method,
                        url,
                        headers=headers,
                        params=params,
                        data=data,
                        json=json_body,
                        timeout=timeout,
                        proxies=proxy,
                    )
                    if resp.status_code in acceptable_status:
                        logger.info("crawler=%s proxy=%s resp=%s", self.cid, proxy, resp.text[:300])
                        return resp
                    if resp.status_code == 404:
                        logger.warning(f'url {url} --> 404')
                        return None
                    if resp.status_code == 401:
                        self.log_request_failure(resp, url, attempt)
                        refreshed_headers = self.refresh_github_auth_header(headers)
                        if refreshed_headers:
                            headers = refreshed_headers
                            continue
                        return None
                    self.log_request_failure(resp, url, attempt)
                    if resp.status_code not in HTTP_RETRY_STATUS:
                        return None
                except requests.RequestException as exc:
                    logger.warning("crawler=%s request exception url=%s attempt=%s error=%s", self.cid, url, attempt, exc)
            time.sleep(min(3 * attempt, 15))
        logger.warning("crawler=%s request failed after retries url=%s", self.cid, url)
        return None

    def log_request_failure(self, resp: requests.Response, url: str, attempt: int):
        wait_seconds = self.github_retry_wait(resp)
        logger.warning(
            "crawler=%s got status=%s url=%s attempt=%s body=%s",
            self.cid,
            resp.status_code,
            url,
            attempt,
            resp.text[:500],
        )
        if wait_seconds:
            logger.warning("crawler=%s GitHub rate limited; sleep=%ss", self.cid, wait_seconds)
            time.sleep(wait_seconds)

    @staticmethod
    def github_retry_wait(resp: requests.Response) -> int:
        if resp.status_code not in (403, 429):
            return 0
        reset_at = resp.headers.get("X-RateLimit-Reset")
        if not reset_at:
            return 0
        try:
            wait_seconds = int(reset_at) - int(time.time()) + 2
            return max(0, min(wait_seconds, 60))
        except ValueError:
            return 0

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

    def github_search_repos_by_keyword(self, keyword: str, since_iso: str, max_page: int) -> List[Dict]:
        results = []
        query = f'''"{keyword.replace('_', '" AND "')}" in:name,description pushed:>={since_iso[:10]}'''
        for page in range(1, int(max_page) + 1):
            resp = self.safe_request(
                "GET",
                f"{GITHUB_URL.rstrip('/')}/search/repositories",
                headers=self.github_headers(),
                params={"q": query, "sort": "updated", "order": "desc", "per_page": DEFAULT_PER_PAGE, "page": page},
            )
            if not resp:
                break
            items = resp.json().get("items", [])
            if not items:
                break
            for item in items:
                owner = item.get("owner") or {}
                results.append(
                    {
                        "name": item.get("full_name") or "",
                        "description": item.get("description") or "",
                        "html_url": item.get("html_url") or "",
                        "full_name": item.get("full_name") or "",
                        "owner_name": owner.get("login") or "",
                        "owner_url": owner.get("html_url") or "",
                    }
                )
            time.sleep(GITHUB_SLEEP)
        return results

    def github_search_code_by_keyword(self, keyword: str, max_page: int) -> List[Dict]:
        results = []
        query = f'''"{keyword.replace('_', '" AND "')}" in:file'''
        for page in range(1, int(max_page) + 1):
            resp = self.safe_request(
                "GET",
                f"{GITHUB_URL.rstrip('/')}/search/code",
                headers=self.github_headers(),
                params={"q": query, "per_page": DEFAULT_PER_PAGE, "page": page},
            )
            if not resp:
                break
            items = resp.json().get("items", [])
            if not items:
                break
            for item in items:
                repo = item.get("repository") or {}
                owner = repo.get("owner") or {}
                file_path = item.get("path") or ""
                repo_full_name = repo.get("full_name") or ""
                if repo_full_name and file_path:
                    results.append(
                        {
                            "repo_full_name": repo_full_name,
                            "repo_html_url": repo.get("html_url") or "",
                            "owner_name": owner.get("login") or "",
                            "owner_url": owner.get("html_url") or "",
                            "file_path": file_path,
                        }
                    )
            time.sleep(GITHUB_SLEEP)
        return results

    def github_get_file_content(self, owner_repo: str, path: str) -> Optional[bytes]:
        safe_owner_repo = quote(owner_repo, safe="/")
        safe_path = quote(path, safe="/")
        url = f"{GITHUB_URL.rstrip('/')}/repos/{safe_owner_repo}/contents/{safe_path}"
        resp = self.safe_request("GET", url, headers=self.github_headers())
        if not resp:
            return None
        payload = resp.json()
        if isinstance(payload, list):
            return None
        if payload.get("encoding") == "base64" and payload.get("content"):
            return base64.b64decode(payload["content"])
        download_url = payload.get("download_url")
        if download_url:
            raw_resp = self.safe_request("GET", download_url, headers=None)
            return raw_resp.content if raw_resp else None
        return None

    def github_get_latest_commit_author(self, owner_repo: str) -> tuple[Any, Any, Any]:
        if owner_repo in self.commit_author_cache:
            return self.commit_author_cache[owner_repo]
        safe_owner_repo = quote(owner_repo, safe="/")
        url = f"{GITHUB_URL.rstrip('/')}/repos/{safe_owner_repo}/commits"
        result = (None, None, None)
        try:
            resp = self.safe_request("GET", url, headers=self.github_headers(), params={"per_page": 1})
            if resp:
                data = resp.json()
                if data:
                    author = data[0].get("commit", {}).get("author", {})
                    date = self.parse_github_time(author.get("date"))
                    result = (author.get("name"), author.get("email"), date)
        except Exception:
            logger.exception("crawler=%s failed fetching latest commit owner_repo=%s", self.cid, owner_repo)
        self.commit_author_cache[owner_repo] = result
        return result

    @staticmethod
    def parse_github_time(value: Optional[str]) -> str:
        if not value:
            return ""
        try:
            dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except ValueError:
            dt = datetime.fromisoformat(value)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def extract_image_urls_from_readme(content: str) -> List[str]:
        markdown_urls = re.findall(r"!\[[^\]]*\]\((https?://[^)\s]+)(?:\s+\"[^\"]*\")?\)", content)
        html_urls = re.findall(r"<img[^>]+src=[\"'](https?://[^\"']+)[\"']", content, flags=re.IGNORECASE)
        return list(dict.fromkeys(markdown_urls + html_urls))

    @staticmethod
    def decode_text(content: bytes) -> str:
        for encoding in ("utf-8", "utf-8-sig", "gb18030"):
            try:
                return content.decode(encoding)
            except UnicodeDecodeError:
                continue
        return content.decode("utf-8", errors="ignore")

    @staticmethod
    def now_text() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def object_path(*parts: str) -> str:
        cleaned = [quote(str(part).strip("/"), safe="@._-+/") for part in parts if part]
        return "/".join(cleaned)

    def is_task_stopped(self, task_id: str) -> bool:
        return bool(redis_client.sismember(STOPPED_TASK_SET, task_id))

    def repo_record(self, task_id: str, keyword: str, repo: Dict, author: Any, email: Any, last: Any) -> Dict:
        return {
            "task_id": task_id,
            "crawler_id": str(self.cid),
            "source": "github",
            "keyword": keyword,
            "name": repo["full_name"],
            "description": repo["description"][:1000],
            "repo_url": repo["html_url"],
            "matched_field": "name_or_description",
            "file_path": "",
            "file_minio_url": "",
            "file_name": "",
            "found_at": self.now_text(),
            "repo_owner_name": repo["owner_name"],
            "repo_owner_url": repo["owner_url"],
            "last_commit_author_name": author,
            "last_commit_author_email": email,
            "last_commit_time": last,
        }

    def github_scan(self, task: Dict):
        keyword = task["keyword"]
        task_id = task["task_id"]
        max_page = int(task.get("max_page", 1))
        if self.is_task_stopped(task_id):
            logger.info("crawler=%s task stopped before scan task_id=%s", self.cid, task_id)
            return

        repos = self.github_search_repos_by_keyword(keyword, SINCE_DATE, max_page)
        logger.info("crawler=%s repo search keyword=%s found=%s", self.cid, keyword, len(repos))
        for repo in repos:
            if self.is_task_stopped(task_id):
                return
            author, email, last = self.github_get_latest_commit_author(repo["full_name"])
            records = [self.repo_record(task_id, keyword, repo, author, email, last)]
            records.extend(self.readme_image_records(task_id, keyword, repo, author, email, last))
            self.publish_result(records)

        code_matches = self.github_search_code_by_keyword(keyword, max_page)
        logger.info("crawler=%s code search keyword=%s found=%s", self.cid, keyword, len(code_matches))
        for match in code_matches:
            if self.is_task_stopped(task_id):
                return
            record = self.code_match_record(task_id, keyword, match)
            if record:
                self.publish_result([record])

    def readme_image_records(self, task_id: str, keyword: str, repo: Dict, author: Any, email: Any, last: Any) -> List[Dict]:
        readme_content = self.github_get_file_content(repo["full_name"], "README.md")
        if not readme_content:
            return []
        image_urls = self.extract_image_urls_from_readme(self.decode_text(readme_content))
        logger.info("crawler=%s repo=%s readme_images=%s", self.cid, repo["full_name"], len(image_urls))

        records = []
        for url in image_urls:
            minio_url = ""
            resp = self.safe_request("GET", url, headers=None, retries=2, timeout=30)
            if resp and resp.content:
                object_name = self.object_path(keyword, "readme_img", repo["full_name"], url.split("/")[-1].split("?")[0])
                minio_url = self.upload_to_minio(MINIO_BUCKET_GITHUB, object_name, resp.content)
            records.append(
                {
                    "task_id": task_id,
                    "crawler_id": str(self.cid),
                    "source": "github",
                    "keyword": keyword,
                    "name": repo["full_name"],
                    "description": repo["description"][:1000],
                    "repo_url": repo["html_url"],
                    "matched_field": "readme_image",
                    "file_path": url,
                    "file_minio_url": minio_url,
                    "file_name": url.split("/")[-1].split("?")[0],
                    "found_at": self.now_text(),
                    "repo_owner_name": repo["owner_name"],
                    "repo_owner_url": repo["owner_url"],
                    "last_commit_author_name": author,
                    "last_commit_author_email": email,
                    "last_commit_time": last,
                }
            )
        return records

    def code_match_record(self, task_id: str, keyword: str, match: Dict) -> Optional[Dict]:
        content = self.github_get_file_content(match["repo_full_name"], match["file_path"])
        if not content:
            return None
        object_name = self.object_path(keyword, "files", match["repo_full_name"], match["file_path"])
        minio_url = self.upload_to_minio(MINIO_BUCKET_GITHUB, object_name, content)
        author, email, last = self.github_get_latest_commit_author(match["repo_full_name"])
        return {
            "task_id": task_id,
            "crawler_id": str(self.cid),
            "source": "github",
            "keyword": keyword,
            "name": match["repo_full_name"],
            "description": "",
            "repo_url": match["repo_html_url"],
            "matched_field": "file_content",
            "file_path": match["file_path"],
            "file_minio_url": minio_url,
            "file_name": match["file_path"].split("/")[-1],
            "found_at": self.now_text(),
            "repo_owner_name": match["owner_name"],
            "repo_owner_url": match["owner_url"],
            "last_commit_author_name": author,
            "last_commit_author_email": email,
            "last_commit_time": last,
        }

    def run(self):
        time.sleep(random.randint(1, 3))
        try:
            self.create_connection()
            logger.info("crawler=%s started, waiting for tasks", self.cid)
            while self._running.is_set():
                method, properties, body = self.ch_consume.basic_get(queue=QUEUE_GITHUB, auto_ack=False)
                if method is None:
                    time.sleep(1)
                    continue
                task = {}
                try:
                    task = json.loads(body)
                    logger.info("crawler=%s processing task=%s", self.cid, task)
                    self.github_scan(task)
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
    main()
