import redis
from common.config import REDIS_HOST, REDIS_PORT, REDIS_PASS

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASS,
    decode_responses=True
)