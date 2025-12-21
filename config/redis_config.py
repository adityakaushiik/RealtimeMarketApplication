# Async Redis client instance
import os
import redis.asyncio as async_redis

from config.settings import get_settings

redis_client = None


def _build_redis_url_from_env():
    # Priority: REDIS_URL > compose-style REDIS_HOST/REDIS_PORT > localhost default
    settings = get_settings()
    redis_url = settings.REDIS_URL
    if redis_url:
        return redis_url
    host = os.getenv("REDIS_HOST", "localhost")
    port = os.getenv("REDIS_PORT", "6379")
    return f"redis://{host}:{port}"


def get_redis():
    """Return a redis.asyncio.Redis client. Uses REDIS_URL or REDIS_HOST/REDIS_PORT from env.

    decode_responses is set to True so commands return Python types (strings) instead of bytes.
    """
    global redis_client
    try:
        if redis_client is None:
            redis_url = _build_redis_url_from_env()
            print(redis_url)
            redis_client = async_redis.from_url(redis_url, decode_responses=False)
            print(f"Created redis client for {redis_url} (decode_responses=True)")
        return redis_client
    except Exception as e:
        print(f"Redis connection error: {e}")
        return None
