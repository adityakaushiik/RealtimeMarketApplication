# Async Redis client instance
import os
import redis.asyncio as async_redis

# Sync Redis client instance
# import redis as sync_redis
# redis = sync_redis.Redis(host='localhost', port=6379, db=0)

redis_client = None

def _build_redis_url_from_env():
    # Priority: REDIS_URL > compose-style REDIS_HOST/REDIS_PORT > localhost default
    redis_url = os.getenv("REDIS_URL")
    if redis_url:
        return redis_url
    host = os.getenv("REDIS_HOST", "localhost")
    port = os.getenv("REDIS_PORT", "6379")
    return f"redis://{host}:{port}"

def get_redis():
    """Return a redis.asyncio.Redis client. Uses REDIS_URL or REDIS_HOST/REDIS_PORT from env.

    decode_responses is set to False so pubsub messages are returned as raw bytes
    and the subscriber code can decide how to decode or forward binary payloads.
    """
    global redis_client
    try:
        if redis_client is None:
            redis_url = _build_redis_url_from_env()
            # DO NOT decode responses here; pubsub may carry binary payloads
            redis_client = async_redis.from_url(redis_url, decode_responses=False)
            print(f"Created redis client for {redis_url} (decode_responses=False)")
        return redis_client
    except Exception as e:
        print(f"Redis connection error: {e}")
        return None

async def ping_redis():
    """Async helper to verify connectivity. Returns True if ping succeeds, False otherwise."""
    r = get_redis()
    if r is None:
        return False
    try:
        return await r.ping()
    except Exception as e:
        print(f"Redis ping failed: {e}")
        return False
