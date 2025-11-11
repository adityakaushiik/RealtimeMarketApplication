# Async Redis client instance
import redis.asyncio as async_redis

redis = async_redis.from_url("redis://redis", decode_responses=True)


# Sync Redis client instance
# import redis as sync_redis
# redis = sync_redis.Redis(host='localhost', port=6379, db=0)

def get_redis():
    return redis
