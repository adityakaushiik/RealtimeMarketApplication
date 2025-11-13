from services.in_memory_db import get_in_memory_db


class RedisHelper:
    def __init__(self):
        self.redis_client = get_in_memory_db()

    # async def initialize_prices_dict(self):
    #     """Initialize the prices_dict in Redis if it does not exist."""
    #     exists = await self.redis_client.exists('prices_dict')
    #     if not exists:
    #         await self.redis_client.set(name='prices_dict', value={})

    def set_value(self, key, value, expire=None):
        """Set a value in Redis with an optional expiration time."""
        self.redis_client.set(key, value, expire)

    def get_value(self, key):
        """Get a value from Redis by key."""
        return self.redis_client.get(key)

    def delete_value(self, key):
        """Delete a value from Redis by key."""
        self.redis_client.delete(key)

    def exists(self, key):
        """Check if a key exists in Redis."""
        return self.redis_client.exists(key) > 0


redis_helper = None


def get_redis_helper() -> RedisHelper:
    global redis_helper
    if redis_helper is None:
        redis_helper = RedisHelper()
    return redis_helper
