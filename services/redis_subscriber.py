from config.redis_config import redis, active_connections


async def redis_subscriber():
    """Background task that listens to Redis and broadcasts to WebSocket clients."""
    pubsub = redis.pubsub()
    await pubsub.psubscribe("stocks:*")
    print("📡 Subscribed to Redis channels: stocks:*")

    async for message in pubsub.listen():
        if message["type"] == "pmessage":
            channel = message["channel"]
            data = message["data"]

            # Broadcast to clients subscribed to this channel
            for ws, subscriptions in list(active_connections.items()):
                if channel in subscriptions:
                    try:
                        await ws.send_text(data)
                    except Exception:
                        # Handle closed connections gracefully
                        del active_connections[ws]
