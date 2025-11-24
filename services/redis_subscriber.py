import asyncio
from typing import List, Union

from fastapi import WebSocket

from config.logger import logger
from config.redis_config import get_redis
from services.websocket_manager import websocket_manager


async def redis_subscriber():
    """
    Background task that listens to Redis pub/sub and broadcasts messages to WebSocket clients.

    This function:
    1. Subscribes to Redis pattern "stocks:*" (matches stocks:AAPL, stocks:GOOGL, etc.)
    2. Listens for messages from Redis publishers
    3. Broadcasts messages concurrently to all subscribed WebSocket clients
    4. Handles client disconnections gracefully
    5. Cleans up resources on shutdown

    Example Flow:
    - Publisher sends: POST /publish/AAPL?price=150.25
    - Redis publishes to channel "stocks:AAPL"
    - This function receives the message
    - Broadcasts to all clients subscribed to "stocks:AAPL"
    """

    redis = get_redis()

    if redis is None:
        logger.error("Redis client is not available. Exiting subscriber.")
        return

    pubsub = redis.pubsub()
    await pubsub.psubscribe("*")

    logger.info("🎧 Redis subscriber started, listening to All Channels *")

    try:
        async for message in pubsub.listen():
            if message.get("type") != "pmessage":
                continue

            # Message fields may be bytes because decode_responses=False
            ## This should be string not bytes
            channel: str = message.get("channel")
            if isinstance(channel, bytes):
                channel = channel.decode("utf-8")

            ## This should be bytes not string
            data: bytes = message.get("data")

            # Broadcast to all subscribed clients concurrently
            await _broadcast_to_clients(channel, data)

    except asyncio.CancelledError:
        logger.info("Redis subscriber cancellation requested")
        raise
    except Exception as e:
        logger.error(f"Redis subscriber error: {e}")
        raise
    finally:
        # Match psubscribe with punsubscribe; then close cleanly
        try:
            await pubsub.punsubscribe("stocks:*")
        except Exception as e:
            logger.warning(f"punsubscribe failed: {e}")
        try:
            await pubsub.close()
        except Exception as e:
            logger.warning(f"pubsub close failed: {e}")
        logger.info("Redis subscriber stopped")


async def _broadcast_to_clients(channel: str, data: Union[str, bytes]) -> None:
    """
    Broadcast a message to all WebSocket clients subscribed to a specific channel.

    Uses concurrent sending (asyncio.gather) to avoid performance bottlenecks:
    - Sequential: 1000 clients × 10ms = 10 seconds ❌
    - Concurrent: 1000 clients in parallel = ~10ms ✅

    Args:
        channel: The Redis channel name (e.g., "stocks:AAPL")
        data: The message payload to send (JSON string)

    Performance Notes:
    - Uses O(1) lookup for subscribers via websocket_manager.get_subscribers()
    - All sends happen concurrently using asyncio.gather()
    - return_exceptions=True ensures one failure doesn't crash all sends
    - Disconnected clients are tracked and cleaned up after broadcasting
    """

    # Get subscribers efficiently (O(1))
    subscribers = websocket_manager.get_subscribers(channel)
    
    if not subscribers:
        # No debug log here to avoid spamming logs for channels with no listeners
        return

    # Create tasks for all subscribers
    # We convert the set to a list to avoid "Set changed size during iteration" if a disconnect happens concurrently
    clients = list(subscribers)
    tasks = [_send_to_client(ws, data) for ws in clients]

    # Execute all sends concurrently
    # return_exceptions=True prevents one failure from canceling others
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Track clients that failed to receive the message
    disconnected_clients: List[WebSocket] = []
    successful_sends = 0

    for ws, result in zip(clients, results):
        if isinstance(result, Exception):
            # Only log real errors, not normal disconnects if possible
            logger.debug(f"Error sending to client: {result}")
            disconnected_clients.append(ws)
        else:
            successful_sends += 1

    # Clean up disconnected clients
    if disconnected_clients:
        for ws in disconnected_clients:
            try:
                websocket_manager.disconnect(ws)
            except Exception:
                pass
        logger.info(f"Cleaned up {len(disconnected_clients)} disconnected clients")

    # Log broadcast summary (only for debug/trace to reduce noise)
    # logger.debug(f"📤 Broadcasted to {successful_sends}/{len(clients)} clients on {channel}")


async def _send_to_client(ws: WebSocket, data: Union[str, bytes]) -> None:
    # Choose text or binary send based on data type
    if isinstance(data, (bytes, bytearray)):
        await ws.send_bytes(data)
    else:
        await ws.send_text(data)
