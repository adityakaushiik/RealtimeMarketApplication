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

    Performance:
    - Uses asyncio.gather() for concurrent broadcasting to avoid bottlenecks
    - Can handle thousands of clients efficiently
    - One slow client doesn't block others

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
    await pubsub.psubscribe("stocks:*")

    logger.info("🎧 Redis subscriber started, listening to stocks:*")

    try:
        async for message in pubsub.listen():

            if message.get("type") != "pmessage":
                continue

            # Message fields may be bytes because decode_responses=False
            channel = message.get("channel")
            data = message.get("data")

            # Decode channel to string (channels are simple ascii identifiers)
            if isinstance(channel, (bytes, bytearray)):
                try:
                    channel = channel.decode("utf-8")
                except Exception:
                    channel = channel.decode("utf-8", errors="replace")

            # Try to decode data as UTF-8 text; if not text, keep as bytes
            if isinstance(data, (bytes, bytearray)):
                try:
                    decoded = data.decode("utf-8")
                    data = decoded
                except Exception:
                    # binary payload -> keep bytes; broadcast function will send as binary
                    data = bytes(data)

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
    - All sends happen concurrently using asyncio.gather()
    - return_exceptions=True ensures one failure doesn't crash all sends
    - Disconnected clients are tracked and cleaned up after broadcasting
    """
    # Collect all clients subscribed to this channel with their send coroutines
    clients_and_tasks = []

    for ws, subscriptions in websocket_manager.active_connections.items():
        if channel in subscriptions:
            # Create a coroutine for each client (will run concurrently via gather)
            send_coroutine = _send_to_client(ws, data)
            clients_and_tasks.append((ws, send_coroutine))

    if not clients_and_tasks:
        # logger.info(f"No clients subscribed to {channel}")
        return

    # Execute all sends concurrently
    # return_exceptions=True prevents one failure from canceling others
    results = await asyncio.gather(
        *[coro for _, coro in clients_and_tasks],
        return_exceptions=True
    )

    # Track clients that failed to receive the message
    disconnected_clients: List[WebSocket] = []
    successful_sends = 0

    for (ws, _), result in zip(clients_and_tasks, results):
        if isinstance(result, Exception):
            logger.error(f"❌ Error sending to client: {result}")
            disconnected_clients.append(ws)
        else:
            successful_sends += 1

    # Clean up disconnected clients
    for ws in disconnected_clients:
        try:
            websocket_manager.disconnect(ws)
            logger.info(f"🔌 Removed disconnected client")
        except Exception as e:
            logger.error(f"Error removing client: {e}")

    # Log broadcast summary
    if successful_sends > 0:
        logger.debug(
            f"📤 Broadcasted to {successful_sends}/{len(clients_and_tasks)} clients on {channel}"
        )


async def _send_to_client(ws: WebSocket, data: Union[str, bytes]) -> None:
    # Choose text or binary send based on data type
    if isinstance(data, (bytes, bytearray)):
        await ws.send_bytes(data)
    else:
        await ws.send_text(data)
