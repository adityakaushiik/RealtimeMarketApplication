import json
import datetime

from fastapi import WebSocket, WebSocketDisconnect, APIRouter
from fastapi.params import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_database_engine
from config.redis_config import get_redis
from features.instruments.instrument_service import get_instrument_by_symbol
from features.marketdata.marketdata_service import get_price_history_daily, get_previous_closes_by_exchange
from features.exchange.exchange_service import get_exchange_by_id
from services.websocket_manager import WebSocketManager, get_websocket_manager
from services.redis_timeseries import RedisTimeSeries, get_redis_timeseries
from utils.common_constants import WebSocketMessageType

websocket_route = APIRouter(prefix="", tags=["socket"])


@websocket_route.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    websocket_manager: WebSocketManager = Depends(get_websocket_manager),
    redis_ts: RedisTimeSeries = Depends(get_redis_timeseries),
):
    """Main WebSocket endpoint for clients to subscribe/unsubscribe to stocks."""
    await websocket.accept()
    websocket_manager.connect(websocket)

    try:
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)

            message_type = msg.get("message_type")
            channel = msg.get("channel")

            if message_type == WebSocketMessageType.SUBSCRIBE.value and channel:
                websocket_manager.subscribe(websocket, channel)

                # Get Snapshot Data
                ltp = await redis_ts.get_last_price(channel)

                # Get Day Open
                today_start = datetime.datetime.now(datetime.timezone.utc).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                start_ts = int(today_start.timestamp() * 1000)
                daily_candle = await redis_ts.get_current_daily_candle(channel, start_ts)
                open_price = daily_candle.get("open") if daily_candle else None

                prev_close = None

                # Get Prev Close (requires DB/Redis lookup)
                engine = get_database_engine()
                async with AsyncSession(engine) as session:
                    instrument = await get_instrument_by_symbol(session, channel)
                    if instrument:
                        exchange = await get_exchange_by_id(session, instrument.exchange_id)
                        if exchange:
                            redis = get_redis()
                            cache_key = f"prev_close:{exchange.code}"
                            found_in_cache = False

                            if redis:
                                cached_data = await redis.get(cache_key)
                                if cached_data:
                                    try:
                                        data = json.loads(cached_data)
                                        for item in data:
                                            if item.get("symbol") == channel:
                                                prev_close = item.get("price")
                                                found_in_cache = True
                                                break
                                    except Exception:
                                        pass

                            # NEW: Try fetching from Hash Map (O(1)) if not found in list
                            if not found_in_cache and redis:
                                 hash_key = f"prev_close_map:{exchange.code}"
                                 price_str = await redis.hget(hash_key, channel)
                                 if price_str:
                                     try:
                                         prev_close = float(price_str)
                                         found_in_cache = True
                                     except:
                                         pass

                            if not found_in_cache:
                                # Fallback to service
                                pcs = await get_previous_closes_by_exchange(session, exchange.code)
                                for item in pcs:
                                    if item.symbol == channel:
                                        prev_close = item.price
                                        break

                # Send Snapshot
                await websocket.send_json(
                    {
                        "message_type": WebSocketMessageType.SNAPSHOT.value,
                        "symbol": channel,
                        "prev_close": prev_close if prev_close is not None else -1,
                        "ltp": ltp if ltp is not None else -1,
                        "open": open_price if open_price is not None else -1
                    }
                )

            elif message_type == WebSocketMessageType.UNSUBSCRIBE.value and channel:
                websocket_manager.unsubscribe(websocket, channel)
                await websocket.send_json(
                    {
                        "message_type": WebSocketMessageType.INFO.value,
                        "message": f"Unsubscribed from {channel}",
                    }
                )

    except WebSocketDisconnect:
        print("🔌 Client disconnected")
        websocket_manager.disconnect(websocket)
