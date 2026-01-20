import json
import datetime

from fastapi import WebSocket, WebSocketDisconnect, APIRouter
from fastapi.params import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_database_engine
from config.redis_config import get_redis
from features.instruments.instrument_service import get_instrument_by_symbol
from features.marketdata.marketdata_service import (
    get_previous_closes_by_exchange,
    get_previous_close_for_symbol,
)
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
            exchange_id = msg.get("exchange_id")
            exchange_code = msg.get("exchange_code")

            if (
                message_type == WebSocketMessageType.SUBSCRIBE.value
                and channel
                and exchange_id
            ):
                websocket_manager.subscribe(websocket, channel)

                # Get Snapshot Data
                ltp = await redis_ts.get_last_price(channel)

                # Get Day Open
                today_start = datetime.datetime.now(datetime.timezone.utc).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                start_ts = int(today_start.timestamp() * 1000)
                daily_candle = await redis_ts.get_current_daily_candle(
                    channel, start_ts
                )
                open_price = daily_candle.get("open") if daily_candle else None

                # OPTIMIZATION: Use robust single-source lookup
                # This checks Redis O(1) -> DB Fallback (Last Trading Day)
                # Removes double-request redundancy.
                prev_close = None
                engine = get_database_engine()
                async with AsyncSession(engine) as session:
                    prev_close = await get_previous_close_for_symbol(
                        session, exchange_code if exchange_code else "UNKNOWN", channel
                    )

                # Send Snapshot
                await websocket.send_json(
                    {
                        "message_type": WebSocketMessageType.SNAPSHOT.value,
                        "symbol": channel,
                        "prev_close": prev_close if prev_close is not None else -1,
                        "ltp": ltp if ltp is not None else -1,
                        "open": open_price if open_price is not None else -1,
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
