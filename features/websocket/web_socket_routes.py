import json

from fastapi import WebSocket, WebSocketDisconnect, APIRouter
from fastapi.params import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_database_engine
from features.instruments.instrument_service import get_instrument_by_symbol
from features.marketdata.marketdata_service import get_price_history_daily
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

                # Send subscription confirmation
                await websocket.send_json(
                    {
                        "message_type": WebSocketMessageType.INFO.value,
                        "message": f"Subscribed to {channel}",
                    }
                )

                # # Fetch and send daily stats (e.g. prev_close)
                # try:
                #     # 1. Try Redis Cache
                #     stats = await redis_ts.get_daily_stats(channel)
                #
                #     # 2. If miss, fetch from DB and cache
                #     if not stats:
                #         engine = get_database_engine()
                #         async with AsyncSession(engine) as session:
                #             instrument = await get_instrument_by_symbol(session, channel)
                #             if instrument:
                #                 # Get latest daily record
                #                 daily_records = await get_price_history_daily(session, instrument.id)
                #                 if daily_records:
                #                     # The latest record in DB is usually the previous day's close
                #                     # (assuming today's isn't written until EOD or we handle it elsewhere)
                #                     prev_close = daily_records[0].close
                #                     stats = {"prev_close": prev_close}
                #                     await redis_ts.set_daily_stats(channel, stats)
                #
                #     if stats:
                #         await websocket.send_json(
                #             {
                #                 "message_type": WebSocketMessageType.INFO.value,
                #                 "symbol": channel,
                #                 "stats": stats
                #             }
                #         )
                # except Exception as e:
                #     print(f"Error fetching stats for {channel}: {e}")

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
