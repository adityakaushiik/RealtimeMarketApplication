import asyncio

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_db_session
from features.auth.auth_service import require_auth
from features.instruments.instrument_schema import InstrumentInDb
from features.instruments.instrument_service import get_instrument_by_symbol
from features.marketdata import marketdata_service
from features.marketdata.marketdata_schema import (
    PriceHistoryIntradayCreate,
    PriceHistoryIntradayInDb,
    PriceHistoryDailyCreate,
    PriceHistoryDailyInDb,
)
from features.marketdata.marketdata_service import (
    get_price_history_daily,
    get_price_history_intraday,
)
from models import PriceHistoryIntraday
from services.redis_timeseries import RedisTimeSeries, get_redis_timeseries
from utils.common_constants import SupportedIntervals

marketdata_router = APIRouter(
    prefix="/marketdata",
    tags=["marketdata"],
)


# @marketdata_router.get("/ohlcv")
# async def get_ohlcv(
#         user_claims: dict = Depends(require_auth()),
#         symbol: str = Query(..., description="Symbol key used for time series e.g. AAPL"),
#         window_minutes: int = Query(
#             15, ge=1, le=15, description="Lookback window in minutes"
#         ),
#         bucket_minutes: int = Query(5, ge=1, le=15, description="Bucket size in minutes"),
#         redis_ts: RedisTimeSeries = Depends(get_redis_timeseries),
# ):
#     try:
#         data = await redis_ts.get_ohlcv_series(
#             symbol, window_minutes=window_minutes, bucket_minutes=bucket_minutes
#         )
#         return {
#             "symbol": symbol,
#             "window_minutes": window_minutes,
#             "bucket_minutes": bucket_minutes,
#             "bars": data,
#         }
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


@marketdata_router.get("/daily/{symbol}")
async def get_info_and_price_daily(
    symbol: str,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    # Check if symbol is valid
    instrument: InstrumentInDb = await get_instrument_by_symbol(session, symbol)
    if instrument is None:
        raise HTTPException(status_code=404, detail="Instrument not found")

    # # Gather All Tasks
    # tasks = []
    #
    # # 1. Fetch Price History from DB
    # if interval == SupportedIntervals.FIVE_MINUTES:
    #     tasks.append(get_price_history_intraday(session=session, instrument_id=instrument.id))
    # elif interval == SupportedIntervals.ONE_DAY:
    #     tasks.append(get_price_history_daily(session=session, instrument_id=instrument.id))
    # else:
    #     tasks.append(asyncio.sleep(0))
    #
    # # 2. Resolve Today's Price from Redis
    # tasks.append(redis_ts.get_ohlcv_last_5m(symbol))
    #
    # # Execute Gathered Tasks
    # results = await asyncio.gather(*tasks)
    # price_history = results[0]
    # current_5m_data = results[1]
    #
    # # Merge logic for 5m interval
    # if interval == SupportedIntervals.FIVE_MINUTES and current_5m_data and current_5m_data.get("ts"):
    #     from datetime import datetime as dt, timezone
    #     # price_history is sorted DESC (newest first)
    #     latest_db_dt = price_history[0].datetime if price_history else dt.min.replace(tzinfo=timezone.utc)
    #     latest_redis_dt = dt.fromtimestamp(int(current_5m_data["ts"]) / 1000, tz=timezone.utc)
    #
    #     # Only append if Redis data is newer than DB data
    #     if latest_redis_dt > latest_db_dt:
    #         latest_candle = PriceHistoryIntradayInDb(
    #             id=-1,  # Temporary ID for live data
    #             instrument_id=instrument.id,
    #             datetime=latest_redis_dt,
    #             open=current_5m_data["open"],
    #             high=current_5m_data["high"],
    #             low=current_5m_data["low"],
    #             close=current_5m_data["close"],
    #             volume=int(current_5m_data["volume"]),
    #             interval=interval.value
    #         )
    #         # Insert at the beginning (DESC order)
    #         price_history.insert(0, latest_candle)
    #
    # # Merge logic for 1day interval
    # elif interval == SupportedIntervals.ONE_DAY and current_5m_data and current_5m_data.get("ts"):
    #     from datetime import datetime, timezone
    #
    #     # Calculate start of day for the Redis timestamp (assuming UTC for now, or exchange time)
    #     # Redis timestamp is in milliseconds
    #     redis_ts_ms = int(current_5m_data["ts"])
    #     redis_dt = datetime.fromtimestamp(redis_ts_ms / 1000, tz=timezone.utc)
    #
    #     # Truncate to start of day (00:00:00)
    #     start_of_day_dt = redis_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    #     start_of_day_dt_naive = start_of_day_dt.replace(tzinfo=None)
    #
    #     latest_db_dt = price_history[0].datetime.replace(tzinfo=None) if price_history else datetime.min
    #
    #     # If DB has a record for today (timestamps match), update it
    #     if latest_db_dt == start_of_day_dt_naive:
    #         # Update the latest candle with live data
    #         # Note: We only update Close, High, Low, Volume. Open stays as is.
    #         latest_candle = price_history[0]
    #         latest_candle.close = current_5m_data["close"]
    #         latest_candle.high = max(latest_candle.high, current_5m_data["high"])
    #         latest_candle.low = min(latest_candle.low, current_5m_data["low"])
    #         latest_candle.volume = (latest_candle.volume or 0) + int(current_5m_data["volume"]) # Volume is cumulative? Or replacement?
    #         # Usually volume in daily is total volume. Redis 5m volume is just for that 5m.
    #         # If we don't have total volume for the day in Redis, we can't accurately update volume.
    #         # But for price (Close), it is accurate.
    #         pass
    #
    #     # If DB does NOT have a record for today (DB is older), append a new one
    #     elif start_of_day_dt_naive > latest_db_dt:
    #          # Create a new daily candle from the 5m data
    #          # This is an approximation since we don't have full day history in Redis
    #         new_daily_candle = PriceHistoryDailyInDb(
    #             id=-1,
    #             instrument_id=instrument.id,
    #             datetime=start_of_day_dt_naive,
    #             open=current_5m_data["open"], # Approximation
    #             high=current_5m_data["high"], # Approximation
    #             low=current_5m_data["low"],   # Approximation
    #             close=current_5m_data["close"],
    #             volume=int(current_5m_data["volume"]), # Approximation (only last 5m volume)
    #             price_not_found=False
    #         )
    #         price_history.insert(0, new_daily_candle)

    # For now we are just returning all daily data from DB
    # You should return data before or equal to current timing
    price_history = await get_price_history_daily(
        session=session, instrument_id=instrument.id
    )

    return price_history


@marketdata_router.get("/intraday/{symbol}")
async def get_info_and_price_intraday(
    symbol: str,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    # Check if symbol is valid
    instrument: InstrumentInDb = await get_instrument_by_symbol(session, symbol)
    if instrument is None:
        raise HTTPException(status_code=404, detail="Instrument not found")

    try:
        price_history = await get_price_history_intraday(
            session=session, instrument_id=instrument.id
        )
        return price_history
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@marketdata_router.get("/prev_close/{exchange_code}")
async def get_previous_closes(
    exchange_code: str,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    try:
        prev_closes = await marketdata_service.get_previous_closes_by_exchange(
            session=session, exchange_code=exchange_code
        )
        return {"exchange_code": exchange_code, "data": prev_closes}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
