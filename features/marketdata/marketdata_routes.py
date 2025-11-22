from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_db_session
from features.auth.auth_service import require_auth
from features.marketdata import marketdata_service
from features.marketdata.marketdata_schema import (
    PriceHistoryIntradayCreate,
    PriceHistoryIntradayInDb,
    PriceHistoryDailyCreate,
    PriceHistoryDailyInDb,
)
from services.redis_timeseries import RedisTimeSeries, get_redis_timeseries

marketdata_router = APIRouter(
    prefix="/marketdata",
    tags=["marketdata"],
)


@marketdata_router.get("/ohlcv")
async def get_ohlcv(
        user_claims: dict = Depends(require_auth()),
        symbol: str = Query(..., description="Symbol key used for time series e.g. AAPL"),
        window_minutes: int = Query(
            15, ge=1, le=15, description="Lookback window in minutes"
        ),
        bucket_minutes: int = Query(5, ge=1, le=15, description="Bucket size in minutes"),
        redis_ts: RedisTimeSeries = Depends(get_redis_timeseries),
):
    try:
        data = await redis_ts.get_ohlcv_series(
            symbol, window_minutes=window_minutes, bucket_minutes=bucket_minutes
        )
        return {
            "symbol": symbol,
            "window_minutes": window_minutes,
            "bucket_minutes": bucket_minutes,
            "bars": data,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# PriceHistoryIntraday CRUD
# @marketdata_router.post("/intraday", response_model=PriceHistoryIntradayInDb, status_code=201)
# async def create_intraday_price(
#     data: PriceHistoryIntradayCreate,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Create a new intraday price history record"""
#     return await marketdata_service.create_price_history_intraday(session, data)


@marketdata_router.get("/intraday", response_model=list[PriceHistoryIntradayInDb])
async def list_intraday_prices(
        user_claims: dict = Depends(require_auth()),
        session: AsyncSession = Depends(get_db_session),
):
    """Get all intraday price history records"""
    return await marketdata_service.get_all_price_history_intraday(session)


# @marketdata_router.get("/intraday/{record_id}", response_model=PriceHistoryIntradayInDb)
# async def get_intraday_price(
#     record_id: int,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Get intraday price history by ID"""
#     record = await marketdata_service.get_price_history_intraday_by_id(session, record_id)
#     if not record:
#         raise HTTPException(status_code=404, detail="Intraday price record not found")
#     return record


# @marketdata_router.put("/intraday/{record_id}", response_model=PriceHistoryIntradayInDb)
# async def update_intraday_price(
#     record_id: int,
#     data: PriceHistoryIntradayUpdate,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Update an intraday price history record"""
#     record = await marketdata_service.update_price_history_intraday(session, record_id, data)
#     if not record:
#         raise HTTPException(status_code=404, detail="Intraday price record not found")
#     return record


# @marketdata_router.delete("/intraday/{record_id}", status_code=204)
# async def delete_intraday_price(
#     record_id: int,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Delete an intraday price history record"""
#     deleted = await marketdata_service.delete_price_history_intraday(session, record_id)
#     if not deleted:
#         raise HTTPException(status_code=404, detail="Intraday price record not found")
#     return None


# PriceHistoryDaily CRUD
# @marketdata_router.post("/daily", response_model=PriceHistoryDailyInDb, status_code=201)
# async def create_daily_price(
#     data: PriceHistoryDailyCreate,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Create a new daily price history record"""
#     return await marketdata_service.create_price_history_daily(session, data)


@marketdata_router.get("/daily", response_model=list[PriceHistoryDailyInDb])
async def list_daily_prices(
        user_claims: dict = Depends(require_auth()),
        session: AsyncSession = Depends(get_db_session),
):
    """Get all daily price history records"""
    return await marketdata_service.get_all_price_history_daily(session)

# @marketdata_router.get("/daily/{record_id}", response_model=PriceHistoryDailyInDb)
# async def get_daily_price(
#     record_id: int,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Get daily price history by ID"""
#     record = await marketdata_service.get_price_history_daily_by_id(session, record_id)
#     if not record:
#         raise HTTPException(status_code=404, detail="Daily price record not found")
#     return record
#
#
# @marketdata_router.put("/daily/{record_id}", response_model=PriceHistoryDailyInDb)
# async def update_daily_price(
#     record_id: int,
#     data: PriceHistoryDailyUpdate,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Update a daily price history record"""
#     record = await marketdata_service.update_price_history_daily(session, record_id, data)
#     if not record:
#         raise HTTPException(status_code=404, detail="Daily price record not found")
#     return record


# @marketdata_router.delete("/daily/{record_id}", status_code=204)
# async def delete_daily_price(
#     record_id: int,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Delete a daily price history record"""
#     deleted = await marketdata_service.delete_price_history_daily(session, record_id)
#     if not deleted:
#         raise HTTPException(status_code=404, detail="Daily price record not found")
#     return None
