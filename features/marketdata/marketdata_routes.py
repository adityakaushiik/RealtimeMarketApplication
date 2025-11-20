from fastapi import APIRouter, Depends, HTTPException, Query
from features.auth.auth_service import require_auth
from services.redis_timeseries import RedisTimeSeries

marketdata_router = APIRouter(
    prefix="/marketdata",
    tags=["marketdata"],
)

redis_ts = RedisTimeSeries()


@marketdata_router.get("/ohlcv")
async def get_ohlcv(
        symbol: str = Query(..., description="Symbol key used for time series e.g. AAPL"),
        window_minutes: int = Query(
            15, ge=1, le=15, description="Lookback window in minutes"
        ),
        bucket_minutes: int = Query(5, ge=1, le=15, description="Bucket size in minutes"),
        user_claims: dict = Depends(require_auth()),
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
