from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_db_session
from features.auth.auth_service import require_auth
from features.instruments.instrument_schema import InstrumentInDb
from features.instruments.instrument_service import get_instrument_by_symbol
from features.marketdata import marketdata_service
from features.marketdata.marketdata_service import (
    get_combined_daily_price_history,
    get_combined_intraday_price_history,
)
from utils.common_constants import is_admin

marketdata_router = APIRouter(
    prefix="/marketdata",
    tags=["marketdata"],
)


@marketdata_router.get("/daily/{symbol}")
async def get_info_and_price_daily(
    symbol: str,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    # Determine if only active instruments should be considered
    only_active = not is_admin(user_claims)

    # Check if symbol is valid
    instrument: InstrumentInDb = await get_instrument_by_symbol(
        session, symbol, only_active=only_active
    )
    if instrument is None:
        raise HTTPException(status_code=404, detail="Instrument not found")

    return await get_combined_daily_price_history(session, instrument)


@marketdata_router.get("/intraday/{symbol}")
async def get_info_and_price_intraday(
    symbol: str,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    # Determine if only active instruments should be considered
    only_active = not is_admin(user_claims)

    # Check if symbol is valid
    instrument: InstrumentInDb = await get_instrument_by_symbol(
        session, symbol, only_active=only_active
    )
    if instrument is None:
        raise HTTPException(status_code=404, detail="Instrument not found")

    return await get_combined_intraday_price_history(session, instrument)


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
