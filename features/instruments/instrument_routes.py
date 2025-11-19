from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_db_session
from features.auth.auth_service import require_auth
from models import Instrument, Exchange

instrument_router = APIRouter(
    prefix="/instrument",
    tags=["instruments"],
)


@instrument_router.get("/list/{exchange}")
async def list_instruments(
    exchange: str,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    # Ideally Fetch from Redis Cache first
    exchange_result = await session.execute(
        select(Exchange).where(Exchange.code == exchange)
    )
    exchange_obj = exchange_result.scalar_one_or_none()
    if not exchange_obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Invalid or Unsupported exchange code"
        )

    result = await session.execute(select(Instrument).where(Instrument.exchange_id == exchange_obj.id))
    instrument_list = result.scalars().all()
    return instrument_list


@instrument_router.get("/details/{exchange_symbol_string}")
async def instrument_details(
    exchange_symbol_string: str,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    user_query = exchange_symbol_string.split(":", 1)
    if len(user_query) != 2:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid exchange:symbol format"
        )

    exchange, symbol = user_query

    # Ideally Fetch from Redis Cache first
    exchange_result = await session.execute(
        select(Exchange).where(Exchange.code == exchange)
    )
    exchange_obj = exchange_result.scalar_one_or_none()
    if not exchange_obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Invalid or Unsupported exchange code"
        )

    # Fetch Instrument
    result = await session.execute(
        select(Instrument).where(
            (Instrument.symbol == symbol) & (Instrument.exchange_id == exchange_obj.id)
        )
    )
    instrument = result.scalar_one_or_none() or []

    return instrument
