from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_db_session
from features.auth.auth_service import require_auth
from features.exchange.exchange_schema import ExchangeInDb
from features.exchange.exchange_service import get_exchange_by_code
from features.instruments import instrument_service
from features.instruments.instrument_schema import (
    InstrumentCreate,
    InstrumentUpdate,
    InstrumentInDb,
)
from models import Instrument, Exchange
from utils.common_constants import UserRoles, is_admin

instrument_router = APIRouter(
    prefix="/instrument",
    tags=["instruments"],
)


# Existing routes
@instrument_router.get("/list/{exchange}")
async def list_instruments(
    exchange: str,
    recording_only: bool | None = None,
    instrument_type_id: int | None = None,
    limit: int = 200,
    user_claims: dict = Depends(require_auth()),
    order_by: str = 'asc',
    session: AsyncSession = Depends(get_db_session),
):
    """List instruments for a given exchange"""

    exchange_obj: ExchangeInDb = await get_exchange_by_code(session, exchange)
    if not exchange_obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Invalid or Unsupported exchange code",
        )

    stmt = select(Instrument).where(Instrument.exchange_id == exchange_obj.id)

    if recording_only:
        stmt = stmt.where(Instrument.should_record_data == True)

    if not is_admin(user_claims):
        stmt = stmt.where(Instrument.is_active == True)

    if instrument_type_id is not None:
        stmt = stmt.where(Instrument.instrument_type_id == instrument_type_id)

    stmt = stmt.order_by(
        Instrument.updated_at.asc() if order_by == 'asc' else Instrument.updated_at.desc()
    ).limit(limit)

    result = await session.execute(stmt)
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
            detail="Invalid exchange:symbol format",
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
            detail="Invalid or Unsupported exchange code",
        )

    # Fetch Instrument
    stmt = select(Instrument).where(
        (Instrument.symbol == symbol) & (Instrument.exchange_id == exchange_obj.id)
    )
    if not is_admin(user_claims):
        stmt = stmt.where(Instrument.is_active == True)

    result = await session.execute(stmt)
    instrument = result.scalar_one_or_none()

    if not instrument:
        return None

    return instrument


# Instrument CRUD
@instrument_router.post(
    "/", response_model=InstrumentInDb, status_code=status.HTTP_201_CREATED
)
async def create_instrument(
    instrument_data: InstrumentCreate,
    user_claims: dict = Depends(require_auth([UserRoles.ADMIN])),
    session: AsyncSession = Depends(get_db_session),
):
    """Create a new instrument and map it to a provider"""
    instrument = await instrument_service.create_instrument(session, instrument_data)
    return instrument


@instrument_router.get("/search")
async def search_instruments(
    query: str,
    exchange: str | None = None,
    session: AsyncSession = Depends(get_db_session),
    user_claims: dict = Depends(require_auth()),
):
    only_active = not is_admin(user_claims)

    exchange_obj: ExchangeInDb = await get_exchange_by_code(session, exchange)
    if not exchange_obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Invalid or Unsupported exchange code",
        )

    return await instrument_service.search_instruments(
        session, query, only_active=only_active, exchange_id=exchange_obj.id
    )


@instrument_router.get("/{instrument_id}", response_model=InstrumentInDb)
async def get_instrument(
    instrument_id: int,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Get instrument by ID"""
    only_active = not is_admin(user_claims)
    instrument = await instrument_service.get_instrument_by_id(
        session, instrument_id, only_active=only_active
    )
    if not instrument:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Instrument not found"
        )
    return instrument


@instrument_router.get("/by-symbol/{symbol}", response_model=InstrumentInDb)
async def get_instrument_by_symbol(
    symbol: str,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Get instrument by symbol (without exchange)"""
    only_active = not is_admin(user_claims)
    instrument = await instrument_service.get_instrument_by_symbol(
        session, symbol, only_active=only_active
    )
    if not instrument:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Instrument not found"
        )
    return instrument


@instrument_router.patch("/{instrument_id}/recording", response_model=InstrumentInDb)
async def toggle_recording(
    instrument_id: int,
    should_record: bool,
    user_claims: dict = Depends(require_auth([UserRoles.ADMIN])),
    session: AsyncSession = Depends(get_db_session),
):
    """
    Enable or disable data recording for an instrument.
    Requires ADMIN role.
    """
    if not is_admin(user_claims):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can change recording settings",
        )

    try:
        instrument = await instrument_service.toggle_instrument_recording(
            session, instrument_id, should_record
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

    if not instrument:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Instrument not found"
        )

    return instrument


@instrument_router.put("/{instrument_id}", response_model=InstrumentInDb)
async def update_instrument(
    instrument_id: int,
    instrument_data: InstrumentUpdate,
    user_claims: dict = Depends(require_auth([UserRoles.ADMIN])),
    session: AsyncSession = Depends(get_db_session),
):
    """Update an instrument"""
    instrument = await instrument_service.update_instrument(
        session, instrument_id, instrument_data
    )
    if not instrument:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Instrument not found"
        )
    return instrument


@instrument_router.get("/recording/all")
async def list_recording_instruments(
    user_claims: dict = Depends(require_auth([UserRoles.ADMIN])),
    session: AsyncSession = Depends(get_db_session),
):
    """List all instruments that have data recording enabled"""
    return await instrument_service.get_recording_instruments(session)
