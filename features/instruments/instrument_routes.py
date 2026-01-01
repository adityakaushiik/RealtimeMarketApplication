from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_db_session
from features.auth.auth_service import require_auth
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
    limit: int = 50,
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
            detail="Invalid or Unsupported exchange code",
        )

    stmt = select(Instrument).where(Instrument.exchange_id == exchange_obj.id)
    if not is_admin(user_claims):
        stmt = stmt.where(Instrument.is_active == True)

    stmt = stmt.limit(limit)

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


@instrument_router.get("/", response_model=list[InstrumentInDb])
async def list_all_instruments(
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Get all instruments"""
    only_active = not is_admin(user_claims)
    return await instrument_service.get_all_instruments(session, only_active=True)


@instrument_router.get("/search")
async def search_instruments(
    query: str,
    session: AsyncSession = Depends(get_db_session),
    user_claims: dict = Depends(require_auth()),
):
    only_active = not is_admin(user_claims)
    return await instrument_service.search_instruments(session, query, only_active=only_active)


@instrument_router.get("/{instrument_id}", response_model=InstrumentInDb)
async def get_instrument(
    instrument_id: int,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Get instrument by ID"""
    only_active = not is_admin(user_claims)
    instrument = await instrument_service.get_instrument_by_id(session, instrument_id, only_active=only_active)
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