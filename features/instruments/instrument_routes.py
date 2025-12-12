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
from features.provider.provider_schema import ProviderInstrumentMappingCreate
from features.provider.provider_service import (
    get_provider_by_id,
    create_provider_instrument_mapping,
)
from models import Instrument, Exchange
from utils.common_constants import UserRoles

instrument_router = APIRouter(
    prefix="/instrument",
    tags=["instruments"],
)


# Existing routes
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
            detail="Invalid or Unsupported exchange code",
        )

    result = await session.execute(
        select(Instrument).where(Instrument.exchange_id == exchange_obj.id)
    )
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
    result = await session.execute(
        select(Instrument).where(
            (Instrument.symbol == symbol) & (Instrument.exchange_id == exchange_obj.id)
        )
    )
    instrument = result.scalar_one_or_none() or []

    return instrument


# Instrument CRUD
@instrument_router.post(
    "/", response_model=InstrumentInDb, status_code=status.HTTP_201_CREATED
)
async def create_instrument(
    instrument_data: InstrumentCreate,
    user_claims: dict = Depends(require_auth([UserRoles.ADMIN])),
    session: AsyncSession = Depends(get_db_session),
    provider_id: int = None,
    provider_search_code: str = None,
):
    """Create a new instrument and map it to a provider"""

    if not provider_id or not provider_search_code:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provider ID and Provider Search Code are required",
        )

    provider = await get_provider_by_id(session, provider_id)
    if not provider:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Provider not found",
        )

    instrument = await instrument_service.create_instrument(session, instrument_data)
    provider_instrument_mapping = ProviderInstrumentMappingCreate(
        provider_id=provider_id,
        instrument_id=instrument.id,
        provider_instrument_search_code=provider_search_code,
    )
    create_mappings = await create_provider_instrument_mapping(
        session, provider_instrument_mapping
    )

    return instrument


@instrument_router.get("/", response_model=list[InstrumentInDb])
async def list_all_instruments(
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Get all instruments"""
    return await instrument_service.get_all_instruments(session)


@instrument_router.get("/{instrument_id}", response_model=InstrumentInDb)
async def get_instrument(
    instrument_id: int,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Get instrument by ID"""
    instrument = await instrument_service.get_instrument_by_id(session, instrument_id)
    if not instrument:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Instrument not found"
        )
    return instrument


@instrument_router.put("/{instrument_id}", response_model=InstrumentInDb)
async def update_instrument(
    instrument_id: int,
    instrument_data: InstrumentUpdate,
    user_claims: dict = Depends(require_auth()),
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
