from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_db_session
from features.auth.auth_service import require_auth
from models import Instrument, Exchange
from features.instruments.instrument_schema import (
    InstrumentCreate,
    InstrumentUpdate,
    InstrumentInDb,
    InstrumentTypeCreate,
    InstrumentTypeUpdate,
    InstrumentTypeInDb,
    SectorCreate,
    SectorUpdate,
    SectorInDb,
)
from features.instruments import instrument_service

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
@instrument_router.post("/", response_model=InstrumentInDb, status_code=status.HTTP_201_CREATED)
async def create_instrument(
    instrument_data: InstrumentCreate,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Create a new instrument"""
    return await instrument_service.create_instrument(session, instrument_data)


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
    instrument = await instrument_service.update_instrument(session, instrument_id, instrument_data)
    if not instrument:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Instrument not found"
        )
    return instrument


# @instrument_router.delete("/{instrument_id}", status_code=status.HTTP_204_NO_CONTENT)
# async def delete_instrument(
#     instrument_id: int,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Delete an instrument"""
#     deleted = await instrument_service.delete_instrument(session, instrument_id)
#     if not deleted:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND, detail="Instrument not found"
#         )
#     return None


# InstrumentType CRUD
# @instrument_router.post("/type", response_model=InstrumentTypeInDb, status_code=status.HTTP_201_CREATED)
# async def create_instrument_type(
#     type_data: InstrumentTypeCreate,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Create a new instrument type"""
#     return await instrument_service.create_instrument_type(session, type_data)


# @instrument_router.get("/type", response_model=list[InstrumentTypeInDb])
# async def list_instrument_types(
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Get all instrument types"""
#     return await instrument_service.get_all_instrument_types(session)
#
#
# @instrument_router.get("/type/{type_id}", response_model=InstrumentTypeInDb)
# async def get_instrument_type(
#     type_id: int,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Get instrument type by ID"""
#     type_obj = await instrument_service.get_instrument_type_by_id(session, type_id)
#     if not type_obj:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND, detail="Instrument type not found"
#         )
#     return type_obj


# @instrument_router.put("/type/{type_id}", response_model=InstrumentTypeInDb)
# async def update_instrument_type(
#     type_id: int,
#     type_data: InstrumentTypeUpdate,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Update an instrument type"""
#     type_obj = await instrument_service.update_instrument_type(session, type_id, type_data)
#     if not type_obj:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND, detail="Instrument type not found"
#         )
#     return type_obj


# @instrument_router.delete("/type/{type_id}", status_code=status.HTTP_204_NO_CONTENT)
# async def delete_instrument_type(
#     type_id: int,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Delete an instrument type"""
#     deleted = await instrument_service.delete_instrument_type(session, type_id)
#     if not deleted:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND, detail="Instrument type not found"
#         )
#     return None


# Sector CRUD
# @instrument_router.post("/sector", response_model=SectorInDb, status_code=status.HTTP_201_CREATED)
# async def create_sector(
#     sector_data: SectorCreate,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Create a new sector"""
#     return await instrument_service.create_sector(session, sector_data)
#
#
# @instrument_router.get("/sector", response_model=list[SectorInDb])
# async def list_sectors(
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Get all sectors"""
#     return await instrument_service.get_all_sectors(session)
#
#
# @instrument_router.get("/sector/{sector_id}", response_model=SectorInDb)
# async def get_sector(
#     sector_id: int,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Get sector by ID"""
#     sector = await instrument_service.get_sector_by_id(session, sector_id)
#     if not sector:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND, detail="Sector not found"
#         )
#     return sector
#
#
# @instrument_router.put("/sector/{sector_id}", response_model=SectorInDb)
# async def update_sector(
#     sector_id: int,
#     sector_data: SectorUpdate,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Update a sector"""
#     sector = await instrument_service.update_sector(session, sector_id, sector_data)
#     if not sector:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND, detail="Sector not found"
#         )
#     return sector


# @instrument_router.delete("/sector/{sector_id}", status_code=status.HTTP_204_NO_CONTENT)
# async def delete_sector(
#     sector_id: int,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Delete a sector"""
#     deleted = await instrument_service.delete_sector(session, sector_id)
#     if not deleted:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND, detail="Sector not found"
#         )
#     return None
