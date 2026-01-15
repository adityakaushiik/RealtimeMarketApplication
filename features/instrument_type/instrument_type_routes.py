from typing import List

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_db_session
from features.auth.auth_service import require_auth
from features.instrument_type import instrument_type_service
from features.instrument_type.instrument_type_schema import (
    InstrumentTypeCreate,
    InstrumentTypeInDb,
    InstrumentTypeUpdate,
)
from utils.common_constants import UserRoles

instrument_type_router = APIRouter(
    prefix="/instrument_type",
    tags=["instrument_types"],
)


@instrument_type_router.post("/")
async def create_instrument_type(
    create_instrument_type: InstrumentTypeCreate,
    user_claims: dict = Depends(require_auth([UserRoles.ADMIN])),
    session: AsyncSession = Depends(get_db_session),
) -> InstrumentTypeInDb:
    instrument_type = await instrument_type_service.create_instrument_type(
        session=session,
        type_data=create_instrument_type,
    )

    return instrument_type


@instrument_type_router.get("/{instrument_type_id}")
async def get_instrument_type(
    instrument_type_id: int,
    user_claims: dict = Depends(require_auth([UserRoles.ADMIN])),
    session: AsyncSession = Depends(get_db_session),
) -> InstrumentTypeInDb:
    instrument_type = await instrument_type_service.get_instrument_type_by_id(
        session=session,
        type_id=instrument_type_id,
    )
    return instrument_type


@instrument_type_router.get("/")
async def list_instrument_types(
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
) -> List[InstrumentTypeInDb]:
    instrument_types = await instrument_type_service.get_all_instrument_types(
        session=session
    )
    return instrument_types


@instrument_type_router.put("/{instrument_type_id}")
async def update_instrument_type(
    instrument_type_id: int,
    update_data: InstrumentTypeUpdate,
    user_claims: dict = Depends(require_auth([UserRoles.ADMIN])),
    session: AsyncSession = Depends(get_db_session),
) -> InstrumentTypeInDb:
    instrument_type = await instrument_type_service.update_instrument_type(
        session=session,
        type_id=instrument_type_id,
        type_data=update_data,
    )
    return instrument_type
