from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_db_session
from features.auth.auth_service import require_auth
from features.instruments import instrument_service
from features.provider.provider_schema import (
    ProviderCreate,
    ProviderInDb,
    ProviderInstrumentMappingCreate,
    ProviderInstrumentMappingInDb,
)
from features.provider import provider_service
from utils.common_constants import UserRoles, is_admin

provider_router = APIRouter(
    prefix="/provider",
    tags=["providers"],
)


@provider_router.post(
    "/", response_model=ProviderInDb, status_code=status.HTTP_201_CREATED
)
async def create_provider(
    provider_data: ProviderCreate,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Create a new provider"""
    # Check if provider with same code already exists
    existing = await provider_service.get_provider_by_code(session, provider_data.code)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provider with this code already exists",
        )

    return await provider_service.create_provider(session, provider_data)


@provider_router.get("/", response_model=list[ProviderInDb])
async def list_providers(
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Get all providers"""
    return await provider_service.get_all_providers(session)


@provider_router.get("/{provider_id}", response_model=ProviderInDb)
async def get_provider(
    provider_id: int,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Get provider by ID"""
    provider = await provider_service.get_provider_by_id(session, provider_id)
    if not provider:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Provider not found"
        )
    return provider


@provider_router.post(
    "/mapping/",
    response_model=ProviderInstrumentMappingInDb,
    status_code=status.HTTP_201_CREATED,
)
async def create_provider_instrument_mapping(
    mapping_data: ProviderInstrumentMappingCreate,
    user_claims: dict = Depends(require_auth([UserRoles.ADMIN])),
    session: AsyncSession = Depends(get_db_session),
):
    """Create a new provider-instrument mapping"""

    ## Validate provider exists
    provider = await provider_service.get_provider_by_id(
        session, mapping_data.provider_id
    )
    if not provider:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Provider not found",
        )

    only_active = not is_admin(user_claims)

    ## Validate instrument exists
    instrument = await instrument_service.get_instrument_by_id(
        session, mapping_data.instrument_id, only_active=only_active
    )
    if not instrument:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Instrument not found",
        )

    return await provider_service.create_provider_instrument_mapping(
        session, mapping_data
    )


@provider_router.put("/mapping/", response_model=ProviderInstrumentMappingInDb)
async def update_provider_instrument_mapping(
    mapping_data: ProviderInstrumentMappingCreate,
    user_claims: dict = Depends(require_auth([UserRoles.ADMIN])),
    session: AsyncSession = Depends(get_db_session),
):
    """Update a provider-instrument mapping"""

    ## Validate provider exists
    provider = await provider_service.get_provider_by_id(
        session, mapping_data.provider_id
    )
    if not provider:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Provider not found",
        )

    only_active = not is_admin(user_claims)

    ## Validate instrument exists
    instrument = await instrument_service.get_instrument_by_id(
        session, mapping_data.instrument_id, only_active=only_active
    )
    if not instrument:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Instrument not found",
        )

    provider_instrument_update = ProviderInstrumentMappingCreate(
        provider_id=mapping_data.provider_id,
        instrument_id=mapping_data.instrument_id,
        provider_instrument_search_code=mapping_data.provider_instrument_search_code,
    )
    updated_mapping = await provider_service.update_provider_instrument_mapping(
        session, provider_instrument_update
    )
    if not updated_mapping:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Provider-Instrument mapping not found",
        )
    return updated_mapping


@provider_router.get(
    "/mapping/instrument/{instrument_id}",
    response_model=list[ProviderInstrumentMappingInDb],
)
async def get_instrument_provider_mappings(
    instrument_id: int,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Get all provider mappings for a specific instrument"""
    return await provider_service.get_provider_mappings_by_instrument_id(
        session, instrument_id
    )
