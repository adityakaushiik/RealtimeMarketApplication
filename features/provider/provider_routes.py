from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from config.database_config import get_db_session
from features.auth.auth_service import require_auth
from features.provider.provider_schema import (
    ProviderCreate,
    ProviderUpdate,
    ProviderInDb,
    ProviderInstrumentMappingCreate,
    ProviderInstrumentMappingUpdate,
    ProviderInstrumentMappingInDb,
)
from features.provider import provider_service
from models import ProviderInstrumentMapping

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


# @provider_router.get("/{provider_id}", response_model=ProviderInDb)
# async def get_provider(
#     provider_id: int,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Get provider by ID"""
#     provider = await provider_service.get_provider_by_id(session, provider_id)
#     if not provider:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND, detail="Provider not found"
#         )
#     return provider


# @provider_router.get("/code/{code}", response_model=ProviderInDb)
# async def get_provider_by_code(
#     code: str,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Get provider by code"""
#     provider = await provider_service.get_provider_by_code(session, code)
#     if not provider:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND, detail="Provider not found"
#         )
#     return provider


# @provider_router.put("/{provider_id}", response_model=ProviderInDb)
# async def update_provider(
#     provider_id: int,
#     provider_data: ProviderUpdate,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Update a provider"""
#     provider = await provider_service.update_provider(
#         session, provider_id, provider_data
#     )
#     if not provider:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND, detail="Provider not found"
#         )
#     return provider


# @provider_router.delete("/{provider_id}", status_code=status.HTTP_204_NO_CONTENT)
# async def delete_provider(
#     provider_id: int,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Delete a provider"""
#     deleted = await provider_service.delete_provider(session, provider_id)
#     if not deleted:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND, detail="Provider not found"
#         )
#     return None


# Provider-Instrument Mapping routes

# @provider_router.get(
#     "/{provider_id}/instruments",
#     response_model=list[ProviderInstrumentMappingInDb],
# )
# async def get_instruments_for_provider(
#     provider_id: int,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Get all instruments mapped to a provider"""
#     return await provider_service.get_mappings_for_provider(session, provider_id)


# @provider_router.post(
#     "/{provider_id}/instruments/{instrument_id}",
#     response_model=ProviderInstrumentMappingInDb,
#     status_code=status.HTTP_201_CREATED,
# )
# async def add_instrument_to_provider(
#     provider_id: int,
#     instrument_id: int,
#     mapping_data: ProviderInstrumentMappingCreate,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Add an instrument to a provider"""
#     mapping_data.provider_id = provider_id
#     mapping_data.instrument_id = instrument_id
#     # Check if already exists
#     existing = await session.execute(
#         select(ProviderInstrumentMapping).where(
#             (ProviderInstrumentMapping.provider_id == provider_id) &
#             (ProviderInstrumentMapping.instrument_id == instrument_id)
#         )
#     )
#     if existing.scalar_one_or_none():
#         raise HTTPException(status_code=400, detail="Mapping already exists")
#     return await provider_service.create_provider_instrument_mapping(session, mapping_data)


# @provider_router.put(
#     "/{provider_id}/instruments/{instrument_id}",
#     response_model=ProviderInstrumentMappingInDb,
# )
# async def update_provider_instrument_mapping(
#     provider_id: int,
#     instrument_id: int,
#     update_data: ProviderInstrumentMappingUpdate,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Update the mapping between a provider and an instrument"""
#     mapping = await provider_service.update_provider_instrument_mapping(
#         session, provider_id, instrument_id, update_data
#     )
#     if not mapping:
#         raise HTTPException(status_code=404, detail="Mapping not found")
#     return mapping


# @provider_router.delete(
#     "/{provider_id}/instruments/{instrument_id}",
#     status_code=status.HTTP_204_NO_CONTENT,
# )
# async def remove_instrument_from_provider(
#     provider_id: int,
#     instrument_id: int,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Remove an instrument from a provider"""
#     deleted = await provider_service.delete_provider_instrument_mapping(session, provider_id, instrument_id)
#     if not deleted:
#         raise HTTPException(status_code=404, detail="Mapping not found")
#     return None
