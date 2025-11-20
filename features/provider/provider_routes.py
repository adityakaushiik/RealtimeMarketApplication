from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_db_session
from features.auth.auth_service import require_auth
from features.provider.provider_schema import (
    ProviderCreate,
    ProviderUpdate,
    ProviderInDb,
)
from features.provider import provider_service

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


@provider_router.get("/code/{code}", response_model=ProviderInDb)
async def get_provider_by_code(
    code: str,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Get provider by code"""
    provider = await provider_service.get_provider_by_code(session, code)
    if not provider:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Provider not found"
        )
    return provider


@provider_router.put("/{provider_id}", response_model=ProviderInDb)
async def update_provider(
    provider_id: int,
    provider_data: ProviderUpdate,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Update a provider"""
    provider = await provider_service.update_provider(
        session, provider_id, provider_data
    )
    if not provider:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Provider not found"
        )
    return provider


@provider_router.delete("/{provider_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_provider(
    provider_id: int,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Delete a provider"""
    deleted = await provider_service.delete_provider(session, provider_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Provider not found"
        )
    return None
