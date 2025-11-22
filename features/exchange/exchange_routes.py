from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from config.database_config import get_db_session
from features.auth.auth_service import require_auth
from features.exchange.exchange_schema import (
    ExchangeCreate,
    ExchangeUpdate,
    ExchangeInDb,
    ExchangeProviderMappingCreate,
    ExchangeProviderMappingUpdate,
    ExchangeProviderMappingInDb,
)
from features.exchange import exchange_service
from models import ExchangeProviderMapping

exchange_router = APIRouter(
    prefix="/exchange",
    tags=["exchanges"],
)


@exchange_router.post(
    "/",
    response_model=ExchangeInDb,
    status_code=status.HTTP_201_CREATED,
)
async def create_exchange(
    exchange_data: ExchangeCreate,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Create a new exchange"""
    # Check if exchange with same code already exists
    existing = await exchange_service.get_exchange_by_code(session, exchange_data.code)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Exchange with this code already exists",
        )

    return await exchange_service.create_exchange(session, exchange_data)


@exchange_router.get(
    "/",
    response_model=list[ExchangeInDb],
)
async def list_exchanges(
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Get all exchanges"""
    return await exchange_service.get_all_exchanges(session)


# @exchange_router.get(
#     "/{exchange_id}",
#     response_model=ExchangeInDb,
# )
# async def get_exchange(
#     exchange_id: int,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Get exchange by ID"""
#     exchange = await exchange_service.get_exchange_by_id(session, exchange_id)
#     if not exchange:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND, detail="Exchange not found"
#         )
#     return exchange


# @exchange_router.get(
#     "/code/{code}",
#     response_model=ExchangeInDb,
# )
# async def get_exchange_by_code(
#     code: str,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Get exchange by code"""
#     exchange = await exchange_service.get_exchange_by_code(session, code)
#     if not exchange:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND, detail="Exchange not found"
#         )
#     return exchange


# @exchange_router.put(
#     "/{exchange_id}",
#     response_model=ExchangeInDb,
# )
# async def update_exchange(
#     exchange_id: int,
#     exchange_data: ExchangeUpdate,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Update an exchange"""
#     exchange = await exchange_service.update_exchange(
#         session, exchange_id, exchange_data
#     )
#     if not exchange:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND, detail="Exchange not found"
#         )
#     return exchange


# @exchange_router.delete(
#     "/{exchange_id}",
#     status_code=status.HTTP_204_NO_CONTENT,
# )
# async def delete_exchange(
#     exchange_id: int,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Delete an exchange"""
#     deleted = await exchange_service.delete_exchange(session, exchange_id)
#     if not deleted:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND, detail="Exchange not found"
#         )
#     return None


# Exchange-Provider Mapping routes
# @exchange_router.get(
#     "/{exchange_id}/providers",
#     response_model=list[ExchangeProviderMappingInDb],
# )
# async def get_providers_for_exchange(
#     exchange_id: int,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Get all providers mapped to an exchange"""
#     return await exchange_service.get_mappings_for_exchange(session, exchange_id)
#
#
# @exchange_router.post(
#     "/{exchange_id}/providers/{provider_id}",
#     response_model=ExchangeProviderMappingInDb,
#     status_code=status.HTTP_201_CREATED,
# )
# async def add_provider_to_exchange(
#     exchange_id: int,
#     provider_id: int,
#     mapping_data: ExchangeProviderMappingCreate = None,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Add a provider to an exchange"""
#     if mapping_data is None:
#         mapping_data = ExchangeProviderMappingCreate(
#             provider_id=provider_id, exchange_id=exchange_id
#         )
#     else:
#         if (
#             mapping_data.provider_id != provider_id
#             or mapping_data.exchange_id != exchange_id
#         ):
#             raise HTTPException(status_code=400, detail="Provider or exchange ID mismatch")
#     # Check if already exists
#     existing = await session.execute(
#         select(ExchangeProviderMapping).where(
#             (ExchangeProviderMapping.provider_id == provider_id)
#             & (ExchangeProviderMapping.exchange_id == exchange_id)
#         )
#     )
#     if existing.scalar_one_or_none():
#         raise HTTPException(status_code=400, detail="Mapping already exists")
#     return await exchange_service.create_exchange_provider_mapping(session, mapping_data)
#
#
# @exchange_router.put(
#     "/{exchange_id}/providers/{provider_id}",
#     response_model=ExchangeProviderMappingInDb,
# )
# async def update_provider_exchange_mapping(
#     exchange_id: int,
#     provider_id: int,
#     update_data: ExchangeProviderMappingUpdate,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Update the mapping between a provider and an exchange"""
#     mapping = await exchange_service.update_exchange_provider_mapping(
#         session, provider_id, exchange_id, update_data
#     )
#     if not mapping:
#         raise HTTPException(status_code=404, detail="Mapping not found")
#     return mapping


# @exchange_router.delete(
#     "/{exchange_id}/providers/{provider_id}",
#     status_code=status.HTTP_204_NO_CONTENT,
# )
# async def remove_provider_from_exchange(
#     exchange_id: int,
#     provider_id: int,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Remove a provider from an exchange"""
#     deleted = await exchange_service.delete_exchange_provider_mapping(session, provider_id, exchange_id)
#     if not deleted:
#         raise HTTPException(status_code=404, detail="Mapping not found")
#     return None
