from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_db_session
from features.auth.auth_service import require_auth
from features.exchange.exchange_schema import ExchangeCreateOrUpdate
from features.exchange import exchange_service

exchange_router = APIRouter(
    prefix="/exchange",
    tags=["exchanges"],
)


@exchange_router.post("/", status_code=status.HTTP_201_CREATED)
async def create_exchange(
    exchange_data: ExchangeCreateOrUpdate,
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


@exchange_router.get("/")
async def list_exchanges(
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Get all exchanges"""
    return await exchange_service.get_all_exchanges(session)


@exchange_router.get("/{exchange_id}")
async def get_exchange(
    exchange_id: int,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Get exchange by ID"""
    exchange = await exchange_service.get_exchange_by_id(session, exchange_id)
    if not exchange:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Exchange not found"
        )
    return exchange


@exchange_router.get("/code/{code}")
async def get_exchange_by_code(
    code: str,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Get exchange by code"""
    exchange = await exchange_service.get_exchange_by_code(session, code)
    if not exchange:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Exchange not found"
        )
    return exchange


@exchange_router.put("/{exchange_id}")
async def update_exchange(
    exchange_id: int,
    exchange_data: ExchangeCreateOrUpdate,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Update an exchange"""
    exchange = await exchange_service.update_exchange(
        session, exchange_id, exchange_data
    )
    if not exchange:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Exchange not found"
        )
    return exchange


@exchange_router.delete("/{exchange_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_exchange(
    exchange_id: int,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Delete an exchange"""
    deleted = await exchange_service.delete_exchange(session, exchange_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Exchange not found"
        )
    return None
