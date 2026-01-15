from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_db_session
from features.auth.auth_service import require_auth
from features.watchlist.watchlist_schema import (
    WatchlistCreate,
    WatchlistUpdate,
    WatchlistInDb,
    WatchlistItemCreate,
    WatchlistItemInDb,
)
from features.watchlist import watchlist_service

watchlist_router = APIRouter(
    prefix="/watchlist",
    tags=["watchlists"],
)


@watchlist_router.post(
    "/",
    response_model=WatchlistInDb,
    status_code=status.HTTP_201_CREATED,
)
async def create_watchlist(
    watchlist_data: WatchlistCreate,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Create a new watchlist"""
    user_id = int(user_claims.get("id"))
    return await watchlist_service.create_watchlist(session, user_id, watchlist_data)


@watchlist_router.get(
    "/",
    response_model=list[WatchlistInDb],
)
async def list_watchlists(
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """List all watchlists for the current user"""
    user_id = int(user_claims.get("id"))
    return await watchlist_service.get_user_watchlists(session, user_id)


@watchlist_router.get(
    "/dashboard",
    response_model=list[WatchlistInDb],
)
async def get_dashboard_watchlists(
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Get watchlists marked to show on dashboard for the current user"""
    user_id = int(user_claims.get("id"))
    return await watchlist_service.get_dashboard_watchlists(session, user_id)


@watchlist_router.get(
    "/{watchlist_id}",
    response_model=WatchlistInDb,
)
async def get_watchlist(
    watchlist_id: int,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Get a specific watchlist"""
    user_id = int(user_claims.get("id"))
    watchlist = await watchlist_service.get_watchlist_by_id(
        session, watchlist_id, user_id
    )
    if not watchlist:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Watchlist not found",
        )
    return watchlist


@watchlist_router.put(
    "/{watchlist_id}",
    response_model=WatchlistInDb,
)
async def update_watchlist(
    watchlist_id: int,
    watchlist_data: WatchlistUpdate,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Update a watchlist"""
    user_id = int(user_claims.get("id"))
    watchlist = await watchlist_service.update_watchlist(
        session, watchlist_id, user_id, watchlist_data
    )
    if not watchlist:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Watchlist not found",
        )
    return watchlist


@watchlist_router.delete(
    "/{watchlist_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_watchlist(
    watchlist_id: int,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Delete a watchlist"""
    user_id = int(user_claims.get("id"))
    success = await watchlist_service.delete_watchlist(session, watchlist_id, user_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Watchlist not found",
        )
    return None


@watchlist_router.post(
    "/{watchlist_id}/items",
    response_model=WatchlistItemInDb,
    status_code=status.HTTP_201_CREATED,
)
async def add_item_to_watchlist(
    watchlist_id: int,
    item_data: WatchlistItemCreate,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Add an item to a watchlist"""
    user_id = int(user_claims.get("id"))
    item = await watchlist_service.add_item_to_watchlist(
        session, watchlist_id, user_id, item_data
    )
    if not item:
        # Could be watchlist not found or item already exists
        # Check if watchlist exists first
        watchlist = await watchlist_service.get_watchlist_by_id(
            session, watchlist_id, user_id
        )
        if not watchlist:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Watchlist not found",
            )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Item already in watchlist",
        )
    return item


@watchlist_router.delete(
    "/{watchlist_id}/items/{instrument_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def remove_item_from_watchlist(
    watchlist_id: int,
    instrument_id: int,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Remove an item from a watchlist"""
    user_id = int(user_claims.get("id"))
    success = await watchlist_service.remove_item_from_watchlist(
        session, watchlist_id, instrument_id, user_id
    )
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Item not found in watchlist",
        )
    return None


@watchlist_router.put(
    "/show_on_dashboard/{watchlist_id}",
    response_model=WatchlistInDb,
)
async def set_watchlist_show_on_dashboard(
    watchlist_id: int,
    show_on_dashboard: bool,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Set a watchlist to show on dashboard, unsetting others"""
    user_id = int(user_claims.get("id"))
    watchlist = await watchlist_service.set_watchlist_show_on_dashboard(
        session, watchlist_id, user_id, show_on_dashboard
    )
    if not watchlist:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Watchlist not found",
        )
    return watchlist
