import asyncio

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from features.instruments import instrument_service
from features.instruments.instrument_schema import InstrumentInDb
from models.watchlist import Watchlist, WatchlistItem
from features.watchlist.watchlist_schema import (
    WatchlistCreate,
    WatchlistUpdate,
    WatchlistInDb,
    WatchlistItemCreate,
    WatchlistItemInDb,
)


async def create_watchlist(
    session: AsyncSession,
    user_id: int,
    watchlist_data: WatchlistCreate,
) -> WatchlistInDb:
    """Create a new watchlist for a user"""
    new_watchlist = Watchlist(
        user_id=user_id,
        name=watchlist_data.name,
        show_on_dashboard=watchlist_data.show_on_dashboard,
    )
    session.add(new_watchlist)
    await session.commit()
    await session.refresh(new_watchlist)

    return WatchlistInDb(
        id=new_watchlist.id,
        user_id=new_watchlist.user_id,
        name=new_watchlist.name,
        show_on_dashboard=new_watchlist.show_on_dashboard,
        items=[],
    )


async def get_user_watchlists(
    session: AsyncSession,
    user_id: int,
) -> list[WatchlistInDb]:
    """Get all watchlists for a user"""
    result = await session.execute(
        select(Watchlist)
        .where(Watchlist.user_id == user_id)
        .options(selectinload(Watchlist.items))
    )
    watchlists = result.scalars().all()

    return [
        WatchlistInDb(
            id=wl.id,
            user_id=wl.user_id,
            name=wl.name,
            show_on_dashboard=wl.show_on_dashboard,
            items=await get_instruments_from_items(session, wl.items),
        )
        for wl in watchlists
    ]


async def get_watchlist_by_id(
    session: AsyncSession,
    watchlist_id: int,
    user_id: int,
    with_instruments: bool = True,
) -> WatchlistInDb | None:
    """Get a specific watchlist by ID"""
    result = await session.execute(
        select(Watchlist)
        .where((Watchlist.id == watchlist_id) & (Watchlist.user_id == user_id))
        .options(selectinload(Watchlist.items))
    )
    watchlist = result.scalar_one_or_none()

    if not watchlist:
        return None

    instruments = []
    if with_instruments:
        instruments = await get_instruments_from_items(session, watchlist.items)

    return WatchlistInDb(
        id=watchlist.id,
        user_id=watchlist.user_id,
        name=watchlist.name,
        show_on_dashboard=watchlist.show_on_dashboard,
        items=instruments,
    )


async def update_watchlist(
    session: AsyncSession,
    watchlist_id: int,
    user_id: int,
    watchlist_data: WatchlistUpdate,
) -> WatchlistInDb | None:
    """Update a watchlist"""
    result = await session.execute(
        select(Watchlist)
        .where((Watchlist.id == watchlist_id) & (Watchlist.user_id == user_id))
        .options(selectinload(Watchlist.items))
    )
    watchlist = result.scalar_one_or_none()

    if not watchlist:
        return None

    if watchlist_data.name is not None:
        watchlist.name = watchlist_data.name

    await session.commit()
    await session.refresh(watchlist)

    return WatchlistInDb(
        id=watchlist.id,
        user_id=watchlist.user_id,
        name=watchlist.name,
        show_on_dashboard=True,
        items=await get_instruments_from_items(session, watchlist.items),
    )


async def delete_watchlist(
    session: AsyncSession,
    watchlist_id: int,
    user_id: int,
) -> bool:
    """Delete a watchlist"""
    result = await session.execute(
        select(Watchlist).where(
            (Watchlist.id == watchlist_id) & (Watchlist.user_id == user_id)
        )
    )
    watchlist = result.scalar_one_or_none()

    if not watchlist:
        return False

    await session.delete(watchlist)
    await session.commit()
    return True


async def add_item_to_watchlist(
    session: AsyncSession,
    watchlist_id: int,
    user_id: int,
    item_data: WatchlistItemCreate,
) -> WatchlistItemInDb | None:
    """Add an item to a watchlist"""
    # Verify watchlist ownership
    result = await session.execute(
        select(Watchlist).where(
            (Watchlist.id == watchlist_id) & (Watchlist.user_id == user_id)
        )
    )
    watchlist = result.scalar_one_or_none()

    if not watchlist:
        return None

    # Check if item already exists
    existing = await session.execute(
        select(WatchlistItem).where(
            (WatchlistItem.watchlist_id == watchlist_id)
            & (WatchlistItem.instrument_id == item_data.instrument_id)
        )
    )
    if existing.scalar_one_or_none():
        # Already exists, return it or raise error?
        # Let's return existing one to be idempotent-ish or handle in route
        return None

    new_item = WatchlistItem(
        watchlist_id=watchlist_id,
        instrument_id=item_data.instrument_id,
    )
    session.add(new_item)
    await session.commit()
    await session.refresh(new_item)

    return WatchlistItemInDb(
        id=new_item.id,
        watchlist_id=new_item.watchlist_id,
        instrument_id=new_item.instrument_id,
    )


async def remove_item_from_watchlist(
    session: AsyncSession,
    watchlist_id: int,
    instrument_id: int,
    user_id: int,
) -> bool:
    """Remove an item from a watchlist"""
    # Verify watchlist ownership
    result = await session.execute(
        select(Watchlist).where(
            (Watchlist.id == watchlist_id) & (Watchlist.user_id == user_id)
        )
    )
    watchlist = result.scalar_one_or_none()

    if not watchlist:
        return False

    # Find item
    item_result = await session.execute(
        select(WatchlistItem).where(
            (WatchlistItem.watchlist_id == watchlist_id)
            & (WatchlistItem.instrument_id == instrument_id)
        )
    )
    item = item_result.scalar_one_or_none()

    if not item:
        return False

    await session.delete(item)
    await session.commit()
    return True


async def set_watchlist_show_on_dashboard(
    session: AsyncSession, watchlist_id: int, user_id: int, show_on_dashboard: bool
):
    result = await session.execute(
        select(Watchlist)
        .where((Watchlist.id == watchlist_id) & (Watchlist.user_id == user_id))
        .options(selectinload(Watchlist.items))
    )
    watchlist = result.scalar_one_or_none()

    if not watchlist:
        return None

    watchlist.show_on_dashboard = show_on_dashboard
    await session.commit()
    await session.refresh(watchlist)

    instruments = await get_instruments_from_items(session, watchlist.items)

    return WatchlistInDb(
        id=watchlist.id,
        user_id=watchlist.user_id,
        name=watchlist.name,
        show_on_dashboard=watchlist.show_on_dashboard,
        items=instruments,
    )


async def get_dashboard_watchlists(
    session: AsyncSession,
    user_id: int,
    with_instruments: bool = True,
) -> list[WatchlistInDb]:
    result = await session.execute(
        select(Watchlist).where(
            (Watchlist.user_id == user_id) & (Watchlist.show_on_dashboard == True)
        )
    )
    watchlists = result.scalars().all()

    watchlist_result = []
    for wl in watchlists:
        wl_data = await get_watchlist_by_id(
            session=session,
            watchlist_id=wl.id,
            user_id=user_id,
            with_instruments=with_instruments,
        )
        if wl_data:
            watchlist_result.append(wl_data)

    return watchlist_result


async def get_instruments_from_items(
    session: AsyncSession,
    items: list[WatchlistItem],
) -> list[InstrumentInDb]:
    """Fetch instruments for given watchlist items concurrently"""
    tasks = [
        instrument_service.get_instrument_by_id(session, item.instrument_id)
        for item in items
    ]
    instruments = await asyncio.gather(*tasks)
    return [inst for inst in instruments if inst is not None]
