from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import PriceHistoryIntraday, PriceHistoryDaily
from features.marketdata.marketdata_schema import (
    PriceHistoryIntradayCreate,
    PriceHistoryIntradayUpdate,
    PriceHistoryIntradayInDb,
    PriceHistoryDailyCreate,
    PriceHistoryDailyUpdate,
    PriceHistoryDailyInDb,
)


# PriceHistoryIntraday CRUD

async def create_price_history_intraday(
    session: AsyncSession,
    data: PriceHistoryIntradayCreate,
) -> PriceHistoryIntradayInDb:
    """Create a new intraday price history"""
    new_record = PriceHistoryIntraday(
        instrument_id=data.instrument_id,
        timestamp=data.timestamp,
        open=data.open,
        high=data.high,
        low=data.low,
        close=data.close,
        previous_close=data.previous_close,
        adj_close=data.adj_close,
        volume=data.volume,
        deliver_percentage=data.deliver_percentage,
        price_not_found=data.price_not_found,
        interval=data.interval,
    )
    session.add(new_record)
    await session.commit()
    await session.refresh(new_record)
    return PriceHistoryIntradayInDb(
        id=new_record.id,
        instrument_id=new_record.instrument_id,
        timestamp=new_record.timestamp,
        open=new_record.open,
        high=new_record.high,
        low=new_record.low,
        close=new_record.close,
        previous_close=new_record.previous_close,
        adj_close=new_record.adj_close,
        volume=new_record.volume,
        deliver_percentage=new_record.deliver_percentage,
        price_not_found=new_record.price_not_found,
        interval=new_record.interval,
    )


async def get_price_history_intraday_by_id(
    session: AsyncSession,
    record_id: int,
) -> PriceHistoryIntradayInDb | None:
    """Get intraday price history by ID"""
    result = await session.execute(select(PriceHistoryIntraday).where(PriceHistoryIntraday.id == record_id))
    record = result.scalar_one_or_none()
    if record:
        return PriceHistoryIntradayInDb(
            id=record.id,
            instrument_id=record.instrument_id,
            timestamp=record.timestamp,
            open=record.open,
            high=record.high,
            low=record.low,
            close=record.close,
            previous_close=record.previous_close,
            adj_close=record.adj_close,
            volume=record.volume,
            deliver_percentage=record.deliver_percentage,
            price_not_found=record.price_not_found,
            interval=record.interval,
        )
    return None


async def get_all_price_history_intraday(
    session: AsyncSession,
) -> list[PriceHistoryIntradayInDb]:
    """Get all intraday price histories"""
    result = await session.execute(select(PriceHistoryIntraday))
    records = result.scalars().all()
    return [
        PriceHistoryIntradayInDb(
            id=r.id,
            instrument_id=r.instrument_id,
            timestamp=r.timestamp,
            open=r.open,
            high=r.high,
            low=r.low,
            close=r.close,
            previous_close=r.previous_close,
            adj_close=r.adj_close,
            volume=r.volume,
            deliver_percentage=r.deliver_percentage,
            price_not_found=r.price_not_found,
            interval=r.interval,
        )
        for r in records
    ]


async def update_price_history_intraday(
    session: AsyncSession,
    record_id: int,
    data: PriceHistoryIntradayUpdate,
) -> PriceHistoryIntradayInDb | None:
    """Update an intraday price history"""
    result = await session.execute(select(PriceHistoryIntraday).where(PriceHistoryIntraday.id == record_id))
    record = result.scalar_one_or_none()
    if not record:
        return None

    update_data = data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(record, key, value)

    await session.commit()
    await session.refresh(record)
    return PriceHistoryIntradayInDb(
        id=record.id,
        instrument_id=record.instrument_id,
        timestamp=record.timestamp,
        open=record.open,
        high=record.high,
        low=record.low,
        close=record.close,
        previous_close=record.previous_close,
        adj_close=record.adj_close,
        volume=record.volume,
        deliver_percentage=record.deliver_percentage,
        price_not_found=record.price_not_found,
        interval=record.interval,
    )


async def delete_price_history_intraday(
    session: AsyncSession,
    record_id: int,
) -> bool:
    """Delete an intraday price history"""
    result = await session.execute(select(PriceHistoryIntraday).where(PriceHistoryIntraday.id == record_id))
    record = result.scalar_one_or_none()
    if not record:
        return False

    await session.delete(record)
    await session.commit()
    return True


# PriceHistoryDaily CRUD

async def create_price_history_daily(
    session: AsyncSession,
    data: PriceHistoryDailyCreate,
) -> PriceHistoryDailyInDb:
    """Create a new daily price history"""
    new_record = PriceHistoryDaily(
        instrument_id=data.instrument_id,
        timestamp=data.timestamp,
        open=data.open,
        high=data.high,
        low=data.low,
        close=data.close,
        previous_close=data.previous_close,
        adj_close=data.adj_close,
        volume=data.volume,
        deliver_percentage=data.deliver_percentage,
        price_not_found=data.price_not_found,
    )
    session.add(new_record)
    await session.commit()
    await session.refresh(new_record)
    return PriceHistoryDailyInDb(
        id=new_record.id,
        instrument_id=new_record.instrument_id,
        timestamp=new_record.timestamp,
        open=new_record.open,
        high=new_record.high,
        low=new_record.low,
        close=new_record.close,
        previous_close=new_record.previous_close,
        adj_close=new_record.adj_close,
        volume=new_record.volume,
        deliver_percentage=new_record.deliver_percentage,
        price_not_found=new_record.price_not_found,
    )


async def get_price_history_daily_by_id(
    session: AsyncSession,
    record_id: int,
) -> PriceHistoryDailyInDb | None:
    """Get daily price history by ID"""
    result = await session.execute(select(PriceHistoryDaily).where(PriceHistoryDaily.id == record_id))
    record = result.scalar_one_or_none()
    if record:
        return PriceHistoryDailyInDb(
            id=record.id,
            instrument_id=record.instrument_id,
            timestamp=record.timestamp,
            open=record.open,
            high=record.high,
            low=record.low,
            close=record.close,
            previous_close=record.previous_close,
            adj_close=record.adj_close,
            volume=record.volume,
            deliver_percentage=record.deliver_percentage,
            price_not_found=record.price_not_found,
        )
    return None


async def get_all_price_history_daily(
    session: AsyncSession,
) -> list[PriceHistoryDailyInDb]:
    """Get all daily price histories"""
    result = await session.execute(select(PriceHistoryDaily))
    records = result.scalars().all()
    return [
        PriceHistoryDailyInDb(
            id=r.id,
            instrument_id=r.instrument_id,
            timestamp=r.timestamp,
            open=r.open,
            high=r.high,
            low=r.low,
            close=r.close,
            previous_close=r.previous_close,
            adj_close=r.adj_close,
            volume=r.volume,
            deliver_percentage=r.deliver_percentage,
            price_not_found=r.price_not_found,
        )
        for r in records
    ]


async def update_price_history_daily(
    session: AsyncSession,
    record_id: int,
    data: PriceHistoryDailyUpdate,
) -> PriceHistoryDailyInDb | None:
    """Update a daily price history"""
    result = await session.execute(select(PriceHistoryDaily).where(PriceHistoryDaily.id == record_id))
    record = result.scalar_one_or_none()
    if not record:
        return None

    update_data = data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(record, key, value)

    await session.commit()
    await session.refresh(record)
    return PriceHistoryDailyInDb(
        id=record.id,
        instrument_id=record.instrument_id,
        timestamp=record.timestamp,
        open=record.open,
        high=record.high,
        low=record.low,
        close=record.close,
        previous_close=record.previous_close,
        adj_close=record.adj_close,
        volume=record.volume,
        deliver_percentage=record.deliver_percentage,
        price_not_found=record.price_not_found,
    )


async def delete_price_history_daily(
    session: AsyncSession,
    record_id: int,
) -> bool:
    """Delete a daily price history"""
    result = await session.execute(select(PriceHistoryDaily).where(PriceHistoryDaily.id == record_id))
    record = result.scalar_one_or_none()
    if not record:
        return False

    await session.delete(record)
    await session.commit()
    return True


async def get_previous_close_price(
    session: AsyncSession,
    instrument_id: int,
    date: int,
) -> float | None:
    """Get the previous close price for a given instrument and date"""
    result = await session.execute(
        select(PriceHistoryDaily)
        .where(
            (PriceHistoryDaily.instrument_id == instrument_id) &
            (PriceHistoryDaily.timestamp < date)
        )
        .order_by(PriceHistoryDaily.timestamp.desc())
        .limit(1)
    )
    record = result.scalar_one_or_none()
    if record:
        return record.close
    return None