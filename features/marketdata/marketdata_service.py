import datetime
from typing import Optional

from anyio import current_time
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from features.exchange.exchange_service import get_exchange_by_code
from features.marketdata.marketdata_schema import (
    PriceHistoryIntradayCreate,
    PriceHistoryIntradayUpdate,
    PriceHistoryIntradayInDb,
    PriceHistoryDailyCreate,
    PriceHistoryDailyUpdate,
    PriceHistoryDailyInDb,
)
from models import PriceHistoryIntraday, PriceHistoryDaily, Instrument


# PriceHistoryIntraday CRUD
async def create_price_history_intraday(
    session: AsyncSession,
    data: PriceHistoryIntradayCreate,
) -> PriceHistoryIntradayInDb:
    """Create a new intraday price history"""
    new_record = PriceHistoryIntraday(
        instrument_id=data.instrument_id,
        datetime=data.datetime,
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
        datetime=new_record.datetime,
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


async def update_price_history_intraday(
    session: AsyncSession,
    record_id: int,
    data: PriceHistoryIntradayUpdate,
) -> PriceHistoryIntradayInDb | None:
    """Update an intraday price history"""
    result = await session.execute(
        select(PriceHistoryIntraday).where(PriceHistoryIntraday.id == record_id)
    )
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
        datetime=record.datetime,
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
    result = await session.execute(
        select(PriceHistoryIntraday).where(PriceHistoryIntraday.id == record_id)
    )
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
        datetime=data.datetime,
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
        datetime=new_record.datetime,
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


async def update_price_history_daily(
    session: AsyncSession,
    record_id: int,
    data: PriceHistoryDailyUpdate,
) -> PriceHistoryDailyInDb | None:
    """Update a daily price history"""
    result = await session.execute(
        select(PriceHistoryDaily).where(PriceHistoryDaily.id == record_id)
    )
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
        datetime=record.datetime,
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
    result = await session.execute(
        select(PriceHistoryDaily).where(PriceHistoryDaily.id == record_id)
    )
    record = result.scalar_one_or_none()
    if not record:
        return False

    await session.delete(record)
    await session.commit()
    return True


async def get_previous_close_price(
    session: AsyncSession,
    instrument_id: int,
    date: datetime.datetime,
) -> float | None:
    """Get the previous close price for a given instrument and date"""
    result = await session.execute(
        select(PriceHistoryDaily)
        .where(
            (PriceHistoryDaily.instrument_id == instrument_id)
            & (PriceHistoryDaily.datetime < date)
        )
        .order_by(PriceHistoryDaily.datetime.desc())
        .limit(1)
    )
    record = result.scalar_one_or_none()
    if record:
        return record.close
    return None


async def get_price_history_intraday(
    session: AsyncSession,
    instrument_id: int,
    interval: str = "5m",
    timeframe_in_days: int = 7,
    start_date: Optional[int] = None,
    end_date: Optional[int] = None,
) -> list[PriceHistoryIntradayInDb]:
    """Get intraday price history for a given instrument"""
    current_time_utc = datetime.datetime.now(datetime.UTC)
    result = await session.execute(
        select(PriceHistoryIntraday)
        .where(
            (PriceHistoryIntraday.instrument_id == instrument_id)
            &
            # (PriceHistoryIntraday.interval == interval) &
            (PriceHistoryIntraday.datetime <= current_time_utc)
        )
        .order_by(PriceHistoryIntraday.datetime.desc())
    )
    records = result.scalars().all()

    return [
        PriceHistoryIntradayInDb(
            id=r.id,
            instrument_id=r.instrument_id,
            datetime=r.datetime,
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


async def get_price_history_daily(
    session: AsyncSession,
    instrument_id: int,
    start_date: Optional[int] = None,
    end_date: Optional[int] = None,
) -> list[PriceHistoryDailyInDb]:
    """Get daily price history for a given instrument"""
    current_time_utc = datetime.datetime.now(datetime.UTC)

    result = await session.execute(
        select(PriceHistoryDaily)
        .where(
            PriceHistoryDaily.instrument_id == instrument_id
            and PriceHistoryDaily.datetime <= current_time_utc
        )
        .order_by(PriceHistoryDaily.datetime.desc())
    )

    records = result.scalars().all()
    return [
        PriceHistoryDailyInDb(
            id=r.id,
            instrument_id=r.instrument_id,
            datetime=r.datetime,
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


async def get_previous_closes_by_exchange(session: AsyncSession, exchange_code: str):
    """Get previous close prices for all instruments in a given exchange"""
    exchange = await get_exchange_by_code(session, exchange_code)
    if not exchange:
        return {}

    result = await session.execute(
        select(PriceHistoryDaily, Instrument)
        .join(Instrument, PriceHistoryDaily.instrument_id == Instrument.id)
        .where(
            Instrument.exchange_id == exchange.id
            and PriceHistoryDaily.datetime < datetime.datetime.now()
        )
        .order_by(PriceHistoryDaily.instrument_id, PriceHistoryDaily.datetime.desc())
    )
    records = result.all()

    result = []
    seen_instruments = set()
    for price_history, instrument in records:
        if instrument.symbol not in seen_instruments:
            result.append({"instrument": instrument, "price_history": price_history})
            seen_instruments.add(instrument.symbol)

    return result
