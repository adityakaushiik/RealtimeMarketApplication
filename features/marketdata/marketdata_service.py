import datetime
from typing import Optional
import json

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from config.redis_config import get_redis
from config.logger import logger
from features.exchange.exchange_service import get_exchange_by_code
from features.instruments.instrument_schema import InstrumentInDb
from features.marketdata.marketdata_schema import (
    PriceHistoryIntradayCreate,
    PriceHistoryIntradayUpdate,
    PriceHistoryIntradayInDb,
    PriceHistoryDailyCreate,
    PriceHistoryDailyUpdate,
    PriceHistoryDailyInDb,
    InstrumentPreviousClose,
)
from models import PriceHistoryIntraday, PriceHistoryDaily, Instrument
from services.data.data_ingestion import get_provider_manager
from services.redis_timeseries import get_redis_timeseries


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
        resolve_required=data.resolve_required,
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
        resolve_required=new_record.resolve_required,
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
        resolve_required=record.resolve_required,
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
        resolve_required=data.resolve_required,
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
        resolve_required=new_record.resolve_required,
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
        resolve_required=record.resolve_required,
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
    interval: Optional[str] = None,
    timeframe_in_days: int = 7,
    start_date: Optional[int] = None,
    end_date: Optional[int] = None,
) -> list[PriceHistoryIntradayInDb]:
    """Get intraday price history for a given instrument"""
    current_time_utc = datetime.datetime.now(datetime.UTC)

    # Build the query
    stmt = select(PriceHistoryIntraday).where(
        (PriceHistoryIntraday.instrument_id == instrument_id)
        & (PriceHistoryIntraday.datetime <= current_time_utc)
    )

    # Only filter by interval if it's provided and not None/Empty
    if interval:
        stmt = stmt.where(PriceHistoryIntraday.interval == interval)

    stmt = stmt.order_by(PriceHistoryIntraday.datetime.desc())

    result = await session.execute(stmt)
    records = result.scalars().all()

    # Convert to Pydantic models
    result_list = [
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
            resolve_required=r.resolve_required,
            interval=r.interval,
        )
        for r in records
    ]

    # Merge with ongoing candle from Redis
    instrument = await session.get(Instrument, instrument_id)
    if instrument:
        rts = get_redis_timeseries()
        # Get latest 5m candle (ongoing)
        latest_candle = await rts.get_ohlcv_last_5m(instrument.symbol)

        if latest_candle:
            ts = latest_candle['ts']
            dt = datetime.datetime.fromtimestamp(ts / 1000, tz=datetime.timezone.utc)

            if not result_list or result_list[0].datetime < dt:
                new_rec = PriceHistoryIntradayInDb(
                    id=-1,
                    instrument_id=instrument_id,
                    datetime=dt,
                    open=latest_candle['open'],
                    high=latest_candle['high'],
                    low=latest_candle['low'],
                    close=latest_candle['close'],
                    volume=int(latest_candle['volume']),
                    resolve_required=False,
                    interval=interval or "5m",
                    previous_close=None,
                    adj_close=None,
                    deliver_percentage=None
                )
                result_list.insert(0, new_rec)
            elif result_list and result_list[0].datetime == dt:
                # Update existing record with fresher data from Redis
                rec = result_list[0]
                rec.open = latest_candle['open']
                rec.high = latest_candle['high']
                rec.low = latest_candle['low']
                rec.close = latest_candle['close']
                # For volume, we should use the Redis volume as it is the most up-to-date for the current bucket
                # The DB record might be lagging or incomplete if we are in the middle of the bucket
                rec.volume = int(latest_candle['volume'])

    return result_list


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
            (PriceHistoryDaily.instrument_id == instrument_id) &
            (PriceHistoryDaily.datetime <= current_time_utc)
        )
        .order_by(PriceHistoryDaily.datetime.desc())
    )

    records = result.scalars().all()

    # Convert to Pydantic models
    result_list = [
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
            resolve_required=r.resolve_required,
        )
        for r in records
    ]

    # Check if the latest record (today) needs resolution
    if result_list and result_list[0].resolve_required:
        await _aggregate_intraday_for_daily_record(session, instrument_id, result_list[0])

    # Merge with ongoing candle from Redis
    instrument = await session.get(Instrument, instrument_id)
    if instrument and result_list:
        rts = get_redis_timeseries()
        latest_candle = await rts.get_ohlcv_last_5m(instrument.symbol)

        if latest_candle:
            # We assume the first record is for the current day
            today_rec = result_list[0]

            # Update High/Low/Close
            today_rec.close = latest_candle['close']

            if today_rec.high is None or latest_candle['high'] > today_rec.high:
                today_rec.high = latest_candle['high']

            if today_rec.low is None or latest_candle['low'] < today_rec.low:
                today_rec.low = latest_candle['low']

            if today_rec.open is None:
                today_rec.open = latest_candle['open']

            # Add volume from the ongoing candle
            today_rec.volume = (today_rec.volume or 0) + int(latest_candle['volume'])

    return result_list


async def _aggregate_intraday_for_daily_record(
    session: AsyncSession,
    instrument_id: int,
    daily_record: PriceHistoryDailyInDb
):
    """
    Helper to aggregate intraday data into a daily record.
    Modifies daily_record in-place.
    """
    # Fetch intraday data for this day to aggregate
    # We assume the daily record datetime is the start of the day
    day_start = daily_record.datetime
    day_end = day_start + datetime.timedelta(days=1)

    stmt_agg = select(
        func.min(PriceHistoryIntraday.low).label("low"),
        func.max(PriceHistoryIntraday.high).label("high"),
        func.sum(PriceHistoryIntraday.volume).label("volume")
    ).where(
        PriceHistoryIntraday.instrument_id == instrument_id,
        PriceHistoryIntraday.datetime >= day_start,
        PriceHistoryIntraday.datetime < day_end
    )

    agg_result = await session.execute(stmt_agg)
    agg_row = agg_result.one_or_none()

    if agg_row and agg_row.low is not None:
        # Get Open (earliest)
        stmt_open = select(PriceHistoryIntraday.open).where(
            PriceHistoryIntraday.instrument_id == instrument_id,
            PriceHistoryIntraday.datetime >= day_start,
            PriceHistoryIntraday.datetime < day_end,
            PriceHistoryIntraday.open.is_not(None)
        ).order_by(PriceHistoryIntraday.datetime.asc()).limit(1)
        open_val = (await session.execute(stmt_open)).scalar_one_or_none()

        # Get Close (latest)
        stmt_close = select(PriceHistoryIntraday.close).where(
            PriceHistoryIntraday.instrument_id == instrument_id,
            PriceHistoryIntraday.datetime >= day_start,
            PriceHistoryIntraday.datetime < day_end,
            PriceHistoryIntraday.close.is_not(None)
        ).order_by(PriceHistoryIntraday.datetime.desc()).limit(1)
        close_val = (await session.execute(stmt_close)).scalar_one_or_none()

        # Update the Pydantic model instance
        daily_record.open = open_val
        daily_record.high = agg_row.high
        daily_record.low = agg_row.low
        daily_record.close = close_val
        daily_record.volume = int(agg_row.volume) if agg_row.volume is not None else None


async def get_previous_closes_by_exchange(session: AsyncSession, exchange_code: str):
    """Get previous close prices for all instruments in a given exchange"""

    # Try to get from Redis cache first
    redis = get_redis()
    cache_key = f"prev_close:{exchange_code}"

    if redis:
        try:
            cached_data = await redis.get(cache_key)
            if cached_data:
                data = json.loads(cached_data)
                return [InstrumentPreviousClose(**item) for item in data]
        except Exception as e:
            logger.error(f"Error reading prev_close from Redis: {e}")

    exchange = await get_exchange_by_code(session, exchange_code)
    if not exchange:
        return []

    response = await _fetch_previous_closes_from_db(session, exchange.id)

    # Cache the result in Redis
    if redis and response:
        try:
            # Cache for 1 hour (or until next update)
            # Serialize with Pydantic's model_dump_json or similar, but list of models needs manual handling
            json_data = json.dumps([item.model_dump(mode='json') for item in response])
            await redis.set(cache_key, json_data, ex=3600)
        except Exception as e:
            logger.error(f"Error caching prev_close to Redis: {e}")

    return response


async def _fetch_previous_closes_from_db(session: AsyncSession, exchange_id: int) -> list[InstrumentPreviousClose]:
    # Calculate start of today in UTC to ensure we get strictly previous day's data
    today_start = datetime.datetime.now(datetime.timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # Use DISTINCT ON to get the latest record for each instrument efficiently
    # This is PostgreSQL specific but highly efficient
    stmt = (
        select(
            Instrument.symbol,
            PriceHistoryDaily.close,
            PriceHistoryDaily.datetime
        )
        .join(Instrument, PriceHistoryDaily.instrument_id == Instrument.id)
        .where(
            Instrument.exchange_id == exchange_id,
            PriceHistoryDaily.datetime < today_start
        )
        .distinct(Instrument.id)
        .order_by(Instrument.id, PriceHistoryDaily.datetime.desc())
    )

    result = await session.execute(stmt)
    records = result.all()

    return [
        InstrumentPreviousClose(
            symbol=r.symbol,
            price=r.close,
            timestamp=r.datetime
        )
        for r in records
    ]


async def get_combined_daily_price_history(
    session: AsyncSession,
    instrument: InstrumentInDb
) -> list[PriceHistoryDailyInDb]:
    if instrument.should_record_data:
        return await get_price_history_daily(session, instrument.id)

    provider_manager = get_provider_manager()
    if not provider_manager:
        logger.error("Provider manager not initialized")
        return []

    data = await provider_manager.get_daily_prices(instrument)

    # Filter data to be up to current time
    current_time_utc = datetime.datetime.now(datetime.timezone.utc)

    return [
        PriceHistoryDailyInDb(
            id=0,
            instrument_id=instrument.id,
            datetime=d.datetime,
            open=d.open,
            high=d.high,
            low=d.low,
            close=d.close,
            previous_close=d.previous_close,
            adj_close=d.adj_close,
            volume=d.volume,
            deliver_percentage=d.deliver_percentage,
            resolve_required=d.resolve_required,
        ) for d in data if d.datetime <= current_time_utc
    ]


async def get_combined_intraday_price_history(
    session: AsyncSession,
    instrument: InstrumentInDb
) -> list[PriceHistoryIntradayInDb]:
    if instrument.should_record_data:
        return await get_price_history_intraday(session, instrument.id)

    provider_manager = get_provider_manager()
    if not provider_manager:
        logger.error("Provider manager not initialized")
        return []

    data = await provider_manager.get_intraday_prices(instrument)

    # Filter data to be up to current time
    current_time_utc = datetime.datetime.now(datetime.timezone.utc)

    return [
        PriceHistoryIntradayInDb(
            id=0,
            instrument_id=instrument.id,
            datetime=d.datetime,
            open=d.open,
            high=d.high,
            low=d.low,
            close=d.close,
            previous_close=d.previous_close,
            adj_close=d.adj_close,
            volume=d.volume,
            deliver_percentage=d.deliver_percentage,
            resolve_required=d.resolve_required,
            interval=d.interval if hasattr(d, 'interval') else "5m",
        ) for d in data if d.datetime <= current_time_utc
    ]
