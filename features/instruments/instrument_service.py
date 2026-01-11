from sqlalchemy import select, exists, delete
from sqlalchemy.ext.asyncio import AsyncSession
import json
from config.redis_config import get_redis
from config.logger import logger

from features.instruments.instrument_schema import (
    InstrumentCreate,
    InstrumentUpdate,
    InstrumentInDb
)
from models import Instrument, ProviderInstrumentMapping, PriceHistoryIntraday, PriceHistoryDaily


# Instrument CRUD
async def create_instrument(
    session: AsyncSession,
    instrument_data: InstrumentCreate,
) -> InstrumentInDb:
    """Create a new instrument"""
    new_instrument = Instrument(
        symbol=instrument_data.symbol,
        name=instrument_data.name,
        exchange_id=instrument_data.exchange_id,
        instrument_type_id=instrument_data.instrument_type_id,
        sector_id=instrument_data.sector_id,
        blacklisted=instrument_data.blacklisted,
        delisted=instrument_data.delisted,
        should_record_data=False, # Always false on creation, must be enabled explicitly after mapping provider
        is_active=False,  # Initially inactive
    )

    # Avoid session.add() during flush by committing first if needed, or just adding.
    # The warning "Usage of the 'Session.add()' operation is not currently supported within the execution stage of the flush process"
    # usually happens if this function is called from within an event listener or during a flush.
    # If this is a standard API call, session.add() is fine.
    # However, if we are seeing that warning, we might be in a nested context.
    # For a standard CRUD operation, the code below is standard.
    # If the warning persists, check if create_instrument is called from an event listener.

    session.add(new_instrument)
    await session.commit()
    await session.refresh(new_instrument)
    return InstrumentInDb(
        id=new_instrument.id,
        symbol=new_instrument.symbol,
        name=new_instrument.name,
        exchange_id=new_instrument.exchange_id,
        instrument_type_id=new_instrument.instrument_type_id,
        sector_id=new_instrument.sector_id,
        blacklisted=new_instrument.blacklisted,
        delisted=new_instrument.delisted,
        should_record_data=new_instrument.should_record_data,
        is_active=new_instrument.is_active,
    )


async def get_instrument_by_id(
    session: AsyncSession,
    instrument_id: int,
    only_active: bool = True,
) -> InstrumentInDb | None:
    """Get instrument by ID"""
    redis = get_redis()
    cache_key = f"instrument:id:{instrument_id}"

    if redis:
        try:
            cached_data = await redis.get(cache_key)
            if cached_data:
                data = json.loads(cached_data)
                instrument = InstrumentInDb(**data)
                if only_active and not instrument.is_active:
                    return None
                return instrument
        except Exception as e:
            logger.error(f"Error reading instrument from Redis: {e}")

    stmt = select(Instrument).where(Instrument.id == instrument_id)
    # We fetch regardless of active status to cache it, then filter

    result = await session.execute(stmt)
    instrument = result.scalar_one_or_none()

    if instrument:
        instrument_db = InstrumentInDb(
            id=instrument.id,
            symbol=instrument.symbol,
            name=instrument.name,
            exchange_id=instrument.exchange_id,
            instrument_type_id=instrument.instrument_type_id,
            sector_id=instrument.sector_id,
            blacklisted=instrument.blacklisted,
            delisted=instrument.delisted,
            should_record_data=instrument.should_record_data,
            is_active=instrument.is_active,
        )

        if redis:
            try:
                await redis.set(cache_key, instrument_db.model_dump_json(), ex=3600)
                # Also cache by symbol for consistency
                await redis.set(f"instrument:symbol:{instrument.symbol}", instrument_db.model_dump_json(), ex=3600)
            except Exception as e:
                logger.error(f"Error caching instrument to Redis: {e}")

        if only_active and not instrument.is_active:
            return None

        return instrument_db

    return None


async def get_instrument_by_symbol(
    session: AsyncSession,
    symbol: str,
    only_active: bool = True,
) -> InstrumentInDb | None:
    """Get instrument by symbol"""
    redis = get_redis()
    cache_key = f"instrument:symbol:{symbol}"

    if redis:
        try:
            cached_data = await redis.get(cache_key)
            if cached_data:
                data = json.loads(cached_data)
                instrument = InstrumentInDb(**data)
                if only_active and not instrument.is_active:
                    return None
                return instrument
        except Exception as e:
            logger.error(f"Error reading instrument from Redis: {e}")

    stmt = select(Instrument).where(Instrument.symbol == symbol)

    result = await session.execute(stmt)
    instrument = result.scalar_one_or_none()

    if instrument:
        instrument_db = InstrumentInDb(
            id=instrument.id,
            symbol=instrument.symbol,
            name=instrument.name,
            exchange_id=instrument.exchange_id,
            instrument_type_id=instrument.instrument_type_id,
            sector_id=instrument.sector_id,
            blacklisted=instrument.blacklisted,
            delisted=instrument.delisted,
            should_record_data=instrument.should_record_data,
            is_active=instrument.is_active,
        )

        if redis:
            try:
                await redis.set(cache_key, instrument_db.model_dump_json(), ex=3600)
                # Also cache by ID
                await redis.set(f"instrument:id:{instrument.id}", instrument_db.model_dump_json(), ex=3600)
            except Exception as e:
                logger.error(f"Error caching instrument to Redis: {e}")

        if only_active and not instrument.is_active:
            return None

        return instrument_db

    return None


async def get_all_instruments(
    session: AsyncSession,
    only_active: bool = True,
    instrument_type_id: int | None = None,
) -> list[InstrumentInDb]:
    """Get all instruments"""
    stmt = select(Instrument)
    if only_active:
        stmt = stmt.where(Instrument.is_active == True)
        
    result = await session.execute(stmt)
    instruments = result.scalars().all()
    return [
        InstrumentInDb(
            id=i.id,
            symbol=i.symbol,
            name=i.name,
            exchange_id=i.exchange_id,
            instrument_type_id=i.instrument_type_id,
            sector_id=i.sector_id,
            blacklisted=i.blacklisted,
            delisted=i.delisted,
            should_record_data=i.should_record_data,
            is_active=i.is_active,
        )
        for i in instruments
    ]


async def update_instrument(
    session: AsyncSession,
    instrument_id: int,
    instrument_data: InstrumentUpdate,
) -> InstrumentInDb | None:
    """Update an instrument"""
    result = await session.execute(
        select(Instrument).where(Instrument.id == instrument_id)
    )
    instrument = result.scalar_one_or_none()
    if not instrument:
        return None

    update_data = instrument_data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(instrument, key, value)

    await session.commit()
    await session.refresh(instrument)

    # Invalidate cache
    redis = get_redis()
    if redis:
        try:
            await redis.delete(f"instrument:id:{instrument.id}")
            await redis.delete(f"instrument:symbol:{instrument.symbol}")
        except Exception as e:
            logger.error(f"Error invalidating instrument cache: {e}")

    return InstrumentInDb(
        id=instrument.id,
        symbol=instrument.symbol,
        name=instrument.name,
        exchange_id=instrument.exchange_id,
        instrument_type_id=instrument.instrument_type_id,
        sector_id=instrument.sector_id,
        blacklisted=instrument.blacklisted,
        delisted=instrument.delisted,
        is_active=instrument.is_active,
        should_record_data=bool(instrument.should_record_data),
    )


async def delete_instrument(
    session: AsyncSession,
    instrument_id: int,
) -> bool:
    """Delete an instrument"""
    result = await session.execute(
        select(Instrument).where(Instrument.id == instrument_id)
    )
    instrument = result.scalar_one_or_none()
    if not instrument:
        return False

    # Capture symbol before delete for cache invalidation
    symbol = instrument.symbol

    await session.delete(instrument)
    await session.commit()

    # Invalidate cache
    redis = get_redis()
    if redis:
        try:
            await redis.delete(f"instrument:id:{instrument_id}")
            await redis.delete(f"instrument:symbol:{symbol}")
        except Exception as e:
            logger.error(f"Error invalidating instrument cache: {e}")

    return True


async def search_instruments(
    session: AsyncSession,
    query: str,
    only_active: bool = True,
) -> list[InstrumentInDb]:
    """Search instruments by symbol or name"""
    stmt = select(Instrument).where(
        (Instrument.symbol.ilike(f"%{query}%")) | 
        (Instrument.name.ilike(f"%{query}%"))
    )
    if only_active:
        stmt = stmt.where(Instrument.is_active == True)
        
    result = await session.execute(stmt)
    instruments = result.scalars().all()
    return [
        InstrumentInDb(
            id=i.id,
            symbol=i.symbol,
            name=i.name,
            exchange_id=i.exchange_id,
            instrument_type_id=i.instrument_type_id,
            sector_id=i.sector_id,
            blacklisted=i.blacklisted,
            delisted=i.delisted,
            is_active=i.is_active,
            should_record_data=i.should_record_data,
        )
        for i in instruments
    ]


async def toggle_instrument_recording(
    session: AsyncSession,
    instrument_id: int,
    should_record: bool,
) -> InstrumentInDb | None:
    """
    Enable or disable data recording for an instrument.
    If enabling, checks if a provider mapping exists.
    """
    stmt = select(Instrument).where(Instrument.id == instrument_id)
    result = await session.execute(stmt)
    instrument : Instrument = result.scalar_one_or_none()

    if not instrument:
        return None

    if should_record:
        # Check if provider mapping exists
        mapping_stmt = select(exists().where(ProviderInstrumentMapping.instrument_id == instrument_id))
        mapping_exists = await session.execute(mapping_stmt)
        if not mapping_exists.scalar():
            raise ValueError("Cannot enable recording: No provider mapping found for this instrument.")
    else:
        # Delete existing data if disabling recording
        await session.execute(
            delete(PriceHistoryIntraday).where(PriceHistoryIntraday.instrument_id == instrument_id)
        )
        await session.execute(
            delete(PriceHistoryDaily).where(PriceHistoryDaily.instrument_id == instrument_id)
        )

    instrument.should_record_data = should_record
    session.add(instrument)
    await session.commit()
    await session.refresh(instrument)

    # Invalidate cache
    redis = get_redis()
    if redis:
        try:
            await redis.delete(f"instrument:id:{instrument.id}")
            await redis.delete(f"instrument:symbol:{instrument.symbol}")
        except Exception as e:
            logger.error(f"Error invalidating instrument cache: {e}")

    return InstrumentInDb(
        id=instrument.id,
        symbol=instrument.symbol,
        name=instrument.name,
        exchange_id=instrument.exchange_id,
        instrument_type_id=instrument.instrument_type_id,
        sector_id=instrument.sector_id,
        blacklisted=instrument.blacklisted,
        delisted=instrument.delisted,
        should_record_data=bool(instrument.should_record_data),
        is_active=instrument.is_active,
    )


async def get_recording_instruments(
    session: AsyncSession,
) -> list[InstrumentInDb]:
    """Get all instruments that have data recording enabled"""
    stmt = select(Instrument).where(Instrument.should_record_data == True)
    result = await session.execute(stmt)
    instruments = result.scalars().all()
    return [
        InstrumentInDb(
            id=i.id,
            symbol=i.symbol,
            name=i.name,
            exchange_id=i.exchange_id,
            instrument_type_id=i.instrument_type_id,
            sector_id=i.sector_id,
            blacklisted=i.blacklisted,
            delisted=i.delisted,
            should_record_data=i.should_record_data,
            is_active=i.is_active,
        )
        for i in instruments
    ]
