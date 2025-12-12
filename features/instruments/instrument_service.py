from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from features.instruments.instrument_schema import (
    InstrumentCreate,
    InstrumentUpdate,
    InstrumentInDb
)
from models import Instrument, InstrumentType


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
    )
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
    )


async def get_instrument_by_id(
    session: AsyncSession,
    instrument_id: int,
) -> InstrumentInDb | None:
    """Get instrument by ID"""
    result = await session.execute(
        select(Instrument).where(Instrument.id == instrument_id)
    )
    instrument = result.scalar_one_or_none()
    if instrument:
        return InstrumentInDb(
            id=instrument.id,
            symbol=instrument.symbol,
            name=instrument.name,
            exchange_id=instrument.exchange_id,
            instrument_type_id=instrument.instrument_type_id,
            sector_id=instrument.sector_id,
            blacklisted=instrument.blacklisted,
            delisted=instrument.delisted,
        )
    return None


async def get_instrument_by_symbol(
    session: AsyncSession,
    symbol: str,
) -> InstrumentInDb | None:
    """Get instrument by symbol"""

    result = await session.execute(
        select(Instrument).where(Instrument.symbol == symbol)
    )
    instrument = result.scalar_one_or_none()
    if instrument:
        return InstrumentInDb(
            id=instrument.id,
            symbol=instrument.symbol,
            name=instrument.name,
            exchange_id=instrument.exchange_id,
            instrument_type_id=instrument.instrument_type_id,
            sector_id=instrument.sector_id,
            blacklisted=instrument.blacklisted,
            delisted=instrument.delisted,
        )
    return None


async def get_all_instruments(
    session: AsyncSession,
) -> list[InstrumentInDb]:
    """Get all instruments"""
    result = await session.execute(select(Instrument))
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
    return InstrumentInDb(
        id=instrument.id,
        symbol=instrument.symbol,
        name=instrument.name,
        exchange_id=instrument.exchange_id,
        instrument_type_id=instrument.instrument_type_id,
        sector_id=instrument.sector_id,
        blacklisted=instrument.blacklisted,
        delisted=instrument.delisted,
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

    await session.delete(instrument)
    await session.commit()
    return True