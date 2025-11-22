from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Instrument, InstrumentType, Sector
from features.instruments.instrument_schema import (
    InstrumentCreate,
    InstrumentUpdate,
    InstrumentInDb,
    InstrumentTypeCreate,
    InstrumentTypeUpdate,
    InstrumentTypeInDb,
    SectorCreate,
    SectorUpdate,
    SectorInDb,
)


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
    result = await session.execute(select(Instrument).where(Instrument.id == instrument_id))
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
    result = await session.execute(select(Instrument).where(Instrument.id == instrument_id))
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
    result = await session.execute(select(Instrument).where(Instrument.id == instrument_id))
    instrument = result.scalar_one_or_none()
    if not instrument:
        return False

    await session.delete(instrument)
    await session.commit()
    return True


# InstrumentType CRUD

async def create_instrument_type(
    session: AsyncSession,
    type_data: InstrumentTypeCreate,
) -> InstrumentTypeInDb:
    """Create a new instrument type"""
    new_type = InstrumentType(
        code=type_data.code,
        name=type_data.name,
        description=type_data.description,
        category=type_data.category,
        display_order=type_data.display_order,
    )
    session.add(new_type)
    await session.commit()
    await session.refresh(new_type)
    return InstrumentTypeInDb(
        id=new_type.id,
        code=new_type.code,
        name=new_type.name,
        description=new_type.description,
        category=new_type.category,
        display_order=new_type.display_order,
    )


async def get_instrument_type_by_id(
    session: AsyncSession,
    type_id: int,
) -> InstrumentTypeInDb | None:
    """Get instrument type by ID"""
    result = await session.execute(select(InstrumentType).where(InstrumentType.id == type_id))
    type_obj = result.scalar_one_or_none()
    if type_obj:
        return InstrumentTypeInDb(
            id=type_obj.id,
            code=type_obj.code,
            name=type_obj.name,
            description=type_obj.description,
            category=type_obj.category,
            display_order=type_obj.display_order,
        )
    return None


async def get_all_instrument_types(
    session: AsyncSession,
) -> list[InstrumentTypeInDb]:
    """Get all instrument types"""
    result = await session.execute(select(InstrumentType))
    types = result.scalars().all()
    return [
        InstrumentTypeInDb(
            id=t.id,
            code=t.code,
            name=t.name,
            description=t.description,
            category=t.category,
            display_order=t.display_order,
        )
        for t in types
    ]


async def update_instrument_type(
    session: AsyncSession,
    type_id: int,
    type_data: InstrumentTypeUpdate,
) -> InstrumentTypeInDb | None:
    """Update an instrument type"""
    result = await session.execute(select(InstrumentType).where(InstrumentType.id == type_id))
    type_obj = result.scalar_one_or_none()
    if not type_obj:
        return None

    update_data = type_data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(type_obj, key, value)

    await session.commit()
    await session.refresh(type_obj)
    return InstrumentTypeInDb(
        id=type_obj.id,
        code=type_obj.code,
        name=type_obj.name,
        description=type_obj.description,
        category=type_obj.category,
        display_order=type_obj.display_order,
    )


async def delete_instrument_type(
    session: AsyncSession,
    type_id: int,
) -> bool:
    """Delete an instrument type"""
    result = await session.execute(select(InstrumentType).where(InstrumentType.id == type_id))
    type_obj = result.scalar_one_or_none()
    if not type_obj:
        return False

    await session.delete(type_obj)
    await session.commit()
    return True


# Sector CRUD

async def create_sector(
    session: AsyncSession,
    sector_data: SectorCreate,
) -> SectorInDb:
    """Create a new sector"""
    new_sector = Sector(
        name=sector_data.name,
        description=sector_data.description,
    )
    session.add(new_sector)
    await session.commit()
    await session.refresh(new_sector)
    return SectorInDb(
        id=new_sector.id,
        name=new_sector.name,
        description=new_sector.description,
    )


async def get_sector_by_id(
    session: AsyncSession,
    sector_id: int,
) -> SectorInDb | None:
    """Get sector by ID"""
    result = await session.execute(select(Sector).where(Sector.id == sector_id))
    sector = result.scalar_one_or_none()
    if sector:
        return SectorInDb(
            id=sector.id,
            name=sector.name,
            description=sector.description,
        )
    return None


async def get_all_sectors(
    session: AsyncSession,
) -> list[SectorInDb]:
    """Get all sectors"""
    result = await session.execute(select(Sector))
    sectors = result.scalars().all()
    return [
        SectorInDb(
            id=s.id,
            name=s.name,
            description=s.description,
        )
        for s in sectors
    ]


async def update_sector(
    session: AsyncSession,
    sector_id: int,
    sector_data: SectorUpdate,
) -> SectorInDb | None:
    """Update a sector"""
    result = await session.execute(select(Sector).where(Sector.id == sector_id))
    sector = result.scalar_one_or_none()
    if not sector:
        return None

    update_data = sector_data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(sector, key, value)

    await session.commit()
    await session.refresh(sector)
    return SectorInDb(
        id=sector.id,
        name=sector.name,
        description=sector.description,
    )


async def delete_sector(
    session: AsyncSession,
    sector_id: int,
) -> bool:
    """Delete a sector"""
    result = await session.execute(select(Sector).where(Sector.id == sector_id))
    sector = result.scalar_one_or_none()
    if not sector:
        return False

    await session.delete(sector)
    await session.commit()
    return True
