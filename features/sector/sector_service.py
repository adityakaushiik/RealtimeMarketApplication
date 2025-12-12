from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from features.sector.sector_schema import SectorInDb, SectorCreate, SectorUpdate
from models import Sector


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
