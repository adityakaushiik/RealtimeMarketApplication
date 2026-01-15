from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import json

from features.sector.sector_schema import SectorInDb, SectorCreate, SectorUpdate
from models import Sector
from config.redis_config import get_redis
from config.logger import logger


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

    # Invalidate cache
    redis = get_redis()
    if redis:
        try:
            await redis.delete("sector:all")
        except Exception as e:
            logger.error(f"Error invalidating sector cache: {e}")

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
    redis = get_redis()
    cache_key = "sector:all"

    if redis:
        try:
            cached_data = await redis.get(cache_key)
            if cached_data:
                data = json.loads(cached_data)
                return [SectorInDb(**item) for item in data]
        except Exception as e:
            logger.error(f"Error reading all sectors from Redis: {e}")

    result = await session.execute(select(Sector))
    sectors = result.scalars().all()

    response = [
        SectorInDb(
            id=s.id,
            name=s.name,
            description=s.description,
        )
        for s in sectors
    ]

    if redis and response:
        try:
            json_data = json.dumps([item.model_dump(mode="json") for item in response])
            await redis.set(cache_key, json_data, ex=86400)
        except Exception as e:
            logger.error(f"Error caching all sectors to Redis: {e}")

    return response


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

    # Invalidate cache
    redis = get_redis()
    if redis:
        try:
            await redis.delete("sector:all")
        except Exception as e:
            logger.error(f"Error invalidating sector cache: {e}")

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

    # Invalidate cache
    redis = get_redis()
    if redis:
        try:
            await redis.delete("sector:all")
        except Exception as e:
            logger.error(f"Error invalidating sector cache: {e}")

    return True
