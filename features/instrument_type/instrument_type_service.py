from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import json

from features.instrument_type.instrument_type_schema import InstrumentTypeCreate, InstrumentTypeInDb, \
    InstrumentTypeUpdate
from models import InstrumentType
from config.redis_config import get_redis
from config.logger import logger


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

    # Invalidate cache
    redis = get_redis()
    if redis:
        try:
            await redis.delete("instrument_type:all")
        except Exception as e:
            logger.error(f"Error invalidating instrument_type cache: {e}")

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
    result = await session.execute(
        select(InstrumentType).where(InstrumentType.id == type_id)
    )
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
    redis = get_redis()
    cache_key = "instrument_type:all"

    if redis:
        try:
            cached_data = await redis.get(cache_key)
            if cached_data:
                data = json.loads(cached_data)
                return [InstrumentTypeInDb(**item) for item in data]
        except Exception as e:
            logger.error(f"Error reading all instrument types from Redis: {e}")

    result = await session.execute(select(InstrumentType))
    types = result.scalars().all()

    response = [
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

    if redis and response:
        try:
            json_data = json.dumps([item.model_dump(mode='json') for item in response])
            await redis.set(cache_key, json_data, ex=86400)
        except Exception as e:
            logger.error(f"Error caching all instrument types to Redis: {e}")

    return response


async def update_instrument_type(
        session: AsyncSession,
        type_id: int,
        type_data: InstrumentTypeUpdate,
) -> InstrumentTypeInDb | None:
    """Update an instrument type"""
    result = await session.execute(
        select(InstrumentType).where(InstrumentType.id == type_id)
    )
    type_obj = result.scalar_one_or_none()
    if not type_obj:
        return None

    update_data = type_data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(type_obj, key, value)

    await session.commit()
    await session.refresh(type_obj)

    # Invalidate cache
    redis = get_redis()
    if redis:
        try:
            await redis.delete("instrument_type:all")
        except Exception as e:
            logger.error(f"Error invalidating instrument_type cache: {e}")

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
    result = await session.execute(
        select(InstrumentType).where(InstrumentType.id == type_id)
    )
    type_obj = result.scalar_one_or_none()
    if not type_obj:
        return False

    await session.delete(type_obj)
    await session.commit()

    # Invalidate cache
    redis = get_redis()
    if redis:
        try:
            await redis.delete("instrument_type:all")
        except Exception as e:
            logger.error(f"Error invalidating instrument_type cache: {e}")

    return True
