from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Exchange, ExchangeProviderMapping
from features.exchange.exchange_schema import (
    ExchangeCreate,
    ExchangeUpdate,
    ExchangeInDb,
    ExchangeProviderMappingCreate,
    ExchangeProviderMappingUpdate,
    ExchangeProviderMappingInDb,
)
from services.data.exchange_data import ExchangeData
import json
from config.redis_config import get_redis
from config.logger import logger


async def create_exchange(
    session: AsyncSession,
    exchange_data: ExchangeCreate,
):
    """Create a new exchange"""
    new_exchange = Exchange(
        name=exchange_data.name,
        code=exchange_data.code,
        timezone=exchange_data.timezone,
        country=exchange_data.country,
        currency=exchange_data.currency,
        pre_market_open_time=exchange_data.pre_market_open_time,
        market_open_time=exchange_data.market_open_time,
        market_close_time=exchange_data.market_close_time,
        post_market_close_time=exchange_data.post_market_close_time,
    )
    session.add(new_exchange)
    await session.commit()
    await session.refresh(new_exchange)
    return ExchangeInDb(
        id=new_exchange.id,
        name=new_exchange.name,
        code=new_exchange.code,
        timezone=new_exchange.timezone,
        country=new_exchange.country,
        currency=new_exchange.currency,
        pre_market_open_time=new_exchange.pre_market_open_time,
        market_open_time=new_exchange.market_open_time,
        market_close_time=new_exchange.market_close_time,
        post_market_close_time=new_exchange.post_market_close_time,
    )


async def get_exchange_by_id(
    session: AsyncSession,
    exchange_id: int,
):
    """Get exchange by ID"""
    redis = get_redis()
    cache_key = f"exchange:id:{exchange_id}"

    if redis:
        try:
            cached_data = await redis.get(cache_key)
            if cached_data:
                data = json.loads(cached_data)
                return ExchangeInDb(**data)
        except Exception as e:
            logger.error(f"Error reading exchange from Redis: {e}")

    result = await session.execute(select(Exchange).where(Exchange.id == exchange_id))
    exchange = result.scalar_one_or_none()
    if exchange:
        exchange_db = ExchangeInDb(
            id=exchange.id,
            name=exchange.name,
            code=exchange.code,
            timezone=exchange.timezone,
            country=exchange.country,
            currency=exchange.currency,
            pre_market_open_time=exchange.pre_market_open_time,
            market_open_time=exchange.market_open_time,
            market_close_time=exchange.market_close_time,
            post_market_close_time=exchange.post_market_close_time,
        )

        if redis:
            try:
                await redis.set(cache_key, exchange_db.model_dump_json(), ex=86400)
                # Also cache by code
                await redis.set(f"exchange:code:{exchange.code}", exchange_db.model_dump_json(), ex=86400)
            except Exception as e:
                logger.error(f"Error caching exchange to Redis: {e}")

        return exchange_db
    return None


async def get_exchange_by_code(
    session: AsyncSession,
    code: str,
):
    """Get exchange by code"""
    redis = get_redis()
    cache_key = f"exchange:code:{code}"

    if redis:
        try:
            cached_data = await redis.get(cache_key)
            if cached_data:
                data = json.loads(cached_data)
                return ExchangeInDb(**data)
        except Exception as e:
            logger.error(f"Error reading exchange from Redis: {e}")

    result = await session.execute(select(Exchange).where(Exchange.code == code))
    exchange = result.scalar_one_or_none()
    if exchange:
        exchange_db = ExchangeInDb(
            id=exchange.id,
            name=exchange.name,
            code=exchange.code,
            timezone=exchange.timezone,
            country=exchange.country,
            currency=exchange.currency,
            pre_market_open_time=exchange.pre_market_open_time,
            market_open_time=exchange.market_open_time,
            market_close_time=exchange.market_close_time,
            post_market_close_time=exchange.post_market_close_time,
        )

        if redis:
            try:
                await redis.set(cache_key, exchange_db.model_dump_json(), ex=86400)
                # Also cache by ID
                await redis.set(f"exchange:id:{exchange.id}", exchange_db.model_dump_json(), ex=86400)
            except Exception as e:
                logger.error(f"Error caching exchange to Redis: {e}")

        return exchange_db
    return None


async def get_all_exchanges(
    session: AsyncSession,
) -> list[ExchangeInDb]:
    """Get all exchanges"""
    redis = get_redis()
    cache_key = "exchange:all"

    if redis:
        try:
            cached_data = await redis.get(cache_key)
            if cached_data:
                data = json.loads(cached_data)
                return [ExchangeInDb(**item) for item in data]
        except Exception as e:
            logger.error(f"Error reading all exchanges from Redis: {e}")

    result = await session.execute(select(Exchange).where(Exchange.is_active == True))
    exchanges = result.scalars().all()

    response = [
        ExchangeInDb(
            id=exchange.id,
            name=exchange.name,
            code=exchange.code,
            timezone=exchange.timezone,
            country=exchange.country,
            currency=exchange.currency,
            pre_market_open_time=exchange.pre_market_open_time,
            market_open_time=exchange.market_open_time,
            market_close_time=exchange.market_close_time,
            post_market_close_time=exchange.post_market_close_time,
        )
        for exchange in exchanges
    ]

    if redis and response:
        try:
            json_data = json.dumps([item.model_dump(mode='json') for item in response])
            await redis.set(cache_key, json_data, ex=86400)
        except Exception as e:
            logger.error(f"Error caching all exchanges to Redis: {e}")

    return response


async def get_all_active_exchanges(
    session: AsyncSession,
) -> list[ExchangeData]:
    """Get all active exchanges as ExchangeData objects"""
    redis = get_redis()
    cache_key = "exchange:active:all"

    if redis:
        try:
            cached_data = await redis.get(cache_key)
            if cached_data:
                data = json.loads(cached_data)
                # ExchangeData is a Pydantic model (or dataclass?)
                # It's a Pydantic model in services/data/exchange_data.py usually
                return [ExchangeData(**item) for item in data]
        except Exception as e:
            logger.error(f"Error reading active exchanges from Redis: {e}")

    result = await session.execute(select(Exchange).where(Exchange.is_active == True))
    exchanges = result.scalars().all()

    response = [
        ExchangeData(
            exchange_name=exchange.name,
            exchange_id=exchange.id,
            market_open_time=exchange.market_open_time,
            market_close_time=exchange.market_close_time,
            pre_market_open_time=exchange.pre_market_open_time,
            post_market_close_time=exchange.post_market_close_time,
            timezone_str=exchange.timezone,
        )
        for exchange in exchanges
    ]

    if redis and response:
        try:
            json_data = json.dumps([item.model_dump(mode='json') for item in response])
            await redis.set(cache_key, json_data, ex=86400)
        except Exception as e:
            logger.error(f"Error caching active exchanges to Redis: {e}")

    return response


async def update_exchange(
    session: AsyncSession,
    exchange_id: int,
    exchange_data: ExchangeUpdate,
):
    """Update an exchange"""
    result = await session.execute(select(Exchange).where(Exchange.id == exchange_id))
    exchange = result.scalar_one_or_none()
    if not exchange:
        return None

    update_data = exchange_data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(exchange, key, value)

    await session.commit()
    await session.refresh(exchange)

    # Invalidate cache
    redis = get_redis()
    if redis:
        try:
            await redis.delete(f"exchange:id:{exchange.id}")
            await redis.delete(f"exchange:code:{exchange.code}")
            await redis.delete("exchange:all")
            await redis.delete("exchange:active:all")
        except Exception as e:
            logger.error(f"Error invalidating exchange cache: {e}")

    return ExchangeInDb(
        id=exchange.id,
        name=exchange.name,
        code=exchange.code,
        timezone=exchange.timezone,
        country=exchange.country,
        currency=exchange.currency,
        pre_market_open_time=exchange.pre_market_open_time,
        market_open_time=exchange.market_open_time,
        market_close_time=exchange.market_close_time,
        post_market_close_time=exchange.post_market_close_time,
    )


async def delete_exchange(
    session: AsyncSession,
    exchange_id: int,
) -> bool:
    """Delete an exchange"""
    result = await session.execute(select(Exchange).where(Exchange.id == exchange_id))
    exchange = result.scalar_one_or_none()
    if not exchange:
        return False

    # Capture code before delete
    code = exchange.code

    await session.delete(exchange)
    await session.commit()

    # Invalidate cache
    redis = get_redis()
    if redis:
        try:
            await redis.delete(f"exchange:id:{exchange_id}")
            await redis.delete(f"exchange:code:{code}")
            await redis.delete("exchange:all")
            await redis.delete("exchange:active:all")
        except Exception as e:
            logger.error(f"Error invalidating exchange cache: {e}")

    return True


# ExchangeProviderMapping functions


async def create_exchange_provider_mapping(
    session: AsyncSession,
    mapping_data: ExchangeProviderMappingCreate,
) -> ExchangeProviderMappingInDb:
    """Create a new exchange-provider mapping"""
    new_mapping = ExchangeProviderMapping(
        provider_id=mapping_data.provider_id,
        exchange_id=mapping_data.exchange_id,
        is_active=mapping_data.is_active,
        is_primary=mapping_data.is_primary,
    )
    session.add(new_mapping)
    await session.commit()
    await session.refresh(new_mapping)
    return ExchangeProviderMappingInDb(
        provider_id=new_mapping.provider_id,
        exchange_id=new_mapping.exchange_id,
        is_active=new_mapping.is_active,
        is_primary=new_mapping.is_primary,
    )


async def get_mappings_for_exchange(
    session: AsyncSession,
    exchange_id: int,
) -> list[ExchangeProviderMappingInDb]:
    """Get all provider mappings for an exchange"""
    result = await session.execute(
        select(ExchangeProviderMapping).where(
            ExchangeProviderMapping.exchange_id == exchange_id
        )
    )
    mappings = result.scalars().all()
    return [
        ExchangeProviderMappingInDb(
            provider_id=mapping.provider_id,
            exchange_id=mapping.exchange_id,
            is_active=mapping.is_active,
            is_primary=mapping.is_primary,
        )
        for mapping in mappings
    ]


async def get_mappings_for_provider(
    session: AsyncSession,
    provider_id: int,
) -> list[ExchangeProviderMappingInDb]:
    """Get all exchange mappings for a provider"""
    result = await session.execute(
        select(ExchangeProviderMapping).where(
            ExchangeProviderMapping.provider_id == provider_id
        )
    )
    mappings = result.scalars().all()
    return [
        ExchangeProviderMappingInDb(
            provider_id=mapping.provider_id,
            exchange_id=mapping.exchange_id,
            is_active=mapping.is_active,
            is_primary=mapping.is_primary,
        )
        for mapping in mappings
    ]


async def update_exchange_provider_mapping(
    session: AsyncSession,
    provider_id: int,
    exchange_id: int,
    update_data: ExchangeProviderMappingUpdate,
) -> ExchangeProviderMappingInDb | None:
    """Update an exchange-provider mapping"""
    result = await session.execute(
        select(ExchangeProviderMapping).where(
            (ExchangeProviderMapping.provider_id == provider_id)
            & (ExchangeProviderMapping.exchange_id == exchange_id)
        )
    )
    mapping = result.scalar_one_or_none()
    if not mapping:
        return None

    update_dict = update_data.model_dump(exclude_unset=True)
    for key, value in update_dict.items():
        setattr(mapping, key, value)

    await session.commit()
    await session.refresh(mapping)
    return ExchangeProviderMappingInDb(
        provider_id=mapping.provider_id,
        exchange_id=mapping.exchange_id,
        is_active=mapping.is_active,
        is_primary=mapping.is_primary,
    )


async def delete_exchange_provider_mapping(
    session: AsyncSession,
    provider_id: int,
    exchange_id: int,
) -> bool:
    """Delete an exchange-provider mapping"""
    result = await session.execute(
        select(ExchangeProviderMapping).where(
            (ExchangeProviderMapping.provider_id == provider_id)
            & (ExchangeProviderMapping.exchange_id == exchange_id)
        )
    )
    mapping = result.scalar_one_or_none()
    if not mapping:
        return False

    await session.delete(mapping)
    await session.commit()
    return True
