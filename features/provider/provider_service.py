from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Provider, ProviderInstrumentMapping
from features.provider.provider_schema import (
    ProviderCreate,
    ProviderUpdate,
    ProviderInDb,
    ProviderInstrumentMappingCreate,
    ProviderInstrumentMappingUpdate,
    ProviderInstrumentMappingInDb,
)


async def create_provider(
    session: AsyncSession, provider_data: ProviderCreate
) -> ProviderInDb:
    new_provider = Provider(
        name=provider_data.name,
        code=provider_data.code,
        credentials=provider_data.credentials,
        rate_limit=provider_data.rate_limit,
    )
    session.add(new_provider)
    await session.commit()
    await session.refresh(new_provider)
    return ProviderInDb(
        id=new_provider.id,
        name=new_provider.name,
        code=new_provider.code,
        credentials=new_provider.credentials,
        rate_limit=new_provider.rate_limit,
        created_at=new_provider.created_at,
        updated_at=new_provider.updated_at,
        is_active=new_provider.is_active,
    )


async def get_all_providers(session: AsyncSession) -> list[ProviderInDb]:
    result = await session.execute(select(Provider))
    providers = result.scalars().all()
    return [
        ProviderInDb(
            id=p.id,
            name=p.name,
            code=p.code,
            credentials=p.credentials,
            rate_limit=p.rate_limit,
            created_at=p.created_at,
            updated_at=p.updated_at,
            is_active=p.is_active,
        )
        for p in providers
    ]


async def get_provider_by_id(
    session: AsyncSession, provider_id: int
) -> ProviderInDb | None:
    result = await session.execute(select(Provider).where(Provider.id == provider_id))
    provider = result.scalar_one_or_none()
    if provider:
        return ProviderInDb(
            id=provider.id,
            name=provider.name,
            code=provider.code,
            credentials=provider.credentials,
            rate_limit=provider.rate_limit,
            created_at=provider.created_at,
            updated_at=provider.updated_at,
            is_active=provider.is_active,
        )
    return None


async def get_provider_by_code(session: AsyncSession, code: str) -> ProviderInDb | None:
    result = await session.execute(select(Provider).where(Provider.code == code))
    provider = result.scalar_one_or_none()
    if provider:
        return ProviderInDb(
            id=provider.id,
            name=provider.name,
            code=provider.code,
            credentials=provider.credentials,
            rate_limit=provider.rate_limit,
            created_at=provider.created_at,
            updated_at=provider.updated_at,
            is_active=provider.is_active,
        )
    return None


async def update_provider(
    session: AsyncSession, provider_id: int, provider_data: ProviderUpdate
) -> ProviderInDb | None:
    result = await session.execute(select(Provider).where(Provider.id == provider_id))
    provider = result.scalar_one_or_none()
    if not provider:
        return None

    update_data = provider_data.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(provider, key, value)

    await session.commit()
    await session.refresh(provider)
    return ProviderInDb(
        id=provider.id,
        name=provider.name,
        code=provider.code,
        credentials=provider.credentials,
        rate_limit=provider.rate_limit,
        created_at=provider.created_at,
        updated_at=provider.updated_at,
        is_active=provider.is_active,
    )


async def delete_provider(session: AsyncSession, provider_id: int) -> bool:
    result = await session.execute(select(Provider).where(Provider.id == provider_id))
    provider = result.scalar_one_or_none()
    if not provider:
        return False

    await session.delete(provider)
    await session.commit()
    return True


async def create_provider_instrument_mapping(
    session: AsyncSession,
    mapping_data: ProviderInstrumentMappingCreate,
) -> ProviderInstrumentMappingInDb:
    """Create a new provider-instrument mapping"""
    new_mapping = ProviderInstrumentMapping(
        provider_id=mapping_data.provider_id,
        instrument_id=mapping_data.instrument_id,
        provider_instrument_search_code=mapping_data.provider_instrument_search_code,
    )
    session.add(new_mapping)
    await session.commit()
    await session.refresh(new_mapping)
    return ProviderInstrumentMappingInDb(
        provider_id=new_mapping.provider_id,
        instrument_id=new_mapping.instrument_id,
        provider_instrument_search_code=new_mapping.provider_instrument_search_code,
    )


async def get_mappings_for_provider(
    session: AsyncSession,
    provider_id: int,
) -> list[ProviderInstrumentMappingInDb]:
    """Get all instrument mappings for a provider"""
    result = await session.execute(
        select(ProviderInstrumentMapping).where(ProviderInstrumentMapping.provider_id == provider_id)
    )
    mappings = result.scalars().all()
    return [
        ProviderInstrumentMappingInDb(
            provider_id=mapping.provider_id,
            instrument_id=mapping.instrument_id,
            provider_instrument_search_code=mapping.provider_instrument_search_code,
        )
        for mapping in mappings
    ]


async def get_mappings_for_instrument(
    session: AsyncSession,
    instrument_id: int,
) -> list[ProviderInstrumentMappingInDb]:
    """Get all provider mappings for an instrument"""
    result = await session.execute(
        select(ProviderInstrumentMapping).where(ProviderInstrumentMapping.instrument_id == instrument_id)
    )
    mappings = result.scalars().all()
    return [
        ProviderInstrumentMappingInDb(
            provider_id=mapping.provider_id,
            instrument_id=mapping.instrument_id,
            provider_instrument_search_code=mapping.provider_instrument_search_code,
        )
        for mapping in mappings
    ]


async def update_provider_instrument_mapping(
    session: AsyncSession,
    provider_id: int,
    instrument_id: int,
    update_data: ProviderInstrumentMappingUpdate,
) -> ProviderInstrumentMappingInDb | None:
    """Update a provider-instrument mapping"""
    result = await session.execute(
        select(ProviderInstrumentMapping).where(
            (ProviderInstrumentMapping.provider_id == provider_id) &
            (ProviderInstrumentMapping.instrument_id == instrument_id)
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
    return ProviderInstrumentMappingInDb(
        provider_id=mapping.provider_id,
        instrument_id=mapping.instrument_id,
        provider_instrument_search_code=mapping.provider_instrument_search_code,
    )


async def delete_provider_instrument_mapping(
    session: AsyncSession,
    provider_id: int,
    instrument_id: int,
) -> bool:
    """Delete a provider-instrument mapping"""
    result = await session.execute(
        select(ProviderInstrumentMapping).where(
            (ProviderInstrumentMapping.provider_id == provider_id) &
            (ProviderInstrumentMapping.instrument_id == instrument_id)
        )
    )
    mapping = result.scalar_one_or_none()
    if not mapping:
        return False

    await session.delete(mapping)
    await session.commit()
    return True
