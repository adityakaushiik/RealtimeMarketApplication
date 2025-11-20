from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Provider
from features.provider.provider_schema import (
    ProviderCreate,
    ProviderUpdate,
    ProviderInDb,
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
