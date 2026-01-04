from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from models import Suggestion, SuggestionType
from features.suggestion.suggestion_schema import (
    SuggestionCreate,
    SuggestionUpdate,
    SuggestionTypeCreate,
    SuggestionTypeUpdate,
    SuggestionInDb,
    SuggestionTypeInDb
)

# Suggestion Type CRUD
async def create_suggestion_type(session: AsyncSession, suggestion_type_data: SuggestionTypeCreate) -> SuggestionTypeInDb:
    new_type = SuggestionType(**suggestion_type_data.model_dump())
    session.add(new_type)
    await session.commit()
    await session.refresh(new_type)
    return SuggestionTypeInDb.model_validate(new_type)

async def get_all_suggestion_types(session: AsyncSession) -> list[SuggestionTypeInDb]:
    result = await session.execute(select(SuggestionType))
    types = result.scalars().all()
    return [SuggestionTypeInDb.model_validate(t) for t in types]

async def get_suggestion_type_by_id(session: AsyncSession, type_id: int) -> SuggestionTypeInDb | None:
    result = await session.execute(select(SuggestionType).where(SuggestionType.id == type_id))
    suggestion_type = result.scalar_one_or_none()
    if suggestion_type:
        return SuggestionTypeInDb.model_validate(suggestion_type)
    return None

async def update_suggestion_type(session: AsyncSession, type_id: int, update_data: SuggestionTypeUpdate) -> SuggestionTypeInDb | None:
    result = await session.execute(select(SuggestionType).where(SuggestionType.id == type_id))
    suggestion_type = result.scalar_one_or_none()
    if not suggestion_type:
        return None

    for key, value in update_data.model_dump(exclude_unset=True).items():
        setattr(suggestion_type, key, value)

    await session.commit()
    await session.refresh(suggestion_type)
    return SuggestionTypeInDb.model_validate(suggestion_type)

async def delete_suggestion_type(session: AsyncSession, type_id: int) -> bool:
    result = await session.execute(select(SuggestionType).where(SuggestionType.id == type_id))
    suggestion_type = result.scalar_one_or_none()
    if not suggestion_type:
        return False

    await session.delete(suggestion_type)
    await session.commit()
    return True

# Suggestion CRUD
async def create_suggestion(session: AsyncSession, user_id: int, suggestion_data: SuggestionCreate) -> SuggestionInDb:
    new_suggestion = Suggestion(user_id=user_id, **suggestion_data.model_dump())
    session.add(new_suggestion)
    await session.commit()
    await session.refresh(new_suggestion)
    return SuggestionInDb.model_validate(new_suggestion)

async def get_all_suggestions(session: AsyncSession) -> list[SuggestionInDb]:
    result = await session.execute(select(Suggestion))
    suggestions = result.scalars().all()
    return [SuggestionInDb.model_validate(s) for s in suggestions]

async def get_suggestions_by_user(session: AsyncSession, user_id: int) -> list[SuggestionInDb]:
    result = await session.execute(select(Suggestion).where(Suggestion.user_id == user_id))
    suggestions = result.scalars().all()
    return [SuggestionInDb.model_validate(s) for s in suggestions]

async def get_suggestion_by_id(session: AsyncSession, suggestion_id: int) -> SuggestionInDb | None:
    result = await session.execute(select(Suggestion).where(Suggestion.id == suggestion_id))
    suggestion = result.scalar_one_or_none()
    if suggestion:
        return SuggestionInDb.model_validate(suggestion)
    return None

async def update_suggestion(session: AsyncSession, suggestion_id: int, update_data: SuggestionUpdate) -> SuggestionInDb | None:
    result = await session.execute(select(Suggestion).where(Suggestion.id == suggestion_id))
    suggestion = result.scalar_one_or_none()
    if not suggestion:
        return None

    for key, value in update_data.model_dump(exclude_unset=True).items():
        setattr(suggestion, key, value)

    await session.commit()
    await session.refresh(suggestion)
    return SuggestionInDb.model_validate(suggestion)

async def delete_suggestion(session: AsyncSession, suggestion_id: int) -> bool:
    result = await session.execute(select(Suggestion).where(Suggestion.id == suggestion_id))
    suggestion = result.scalar_one_or_none()
    if not suggestion:
        return False

    await session.delete(suggestion)
    await session.commit()
    return True

