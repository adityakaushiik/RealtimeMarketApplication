from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from config.database_config import get_db_session
from features.auth.auth_service import require_auth
from utils.common_constants import is_admin
from features.suggestion.suggestion_schema import (
    SuggestionCreate,
    SuggestionUpdate,
    SuggestionInDb,
    SuggestionTypeCreate,
    SuggestionTypeUpdate,
    SuggestionTypeInDb,
    SuggestionResponse,
)
from features.suggestion import suggestion_service

router = APIRouter(prefix="/suggestions", tags=["Suggestions"])


# Suggestion Types Routes (Admin only ideally, but keeping open for now or restricted to authenticated users)
@router.post(
    "/types", response_model=SuggestionTypeInDb, status_code=status.HTTP_201_CREATED
)
async def create_suggestion_type(
    type_data: SuggestionTypeCreate,
    session: AsyncSession = Depends(get_db_session),
    user_claims: dict = Depends(require_auth()),
):
    if not is_admin(user_claims):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized"
        )
    return await suggestion_service.create_suggestion_type(session, type_data)


@router.get("/types", response_model=list[SuggestionTypeInDb])
async def get_suggestion_types(session: AsyncSession = Depends(get_db_session)):
    return await suggestion_service.get_all_suggestion_types(session)


@router.get("/types/{type_id}", response_model=SuggestionTypeInDb)
async def get_suggestion_type(
    type_id: int, session: AsyncSession = Depends(get_db_session)
):
    suggestion_type = await suggestion_service.get_suggestion_type_by_id(
        session, type_id
    )
    if not suggestion_type:
        raise HTTPException(status_code=404, detail="Suggestion type not found")
    return suggestion_type


@router.put("/types/{type_id}", response_model=SuggestionTypeInDb)
async def update_suggestion_type(
    type_id: int,
    type_data: SuggestionTypeUpdate,
    session: AsyncSession = Depends(get_db_session),
    user_claims: dict = Depends(require_auth()),
):
    if not is_admin(user_claims):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized"
        )
    updated_type = await suggestion_service.update_suggestion_type(
        session, type_id, type_data
    )
    if not updated_type:
        raise HTTPException(status_code=404, detail="Suggestion type not found")
    return updated_type


@router.delete("/types/{type_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_suggestion_type(
    type_id: int,
    session: AsyncSession = Depends(get_db_session),
    user_claims: dict = Depends(require_auth()),
):
    if not is_admin(user_claims):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized"
        )
    success = await suggestion_service.delete_suggestion_type(session, type_id)
    if not success:
        raise HTTPException(status_code=404, detail="Suggestion type not found")
    return None


# Suggestions Routes
@router.post("/", response_model=SuggestionInDb, status_code=status.HTTP_201_CREATED)
async def create_suggestion(
    suggestion_data: SuggestionCreate,
    session: AsyncSession = Depends(get_db_session),
    user_claims: dict = Depends(require_auth()),
):
    user_id = int(user_claims.get("id"))
    return await suggestion_service.create_suggestion(session, user_id, suggestion_data)


@router.get("/", response_model=list[SuggestionResponse])
async def get_suggestions(
    session: AsyncSession = Depends(get_db_session),
    user_claims: dict = Depends(require_auth()),
):
    if is_admin(user_claims):
        return await suggestion_service.get_all_suggestions(session)

    user_id = int(user_claims.get("id"))
    return await suggestion_service.get_suggestions_by_user(session, user_id)


@router.get("/my", response_model=list[SuggestionResponse])
async def get_my_suggestions(
    session: AsyncSession = Depends(get_db_session),
    user_claims: dict = Depends(require_auth()),
):
    user_id = int(user_claims.get("id"))
    return await suggestion_service.get_suggestions_by_user(session, user_id)


@router.get("/{suggestion_id}", response_model=SuggestionInDb)
async def get_suggestion(
    suggestion_id: int,
    session: AsyncSession = Depends(get_db_session),
    user_claims: dict = Depends(require_auth()),
):
    suggestion = await suggestion_service.get_suggestion_by_id(session, suggestion_id)
    if not suggestion:
        raise HTTPException(status_code=404, detail="Suggestion not found")

    user_id = int(user_claims.get("id"))
    if suggestion.user_id != user_id and not is_admin(user_claims):
        raise HTTPException(
            status_code=403, detail="Not authorized to view this suggestion"
        )
    return suggestion


@router.put("/{suggestion_id}", response_model=SuggestionInDb)
async def update_suggestion(
    suggestion_id: int,
    suggestion_data: SuggestionUpdate,
    session: AsyncSession = Depends(get_db_session),
    user_claims: dict = Depends(require_auth()),
):
    suggestion = await suggestion_service.get_suggestion_by_id(session, suggestion_id)
    if not suggestion:
        raise HTTPException(status_code=404, detail="Suggestion not found")

    user_id = int(user_claims.get("id"))
    if suggestion.user_id != user_id and not is_admin(user_claims):
        raise HTTPException(
            status_code=403, detail="Not authorized to update this suggestion"
        )

    updated_suggestion = await suggestion_service.update_suggestion(
        session, suggestion_id, suggestion_data
    )
    return updated_suggestion


@router.delete("/{suggestion_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_suggestion(
    suggestion_id: int,
    session: AsyncSession = Depends(get_db_session),
    user_claims: dict = Depends(require_auth()),
):
    suggestion = await suggestion_service.get_suggestion_by_id(session, suggestion_id)
    if not suggestion:
        raise HTTPException(status_code=404, detail="Suggestion not found")

    user_id = int(user_claims.get("id"))
    if suggestion.user_id != user_id and not is_admin(user_claims):
        raise HTTPException(
            status_code=403, detail="Not authorized to delete this suggestion"
        )

    await suggestion_service.delete_suggestion(session, suggestion_id)
    return None
