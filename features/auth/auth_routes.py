from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette import status

from config.database_config import get_db_session
from models import User
from features.auth.auth_schema import LoginResponse, LoginRequest
from features.user.user_service import create_user
from features.user.user_schema import UserWithPassword, UserInDb, UserCreate
from features.auth.auth_service import authenticate_user, create_access_token, require_auth
from utils.common_constants import UserRoles

auth_router = APIRouter(
    prefix="/auth",
    tags=["auth"],
)


@auth_router.post(
    "/login", response_model=LoginResponse, status_code=status.HTTP_200_OK
)
async def login(
    payload: LoginRequest, session: AsyncSession = Depends(get_db_session)
) -> LoginResponse:
    user: UserInDb = await authenticate_user(
        session, payload.username_or_email, payload.password
    )
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials"
        )

    roles = [user.role_id] if getattr(user, "role_id", None) else []
    claims = {
        "id": str(user.id),
        "email": user.email,
        "roles": roles,
    }
    token = create_access_token(claims)

    # Also give it user
    return LoginResponse(access_token=token, user=user)


@auth_router.post("/register")
async def register(
    payload: UserWithPassword, session: AsyncSession = Depends(get_db_session)
) -> UserInDb:
    # Check if user with email or username already exists
    # Check email
    result = await session.execute(select(User).where(User.email == payload.email))
    existing_user = result.scalar_one_or_none()
    if existing_user:
        raise HTTPException(
            status_code=400, detail="User with this email already exists"
        )

    # Check username if provided
    if payload.username:
        result = await session.execute(
            select(User).where(User.username == payload.username)
        )
        existing_user = result.scalar_one_or_none()
        if existing_user:
            raise HTTPException(
                status_code=400, detail="User with this username already exists"
            )

    # Create new user
    user_data = UserCreate(**payload.model_dump())
    user = await create_user(session, user_data)

    return user