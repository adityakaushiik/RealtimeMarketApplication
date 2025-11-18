from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from starlette import status
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_database_engine
from services.auth_service import authenticate_user, create_access_token, require_auth

auth_router = APIRouter(
    prefix="/auth",
    tags=["auth"],
)


class LoginRequest(BaseModel):
    username_or_email: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


@auth_router.post("/login", response_model=LoginResponse, status_code=status.HTTP_200_OK)
async def login(payload: LoginRequest, session: AsyncSession = Depends(get_database_engine)) -> LoginResponse:
    user = await authenticate_user(session, payload.username_or_email, payload.password)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    roles = [user.role_id] if getattr(user, "role_id", None) else []
    claims = {
        "id": str(user.id),
        "email": user.email,
        "roles": roles,
    }
    token = create_access_token(claims)

    # Also give it user
    return LoginResponse(access_token=token)
