from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_db_session
from features.auth.auth_service import require_auth
from features.user.user_schema import UserUpdate, UserInDb, ChangePasswordRequest, ResetPasswordRequest
from features.user import user_service
from utils.common_constants import UserRoles

user_router = APIRouter(
    prefix="/user",
    tags=["users"],
)


# @user_router.post("/", response_model=UserInDb, status_code=status.HTTP_201_CREATED)
# async def create_user(
#     user_data: UserCreate,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Create a new user"""
#     # Check if user with same email already exists
#     existing = await user_service.get_user_by_email(session, user_data.email)
#     if existing:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail="User with this email already exists",
#         )
#
#     return await user_service.create_user(session, user_data)


@user_router.get("/", response_model=list[UserInDb])
async def list_users(
    user_status: Annotated[int | None, Query()] = None,
    user_claims: dict = Depends(require_auth([UserRoles.ADMIN])),
    session: AsyncSession = Depends(get_db_session),
):
    """Get all users"""
    return await user_service.get_all_users(session=session, status=user_status)


@user_router.get("/{user_id}", response_model=UserInDb)
async def get_user(
    user_id: int,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Get user by ID"""
    user = await user_service.get_user_by_id(session, user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    return user


@user_router.get("/email/{email}", response_model=UserInDb)
async def get_user_by_email(
    email: str,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Get user by email"""
    user = await user_service.get_user_by_email(session, email)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    return user


@user_router.put("/{user_id}", response_model=UserInDb)
async def update_user(
    user_id: int,
    user_data: UserUpdate,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Update a user"""
    user = await user_service.update_user(session, user_id, user_data)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    return user


@user_router.patch("/update_status/{user_id}", response_model=UserInDb)
async def update_user_status(
        user_id: int,
        user_status: int,
        user_claims: dict = Depends(require_auth([UserRoles.ADMIN])),
        session: AsyncSession = Depends(get_db_session),
):
    """Update user status"""
    updated_user = await user_service.update_user_status(session, user_id, user_status)
    if not updated_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    return updated_user


@user_router.post("/change-password", status_code=status.HTTP_200_OK)
async def change_password(
    data: ChangePasswordRequest,
    user_claims: dict = Depends(require_auth()),
    session: AsyncSession = Depends(get_db_session),
):
    """Change current user's password"""
    user_id = int(user_claims.get("id"))
    success = await user_service.change_user_password(
        session, user_id, data.old_password, data.new_password
    )
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Incorrect old password"
        )
    return {"message": "Password changed successfully"}


@user_router.post("/{user_id}/reset-password", status_code=status.HTTP_200_OK)
async def reset_password(
    user_id: int,
    data: ResetPasswordRequest,
    user_claims: dict = Depends(require_auth([UserRoles.ADMIN])),
    session: AsyncSession = Depends(get_db_session),
):
    """Reset a user's password (Admin only)"""
    success = await user_service.reset_user_password(
        session, user_id, data.new_password
    )
    if not success:
        # Should technically be handled by service raising 404, but just in case
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    return {"message": "Password reset successfully"}

# @user_router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
# async def delete_user(
#     user_id: int,
#     user_claims: dict = Depends(require_auth()),
#     session: AsyncSession = Depends(get_db_session),
# ):
#     """Delete a user"""
#     deleted = await user_service.delete_user(session, user_id)
#     if not deleted:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
#         )
#     return None
