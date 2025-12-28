from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from features.auth.auth_service import hash_password
from models import User
from features.user.user_schema import UserInDb, UserCreate, UserUpdate
from utils.common_constants import UserRoles


async def create_user(
    session: AsyncSession,
    user_data: UserCreate,
) -> UserInDb:
    hashed_password = hash_password(user_data.password)

    new_user = User(
        email=user_data.email,
        hashed_password=hashed_password,
        fname=user_data.fname,
        lname=user_data.lname,
        username=user_data.username,
        profile_picture_url=user_data.profile_picture_url,
        role_id=UserRoles.VIEWER.value
    )
    session.add(new_user)
    await session.commit()
    await session.refresh(new_user)
    return UserInDb(
        id=new_user.id,
        email=new_user.email,
        fname=new_user.fname,
        lname=new_user.lname,
        username=new_user.username,
        profile_picture_url=new_user.profile_picture_url,
        blacklisted=new_user.blacklisted,
        role_id=new_user.role_id,
    )


async def get_user_by_id(
    session: AsyncSession,
    user_id: int,
) -> UserInDb | None:
    """Get user by ID"""
    result = await session.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if user:
        return UserInDb(
            id=user.id,
            email=user.email,
            fname=user.fname,
            lname=user.lname,
            username=user.username,
            profile_picture_url=user.profile_picture_url,
            blacklisted=user.blacklisted,
            role_id=user.role_id,
        )
    return None


async def get_user_by_email(
    session: AsyncSession,
    email: str,
) -> UserInDb | None:
    """Get user by email"""
    result = await session.execute(select(User).where(User.email == email))
    user = result.scalar_one_or_none()
    if user:
        return UserInDb(
            id=user.id,
            email=user.email,
            fname=user.fname,
            lname=user.lname,
            username=user.username,
            profile_picture_url=user.profile_picture_url,
            blacklisted=user.blacklisted,
            role_id=user.role_id,
        )
    return None


async def get_all_users(
    session: AsyncSession,
) -> list[UserInDb]:
    """Get all users"""
    result = await session.execute(select(User))
    users = result.scalars().all()
    return [
        UserInDb(
            id=user.id,
            email=user.email,
            fname=user.fname,
            lname=user.lname,
            username=user.username,
            profile_picture_url=user.profile_picture_url,
            blacklisted=user.blacklisted,
            role_id=user.role_id,
        )
        for user in users
    ]


async def update_user(
    session: AsyncSession,
    user_id: int,
    user_data: UserUpdate,
) -> UserInDb | None:
    """Update a user"""
    result = await session.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        return None

    update_data = user_data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(user, key, value)

    await session.commit()
    await session.refresh(user)
    return UserInDb(
        id=user.id,
        email=user.email,
        fname=user.fname,
        lname=user.lname,
        username=user.username,
        profile_picture_url=user.profile_picture_url,
        blacklisted=user.blacklisted,
        role_id=user.role_id,
    )


async def delete_user(
    session: AsyncSession,
    user_id: int,
) -> bool:
    """Delete a user"""
    result = await session.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        return False

    await session.delete(user)
    await session.commit()
    return True
