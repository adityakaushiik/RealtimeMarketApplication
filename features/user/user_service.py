from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from features.auth.auth_service import hash_password, verify_password
from models import User
from features.user.user_schema import UserInDb, UserCreate, UserUpdate
from utils.common_constants import UserRoles, UserStatus
from fastapi import HTTPException, status


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
        role_id=UserRoles.VIEWER.value,
        is_active=False,
        status=UserStatus.PENDING.value,
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
        is_active=new_user.is_active,
        status=new_user.status,
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
            is_active=user.is_active,
            status=user.status,
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
            is_active=user.is_active,
            status=user.status,
        )
    return None


async def get_all_users(
    session: AsyncSession,
    status: int | None = None,
) -> list[UserInDb]:
    """Get all users"""
    query = select(User)
    if status is not None:
        query = query.where(User.status == status)

    result = await session.execute(query)
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
            is_active=user.is_active,
            status=user.status,
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
        is_active=user.is_active,
        status=user.status,
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


async def activate_user(
    session: AsyncSession,
    user_id: int,
    status: int = 1,
):
    """Activate a user"""
    result = await session.execute(select(User).where(User.id == user_id))
    user: User = result.scalar_one_or_none()
    if not user:
        return None

    user.is_active = True if status == 1 else False

    await session.commit()
    await session.refresh(user)
    return user


async def update_user_status(
    session: AsyncSession,
    user_id: int,
    status: int,
) -> UserInDb | None:
    """Update user status"""
    result = await session.execute(select(User).where(User.id == user_id))
    user: User = result.scalar_one_or_none()
    if not user:
        return None

    user.status = status
    if status == UserStatus.APPROVED.value:
        user.is_active = True
    else:
        user.is_active = False

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
        is_active=user.is_active,
        status=user.status,
    )


async def change_user_password(
    session: AsyncSession, user_id: int, old_password: str, new_password: str
) -> bool:
    """Change user's own password"""
    result = await session.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )

    if not verify_password(old_password, user.hashed_password):
        return False

    user.hashed_password = hash_password(new_password)
    await session.commit()
    return True


async def reset_user_password(
    session: AsyncSession, user_id: int, new_password: str
) -> bool:
    """Reset user password (Admin only)"""
    result = await session.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )

    user.hashed_password = hash_password(new_password)
    await session.commit()
    return True


async def update_user_role(
    session: AsyncSession,
    user_id: int,
    role_id: int,
) -> UserInDb | None:
    """Update user role"""
    result = await session.execute(select(User).where(User.id == user_id))
    user: User = result.scalar_one_or_none()
    if not user:
        return None

    user.role_id = role_id

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
        is_active=user.is_active,
        status=user.status,
    )
