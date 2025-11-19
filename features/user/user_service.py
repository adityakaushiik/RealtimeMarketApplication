from sqlalchemy.ext.asyncio import AsyncSession

from features.auth.auth_service import hash_password
from models import User
from features.user.user_schema import UserInDb


async def create_user(
    session: AsyncSession,
    email: str,
    plain_password: str,
    fname: str | None = None,
    lname: str | None = None,
    username: str | None = None,
    profile_picture_url: str | None = None
) -> UserInDb:

    hashed_password = hash_password(plain_password)

    new_user = User(
        email=email,
        hashed_password=hashed_password,
        fname=fname,
        lname=lname,
        username=username,
        profile_picture_url=profile_picture_url
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