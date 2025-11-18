from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional

import jwt
from fastapi import HTTPException, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette import status

from config.settings import get_settings
from utils.common_constants import UserRoles

# New imports for authentication
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from passlib.context import CryptContext
from models.user import User

# Password hashing context (bcrypt with safe defaults)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Reusable bearer scheme instance
bearer_scheme = HTTPBearer(auto_error=True)


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


async def authenticate_user(session: AsyncSession, username_or_email: str, password: str) -> Optional[User]:
    """Return user if username/email+password are valid; else None."""
    stmt = select(User).where((User.email == username_or_email) | (User.username == username_or_email))
    res = await session.execute(stmt)
    user: Optional[User] = res.scalar_one_or_none()
    if not user:
        return None
    if user.blacklisted:
        return None
    if not verify_password(password, user.hashed_password):
        return None
    return user


def create_access_token(data: dict, expires_delta_in_days: Optional[float] = 2) -> str:
    settings = get_settings()
    to_encode = data.copy()
    # Use timezone-aware UTC timestamps for exp & iat
    now = datetime.now(timezone.utc)
    expire = now + timedelta(days=expires_delta_in_days)
    to_encode.update({"exp": expire, "iat": now})
    return jwt.encode(to_encode, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)


def verify_jwt(token: str) -> Dict[str, Any]:
    """Verify JWT token and return decoded payload or raise HTTPException."""
    settings = get_settings()
    try:
        payload = jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.InvalidSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token signature",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


def require_auth(required_roles: Optional[list[UserRoles]] = None, require_all: bool = False):
    """Dependency factory enforcing authentication and optional role authorization.

    Usage:
        @router.get("/me")
        async def me(claims: dict = Depends(require_auth())):
            return claims

        @router.get("/admin")
        async def admin(claims: dict = Depends(require_auth([UserRoles.ADMIN]))):
            ...

    Args:
        required_roles: list of UserRoles to check against JWT's roles claim.
        require_all: if True require ALL roles, else ANY overlap.
    Returns:
        dependency coroutine that yields decoded JWT payload (dict).
    """

    async def dependency(
        request: Request,
        credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
    ) -> Dict[str, Any]:
        token = credentials.credentials
        payload = verify_jwt(token)

        # Optional role enforcement
        if required_roles:
            user_roles = set(payload.get("roles", []))
            needed = {r.value if hasattr(r, "value") else r for r in required_roles}
            if require_all:
                authorized = needed.issubset(user_roles)
            else:
                authorized = not user_roles.isdisjoint(needed)
            if not authorized:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Insufficient permissions",
                )
        # Stash on request.state for downstream access
        request.state.user = payload
        return payload

    return dependency
