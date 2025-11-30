from typing import Optional

from models.base_model_py import BaseModelPy


class UserBase(BaseModelPy):
    email: str
    fname: Optional[str] = None
    lname: Optional[str] = None
    username: Optional[str] = None
    profile_picture_url: Optional[str] = None


class UserWithPassword(UserBase):
    password: str


class UserCreate(UserWithPassword):
    pass


class UserUpdate(BaseModelPy):
    email: Optional[str] = None
    fname: Optional[str] = None
    lname: Optional[str] = None
    username: Optional[str] = None
    profile_picture_url: Optional[str] = None
    blacklisted: Optional[bool] = None
    role_id: Optional[int] = None


class UserInDb(UserBase):
    id: int
    blacklisted: bool
    role_id: Optional[int] = None
