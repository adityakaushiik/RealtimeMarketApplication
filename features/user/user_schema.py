from models.base_model_py import BaseModelPy


class UserBase(BaseModelPy):
    email: str
    fname: str | None = None
    lname: str | None = None
    username: str | None = None
    profile_picture_url: str | None = None


class UserWithPassword(UserBase):
    password: str


class UserCreate(UserWithPassword):
    pass


class UserUpdate(BaseModelPy):
    email: str | None = None
    fname: str | None = None
    lname: str | None = None
    username: str | None = None
    profile_picture_url: str | None = None
    blacklisted: bool | None = None
    role_id: int | None = None


class UserInDb(UserBase):
    id: int
    blacklisted: bool
    role_id: int | None = None
