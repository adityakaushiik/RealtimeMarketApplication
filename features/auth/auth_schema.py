from features.user.user_schema import UserInDb
from models.base_model_py import BaseModelPy


class LoginRequest(BaseModelPy):
    username_or_email: str
    password: str


class LoginResponse(BaseModelPy):
    user: UserInDb
    access_token: str
    token_type: str = "bearer"
