from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # For Render or equivalent hosting
    PYTHON_VERSION: str = "3.13"

    # For Neon Postgres Db
    DATABASE_URL: str = ""
    SECRET_KEY: str = ""

    # For Google OAuth
    GOOGLE_CLIENT_ID: str = ""
    GOOGLE_CLIENT_SECRET: str = ""
    GOOGLE_AUTH_URL: str = ""
    GOOGLE_TOKEN_URL: str = ""
    GOOGLE_USER_INFO_URL: str = ""
    GOOGLE_REDIRECT_URL: str = ""

    # For JWT
    JWT_SECRET: str = ""
    JWT_ALGORITHM: str = "HS256"

    # For Dhan API
    DHAN_CLIENT_ID: str = ""
    DHAN_ACCESS_TOKEN: str = ""

    # Redis
    REDIS_URL: str | None = None

    model_config = SettingsConfigDict(env_file=".env")


@lru_cache
def get_settings():
    return Settings()
