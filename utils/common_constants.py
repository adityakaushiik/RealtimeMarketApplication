from enum import Enum

from models.base_model_py import BaseModelPy


class UserRoles(Enum):
    ADMIN = 1
    VIEWER = 2


class WebSocketMessageType(Enum):
    # Message Types from clients to server
    SUBSCRIBE = 1
    UNSUBSCRIBE = 2

    # Message Updates from server to clients
    SNAPSHOT = 10
    UPDATE = 11

    # Log Levels
    INFO = 100
    ERROR = 101


class SupportedIntervals(Enum):
    FIVE_MINUTES = "5m"
    ONE_DAY = "1day"


class DataBroadcastFormat(BaseModelPy):
    timestamp: int
    symbol: str
    price: float
    volume: float


class DataIngestionFormat(DataBroadcastFormat):
    provider_code: str


def is_admin(user_claims: dict) -> bool:
    roles = user_claims.get("roles", [])
    return UserRoles.ADMIN.value in roles
