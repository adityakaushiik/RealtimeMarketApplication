from enum import Enum

from models.base_model_py import BaseModelPy


class UserRoles(Enum):
    ADMIN = 1
    VIEWER = 2


class WebSocketMessageType(Enum):
    # Message Types from clients to server
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"

    # Message Updates from server to clients
    LIVE_UPDATE = "live_update"
    CHART_DATA = "chart_data"
    INFO = "info"
    ERROR = "error"

class DataBroadcastFormat(BaseModelPy):
    timestamp: int
    symbol: str
    price: float
    volume: float

class DataIngestionFormat(DataBroadcastFormat):
    provider_code: str


