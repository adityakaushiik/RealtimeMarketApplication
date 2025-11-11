from enum import Enum


class WebSocketMessageType(Enum):

    # Message Types from clients to server
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"

    # Message Updates from server to clients
    LIVE_UPDATE = "live_update"
    CHART_DATA = "chart_data"
    INFO = "info"
    ERROR = "error"
